/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.network;

import org.apache.kafka.common.utils.internals.ByteUtils;

import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.function.IntSupplier;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;

/**
 * The TLS records {@link SslTransportLayer} has wrapped but not yet sent.
 *
 * <p>Most records are wrapped into an empty buffer, with {@link #wrap(SSLEngine, ByteBuffer)}, and sent before the
 * next one. TLSv1.3 post-handshake messages that the engine produces while reading, such as KeyUpdate responses, are
 * instead wrapped behind the unsent bytes, with {@link #wrapPostHandshakeMessage(SSLEngine, boolean)}, and sent in
 * batches:
 * <ul>
 *     <li>KeyUpdates the engine starts on its own, once its key has been used enough, when it reads records with
 *     data. They are held until the next write, because an OpenSSL peer fails if a KeyUpdate arrives while it is
 *     writing. They are still sent once a few are held, so that they go out in short bursts, since BoringSSL fails
 *     after 32 KeyUpdates in a row, or once enough data was read, so that a peer we never write to, such as a producer
 *     using acks=0, gets them too.</li>
 *     <li>Replies to the peer's post-handshake messages, such as its KeyUpdates, which arrive in records without
 *     data. They are not held, and are sent once the read has unwrapped what it read.</li>
 *     <li>The engine needs room for a whole packet to wrap even a small message, and returns BUFFER_OVERFLOW
 *     otherwise. The buffer then grows to one batch of two packets, and a full batch is sent rather than grow past
 *     it. It grows further, up to {@code ssl.max.net.write.buffer.bytes}, only if the socket doesn't take the bytes,
 *     and shrinks back to one record at the end of the read that sends them.</li>
 * </ul>
 *
 * <p>This class never writes to the socket on its own. {@link SslTransportLayer} asks {@link #shouldSendHeld()},
 * {@link #isBatchFull()} and {@link #shouldSendAfterRead()}, and calls {@link #writeTo(WritableByteChannel)}.
 */
final class SslNetWriteBuffer {
    private final IntSupplier packetSize;
    private final SslTransportLayer.Limits limits;
    private final Logger log;

    // In read mode between calls: its position to its limit are the unsent bytes.
    private ByteBuffer buffer;
    // The capacity one record needs: the packet size when the buffer was created or last grew for a record. The
    // buffer grows past it only for post-handshake messages.
    private int recordCapacity;
    // Unsent KeyUpdates the engine started on its own, which are held.
    private int engineStartedMessages;
    // Bytes of data read while engineStartedMessages are held.
    private long bytesReadWhileHeld;
    // Unsent replies to the peer's post-handshake messages, which are sent once the read ends.
    private int peerReplies;

    /**
     * @param packetSize the engine's current packet buffer size, which may grow after the handshake
     */
    SslNetWriteBuffer(IntSupplier packetSize, SslTransportLayer.Limits limits, Logger log) {
        this.packetSize = packetSize;
        this.limits = limits;
        this.log = log;
        this.recordCapacity = packetSize.getAsInt();
        this.buffer = emptyBuffer(recordCapacity);
    }

    private static ByteBuffer emptyBuffer(int capacity) {
        return ByteBuffer.allocate(capacity).flip();
    }

    boolean hasRemaining() {
        return buffer.hasRemaining();
    }

    int remaining() {
        return buffer.remaining();
    }

    int position() {
        return buffer.position();
    }

    // Visible for testing
    int capacity() {
        return buffer.capacity();
    }

    /**
     * Writes the unsent bytes to {@code channel}, non blocking.
     *
     * @return true if every byte was sent
     */
    boolean writeTo(WritableByteChannel channel) throws IOException {
        int remaining = buffer.remaining();
        if (remaining > 0 && channel.write(buffer) < remaining) {
            return false;
        }
        clearPostHandshakeMessages();
        return true;
    }

    /**
     * Shrinks a buffer grown for post-handshake messages back to one record once every byte was sent, so a
     * connection doesn't keep the room it needed for them. Called once a read ends rather than on every send, so a
     * read that sends several batches reuses the buffer.
     */
    void shrinkIfSent() {
        if (!buffer.hasRemaining() && buffer.capacity() > recordCapacity) {
            buffer = emptyBuffer(recordCapacity);
        }
    }

    /**
     * Drops the unsent bytes, for a peer that has disconnected.
     */
    void discard() {
        buffer.position(0);
        buffer.limit(0);
        clearPostHandshakeMessages();
    }

    private void clearPostHandshakeMessages() {
        engineStartedMessages = 0;
        bytesReadWhileHeld = 0;
        peerReplies = 0;
    }

    /**
     * Wraps {@code src} into a record. Every byte must have been sent.
     */
    SSLEngineResult wrap(SSLEngine engine, ByteBuffer src) throws SSLException {
        if (buffer.hasRemaining()) {
            throw new IllegalStateException("Wrapping a record with " + buffer.remaining() + " bytes unsent");
        }
        buffer.clear();
        try {
            return engine.wrap(src, buffer);
        } finally {
            // Even if the wrap failed, the wrapped bytes become unsent bytes.
            buffer.flip();
        }
    }

    /**
     * Wraps a post-handshake message the engine needs to send, such as a KeyUpdate, behind the unsent bytes. The
     * engine returns BUFFER_OVERFLOW, leaving the buffer as it was, if they leave less than a packet free.
     *
     * @param replyToPeer true if the record read produced no data, so the message replies to the peer's post-handshake
     *                    message, such as its KeyUpdate. False if it is a KeyUpdate the engine started on its own.
     */
    SSLEngineResult wrapPostHandshakeMessage(SSLEngine engine, boolean replyToPeer) throws SSLException {
        toWriteMode();
        SSLEngineResult result;
        try {
            result = engine.wrap(ByteUtils.EMPTY_BUF, buffer);
        } finally {
            buffer.flip();
        }
        if (result.getStatus() == Status.OK && result.bytesProduced() > 0) {
            if (replyToPeer) {
                peerReplies++;
            } else {
                engineStartedMessages++;
            }
        }
        return result;
    }

    /**
     * Leaves the buffer in write mode behind the unsent bytes. A partial send leaves them further in, so they are moved
     * to the start, where the flip after the wrap expects them.
     */
    private void toWriteMode() {
        if (buffer.position() > 0) {
            buffer.compact();
        } else {
            // Same as compact(), without copying the unsent bytes onto themselves on every wrap while a batch builds up.
            buffer.position(buffer.limit()).limit(buffer.capacity());
        }
    }

    /**
     * Whether more than a packet is unsent, so the buffer holds a full batch of post-handshake messages, which is sent
     * rather than grow past it. Asked after the engine found less than a packet free to wrap the next one. A batch is
     * then two packets: a packet of unsent bytes, about 390 KeyUpdates of 43 bytes, and the free packet the engine
     * needs. Replies to the peer can fill it, since one read may unwrap any number of the peer's KeyUpdates. KeyUpdates
     * the engine started are sent once 16 are held, {@code 16 * 43 = 688} bytes.
     */
    boolean isBatchFull() {
        return buffer.remaining() > packetSize.getAsInt();
    }

    /**
     * Grows the buffer, doubling up to {@code ssl.max.net.write.buffer.bytes}, if the unsent bytes leave less than a
     * packet free. Copies only the unsent bytes, so it also drops what a partial send already sent.
     *
     * @return whether it grew
     * @throws SSLException if a packet does not fit even at {@code ssl.max.net.write.buffer.bytes}, which closes the
     *                      connection. The buffer is left as it was.
     */
    boolean growIfNoRoom() throws SSLException {
        int unsentBytes = buffer.remaining();
        int required = unsentBytes + packetSize.getAsInt();
        if (required <= buffer.capacity()) {
            return false;
        }
        if (required > limits.maxNetWriteBufferBytes) {
            log.info("Closing channel since {} bytes are unsent along with TLS post-handshake messages", unsentBytes);
            throw new SSLException(SslTransportLayer.UNSENT_POST_HANDSHAKE_MESSAGES_MESSAGE);
        }
        // Doubling keeps the copies few.
        int newCapacity = Math.min(Math.max(required, 2 * buffer.capacity()), limits.maxNetWriteBufferBytes);
        buffer = ByteBuffer.allocate(newCapacity).put(buffer).flip();
        return true;
    }

    /**
     * After the engine returned BUFFER_OVERFLOW wrapping into an empty buffer, because its packet size grew, grows the
     * buffer to the new packet size so the wrap can be retried.
     */
    void growForRecord() {
        if (buffer.hasRemaining()) {
            throw new IllegalStateException("Buffer overflow with " + buffer.remaining() + " bytes unsent");
        }
        int size = packetSize.getAsInt();
        if (buffer.capacity() < size) {
            recordCapacity = size;
            buffer = emptyBuffer(size);
        }
    }

    /**
     * Records data the read has unwrapped, which counts toward
     * {@link SslTransportLayer.Limits#heldMessagesMaxReadBytes} while KeyUpdates the engine started are held.
     */
    void dataRead(int bytes) {
        if (engineStartedMessages > 0) {
            bytesReadWhileHeld += bytes;
        }
    }

    /**
     * Whether to send once the read has unwrapped what it read: there are replies to the peer, or enough KeyUpdates the
     * engine started are held.
     */
    boolean shouldSendAfterRead() {
        return peerReplies > 0 || shouldSendHeld();
    }

    /**
     * Whether enough KeyUpdates the engine started are held that they are sent before wrapping another post-handshake
     * message.
     */
    boolean shouldSendHeld() {
        return engineStartedMessages >= SslTransportLayer.Limits.MAX_HELD_MESSAGES ||
            (engineStartedMessages > 0 && bytesReadWhileHeld >= limits.heldMessagesMaxReadBytes);
    }

    @Override
    public String toString() {
        return buffer.toString();
    }
}
