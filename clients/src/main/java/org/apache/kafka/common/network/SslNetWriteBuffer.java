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
 *     <li>The engine needs room for a whole packet to wrap even a small message. The buffer may grow to one batch of
 *     two packets, and a full batch is sent rather than grow past it. It grows further, up to
 *     {@code ssl.max.net.write.buffer.bytes}, only if the socket doesn't take the bytes, and shrinks back to one
 *     packet once they are sent.</li>
 * </ul>
 *
 * <p>This class never writes to the socket on its own. {@link SslTransportLayer} asks
 * {@link #shouldSendBeforePostHandshakeMessage()} and {@link #shouldSendAfterRead()}, and calls
 * {@link #writeTo(WritableByteChannel)}.
 */
final class SslNetWriteBuffer {
    private final IntSupplier packetSize;
    private final SslTransportLayer.Limits limits;
    private final Logger log;

    // In read mode between calls: its position to its limit are the unsent bytes.
    private ByteBuffer buffer;
    // Whether the buffer grew for post-handshake messages since the last full send, so it may have to shrink.
    private boolean grown;
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
        this.buffer = emptyBuffer(packetSize.getAsInt());
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
     * @return true if every byte was sent. A buffer grown past a packet then shrinks back to one, so a connection
     *         doesn't keep the room it needed for post-handshake messages.
     */
    boolean writeTo(WritableByteChannel channel) throws IOException {
        int remaining = buffer.remaining();
        if (remaining > 0 && channel.write(buffer) < remaining) {
            return false;
        }
        clearPostHandshakeMessages();
        // Checked only after a growth, since asking the engine for its packet size on every write isn't free.
        if (grown) {
            grown = false;
            int size = packetSize.getAsInt();
            if (buffer.capacity() > size) {
                buffer = emptyBuffer(size);
            }
        }
        return true;
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
     * Wraps a post-handshake message the engine needs to send, such as a KeyUpdate, behind the unsent bytes.
     *
     * @param replyToPeer true if the record read produced no data, so the message replies to the peer's post-handshake
     *                    message, such as its KeyUpdate. False if it is a KeyUpdate the engine started on its own.
     * @throws SSLException if the unsent bytes leave no room for the message, which closes the connection. The buffer
     *                      is left as it was.
     */
    SSLEngineResult wrapPostHandshakeMessage(SSLEngine engine, boolean replyToPeer) throws SSLException {
        makeRoomForPostHandshakeMessage();
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
     * Leaves the buffer in write mode, behind the unsent bytes, with room for a packet. Grows it, doubling up to
     * {@code ssl.max.net.write.buffer.bytes}, if the unsent bytes leave no room.
     */
    private void makeRoomForPostHandshakeMessage() throws SSLException {
        int unsentBytes = buffer.remaining();
        int required = unsentBytes + packetSize.getAsInt();
        if (required > limits.maxNetWriteBufferBytes) {
            log.info("Closing channel since {} bytes are unsent along with TLS post-handshake messages", unsentBytes);
            throw new SSLException(SslTransportLayer.UNSENT_POST_HANDSHAKE_MESSAGES_MESSAGE);
        }
        if (required > buffer.capacity()) {
            // Doubling keeps the copies few.
            int newCapacity = Math.min(Math.max(required, 2 * buffer.capacity()), limits.maxNetWriteBufferBytes);
            buffer = ByteBuffer.allocate(newCapacity).put(buffer);
            grown = true;
        } else if (buffer.position() > 0) {
            buffer.compact();
        } else {
            // The unsent bytes are already at the start.
            buffer.position(unsentBytes).limit(buffer.capacity());
        }
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
     * Whether to send before wrapping another post-handshake message: enough KeyUpdates the engine started are held,
     * or the replies to the peer fill a batch.
     */
    boolean shouldSendBeforePostHandshakeMessage() {
        return shouldSendHeld() || isBatchFull();
    }

    /**
     * Whether wrapping the next message would grow the buffer past {@link #desiredCapacity()}, since the engine
     * needs a free packet to wrap it.
     */
    private boolean isBatchFull() {
        int requiredBytes = buffer.remaining() + packetSize.getAsInt();
        // The buffer may have grown past the desired capacity while the socket was full.
        int batchCapacity = Math.max(buffer.capacity(), desiredCapacity());
        return requiredBytes > batchCapacity;
    }

    /**
     * The capacity a batch of post-handshake messages may use:
     * <pre>
     * desiredCapacity = packetSize    // free room the engine needs to wrap the next message
     *                 + packetSize    // unsent bytes: about 390 KeyUpdates of 43 bytes, or a partly sent record
     * </pre>
     * The buffer grows past it only while the socket is full. Replies to the peer can fill it, since one read may
     * unwrap any number of the peer's KeyUpdates. KeyUpdates the engine started are flushed once 16 are held,
     * {@code 16 * 43 = 688} bytes.
     */
    private int desiredCapacity() {
        return 2 * packetSize.getAsInt();
    }

    /**
     * Whether to send once the read has unwrapped what it read: there are replies to the peer, or enough KeyUpdates the
     * engine started are held.
     */
    boolean shouldSendAfterRead() {
        return peerReplies > 0 || shouldSendHeld();
    }

    private boolean shouldSendHeld() {
        return engineStartedMessages >= SslTransportLayer.Limits.MAX_HELD_MESSAGES ||
            (engineStartedMessages > 0 && bytesReadWhileHeld >= limits.heldMessagesMaxReadBytes);
    }

    @Override
    public String toString() {
        return buffer.toString();
    }
}
