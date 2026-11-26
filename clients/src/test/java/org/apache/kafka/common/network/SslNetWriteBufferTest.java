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

import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SslNetWriteBufferTest {
    private static final int PACKET = 1000;
    private static final int MESSAGE = 43;
    // The messages sent in one batch: wrapped until more than a packet is held, the most two packets hold along with
    // a free packet to wrap the next one.
    private static final int BATCH_MESSAGES = PACKET / MESSAGE + 1;
    private static final int MAX_CAPACITY = 8 * PACKET;

    private final AtomicInteger packetSize = new AtomicInteger(PACKET);
    private final SslNetWriteBuffer buffer = newBuffer(Long.MAX_VALUE);
    private final Socket socket = new Socket();
    private final SSLEngine engine = mock(SSLEngine.class);

    private SslNetWriteBuffer newBuffer(long heldMessagesMaxReadBytes) {
        return new SslNetWriteBuffer(packetSize::get, new SslTransportLayer.Limits(MAX_CAPACITY, heldMessagesMaxReadBytes),
            new LogContext().logger(SslNetWriteBufferTest.class));
    }

    /**
     * A socket that takes at most {@link #accept} bytes, and counts its writes.
     */
    private static final class Socket implements WritableByteChannel {
        int accept = Integer.MAX_VALUE;
        int writes;
        int bytes;

        @Override
        public int write(ByteBuffer src) {
            int n = Math.min(accept, src.remaining());
            src.position(src.position() + n);
            accept -= n;
            bytes += n;
            if (n > 0) {
                writes++;
            }
            return n;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {
        }
    }

    @BeforeEach
    public void setUp() throws SSLException {
        when(engine.wrap(any(ByteBuffer.class), any(ByteBuffer.class)))
            .thenAnswer(invocation -> wrap(invocation.getArgument(0), invocation.getArgument(1)));
    }

    /**
     * Wraps like the JDK, which needs a free packet to wrap even a small message and otherwise returns BUFFER_OVERFLOW
     * without touching the buffers: data into a record of the same size, and nothing into a {@link #MESSAGE} byte
     * post-handshake message.
     */
    private SSLEngineResult wrap(ByteBuffer src, ByteBuffer dst) {
        if (dst.remaining() < packetSize.get()) {
            return new SSLEngineResult(Status.BUFFER_OVERFLOW, HandshakeStatus.NOT_HANDSHAKING, 0, 0);
        }
        int consumed = src.remaining();
        int produced = consumed > 0 ? consumed : MESSAGE;
        src.position(src.limit());
        dst.put(new byte[produced]);
        return new SSLEngineResult(Status.OK, HandshakeStatus.NOT_HANDSHAKING, consumed, produced);
    }

    /**
     * Wraps a message the way SslTransportLayer does: sends first if enough are held, and on BUFFER_OVERFLOW sends a
     * full batch, grows the buffer if there is still no room, and retries.
     */
    private void wrapMessage(SslNetWriteBuffer buffer, boolean replyToPeer) throws IOException {
        if (buffer.shouldSendHeld()) {
            buffer.writeTo(socket);
        }
        while (buffer.wrapPostHandshakeMessage(engine, replyToPeer).getStatus() == Status.BUFFER_OVERFLOW) {
            int unsentBefore = buffer.remaining();
            if (buffer.isBatchFull()) {
                buffer.writeTo(socket);
            }
            boolean sentSome = buffer.remaining() < unsentBefore;
            assertTrue(buffer.growIfNoRoom() || sentSome, "No room made after BUFFER_OVERFLOW");
        }
    }

    private void wrapRecord(int bytes) throws SSLException {
        buffer.wrap(engine, ByteBuffer.allocate(bytes));
    }

    @Test
    public void testRecordIsSentAndBufferEmptied() throws IOException {
        wrapRecord(100);
        assertEquals(100, buffer.remaining());
        assertTrue(buffer.writeTo(socket));
        assertFalse(buffer.hasRemaining());
        assertEquals(100, socket.bytes);
    }

    @Test
    public void testPartialWriteKeepsTheRest() throws IOException {
        wrapRecord(100);
        socket.accept = 60;
        assertFalse(buffer.writeTo(socket));
        assertEquals(40, buffer.remaining());
        assertThrows(IllegalStateException.class, () -> wrapRecord(10));
        assertEquals(40, buffer.remaining(), "A failed wrap must not touch the unsent bytes");
        socket.accept = Integer.MAX_VALUE;
        assertTrue(buffer.writeTo(socket));
        assertEquals(100, socket.bytes);
    }

    @Test
    public void testGrowForRecordFollowsPacketSize() {
        packetSize.set(2 * PACKET);
        buffer.growForRecord();
        assertEquals(2 * PACKET, buffer.capacity());
        assertFalse(buffer.hasRemaining());
    }

    @Test
    public void testEngineStartedMessagesAreSentOnceMaxAreHeld() throws IOException {
        for (int i = 0; i < SslTransportLayer.Limits.MAX_HELD_MESSAGES - 1; i++) {
            wrapMessage(buffer, false);
        }
        assertFalse(buffer.shouldSendAfterRead());
        assertFalse(buffer.shouldSendHeld());
        wrapMessage(buffer, false);
        assertTrue(buffer.shouldSendAfterRead());
        assertTrue(buffer.shouldSendHeld());
        assertEquals(0, socket.writes);
        assertTrue(buffer.writeTo(socket));
        assertFalse(buffer.shouldSendAfterRead(), "Sending resets the held messages");
    }

    @Test
    public void testEngineStartedMessagesAreSentAfterMaxReadBytes() throws IOException {
        SslNetWriteBuffer buffer = newBuffer(1000);
        buffer.dataRead(5000);
        wrapMessage(buffer, false);
        assertFalse(buffer.shouldSendAfterRead(), "Data read before a message was held doesn't count");
        buffer.dataRead(999);
        assertFalse(buffer.shouldSendAfterRead());
        buffer.dataRead(1);
        assertTrue(buffer.shouldSendAfterRead());
    }

    @Test
    public void testPeerRepliesAreSentAfterTheRead() throws IOException {
        // More than the KeyUpdates the engine started that are held, but fewer than a batch.
        int messages = SslTransportLayer.Limits.MAX_HELD_MESSAGES + 4;
        assertTrue(messages < BATCH_MESSAGES);
        for (int i = 0; i < messages; i++) {
            wrapMessage(buffer, true);
        }
        assertFalse(buffer.shouldSendHeld(), "Replies to the peer don't count toward the held KeyUpdates");
        assertEquals(0, socket.writes);
        assertTrue(buffer.shouldSendAfterRead());
        assertTrue(buffer.writeTo(socket));
        assertFalse(buffer.shouldSendAfterRead());
    }

    @Test
    public void testMessagesAreSentInBatches() throws IOException {
        int messages = 5 * BATCH_MESSAGES + 7;
        for (int i = 0; i < messages; i++) {
            wrapMessage(buffer, true);
            assertTrue(buffer.capacity() <= 2 * PACKET, "Grew past one batch");
        }
        assertEquals(5, socket.writes);
        assertEquals(5 * BATCH_MESSAGES * MESSAGE, socket.bytes);
        assertTrue(buffer.shouldSendAfterRead());
        assertTrue(buffer.writeTo(socket));
        assertEquals(messages * MESSAGE, socket.bytes);
        assertEquals(2 * PACKET, buffer.capacity(), "Sending alone doesn't shrink the buffer");
        buffer.shrinkIfSent();
        assertEquals(PACKET, buffer.capacity(), "The buffer shrinks back to a packet once sent");
    }

    @Test
    public void testGrowsWhileTheSocketTakesNothingThenCloses() throws IOException {
        socket.accept = 0;
        // As many as fit in the max capacity along with a free packet.
        int messages = (MAX_CAPACITY - PACKET) / MESSAGE + 1;
        for (int i = 0; i < messages; i++) {
            wrapMessage(buffer, true);
        }
        SSLException e = assertThrows(SSLException.class, () -> wrapMessage(buffer, true));
        assertEquals(SslTransportLayer.UNSENT_POST_HANDSHAKE_MESSAGES_MESSAGE, e.getMessage());
        assertEquals(MAX_CAPACITY, buffer.capacity());
        assertEquals(messages * MESSAGE, buffer.remaining(), "The unsent bytes are kept in read mode");

        buffer.shrinkIfSent();
        assertEquals(MAX_CAPACITY, buffer.capacity(), "Shrunk with bytes unsent");
        socket.accept = Integer.MAX_VALUE;
        assertTrue(buffer.writeTo(socket));
        buffer.shrinkIfSent();
        assertEquals(PACKET, buffer.capacity(), "A grown buffer shrinks back to a packet once sent");
    }

    @Test
    public void testShrinksToTheCapacityARecordNeeds() throws IOException {
        packetSize.set(2 * PACKET);
        buffer.growForRecord();
        socket.accept = 0;
        // Enough to grow past the record's capacity.
        for (int i = 0; i < 2; i++) {
            wrapMessage(buffer, true);
        }
        assertEquals(4 * PACKET, buffer.capacity());
        socket.accept = Integer.MAX_VALUE;
        assertTrue(buffer.writeTo(socket));
        buffer.shrinkIfSent();
        assertEquals(2 * PACKET, buffer.capacity());
    }

    @Test
    public void testDiscardDropsEverything() throws IOException {
        socket.accept = 0;
        for (int i = 0; i < SslTransportLayer.Limits.MAX_HELD_MESSAGES; i++) {
            wrapMessage(buffer, false);
        }
        wrapMessage(buffer, true);
        buffer.discard();
        assertFalse(buffer.hasRemaining());
        assertFalse(buffer.shouldSendAfterRead());
        assertEquals(0, socket.bytes);
    }
}
