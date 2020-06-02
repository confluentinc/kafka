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
package org.apache.kafka.raft;

import org.apache.kafka.common.errors.KafkaRaftException;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.ConvertedRecords;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.AbstractIterator;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;

import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RaftUtilTest {

    @Test
    public void testSerializeNonSupportedRecords() {
        class MockRecords extends AbstractRecords {

            @Override
            public long writeTo(GatheringByteChannel channel, long position, int length) {
                return 0;
            }

            @Override
            public Iterable<? extends RecordBatch> batches() {
                return null;
            }

            @Override
            public AbstractIterator<? extends RecordBatch> batchIterator() {
                return null;
            }

            @Override
            public ConvertedRecords<? extends Records> downConvert(byte toMagic, long firstOffset, Time time) {
                return null;
            }

            @Override
            public int sizeInBytes() {
                return 0;
            }
        }

        assertThrows(UnsupportedOperationException.class,
            () -> RaftUtil.serializeRecords(new MockRecords()));
    }

    @Test
    public void testSerializeMemoryRecords() {
        ByteBuffer data = ByteBuffer.wrap("data".getBytes());
        MemoryRecords records = MemoryRecords.readableRecords(data);
        assertEquals(data, RaftUtil.serializeRecords(records));
    }

    @Test
    public void testSerializeExistingFileRecords() throws IOException {
        try (FileRecords fileRecords = FileRecords.open(tempFile())) {
            fileRecords.append(MemoryRecords.readableRecords(ByteBuffer.wrap("file".getBytes())));
            assertEquals(ByteBuffer.wrap("file".getBytes()), RaftUtil.serializeRecords(fileRecords));
        }
    }

    @Test
    public void testSerializeErrorFileRecords() throws IOException {
        class ErrorFileRecords extends FileRecords {

            private ErrorFileRecords(FileChannel channelMock) throws IOException {
                super(null, channelMock, 0, Integer.MAX_VALUE, false);
            }

            @Override
            public void readInto(ByteBuffer buffer, int position) throws IOException {
                throw new IOException("Could not read error file records");
            }
        }

        FileChannel channelMock = mock(FileChannel.class);

        when(channelMock.size()).thenReturn(42L);
        when(channelMock.position(42L)).thenReturn(null);

        KafkaRaftException thrown = assertThrows(KafkaRaftException.class,
            () -> RaftUtil.serializeRecords(new ErrorFileRecords(channelMock)));

        assertEquals(IOException.class, thrown.getCause().getClass());
    }
}
