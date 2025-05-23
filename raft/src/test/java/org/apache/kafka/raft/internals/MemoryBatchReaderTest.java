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
package org.apache.kafka.raft.internals;

import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MemoryBatchReaderTest {

    @Test
    public void testIteration() {
        Batch<String> batch1 = Batch.data(
            0L, 1, 0L, 3, List.of("a", "b", "c")
        );
        Batch<String> batch2 = Batch.data(
            3L, 2, 1L, 2, List.of("d", "e")
        );
        Batch<String> batch3 = Batch.data(
            5L, 2, 3L, 4, List.of("f", "g", "h", "i")
        );

        @SuppressWarnings("unchecked")
        CloseListener<BatchReader<String>> listener = Mockito.mock(CloseListener.class);
        MemoryBatchReader<String> reader = MemoryBatchReader.of(
            List.of(batch1, batch2, batch3),
            listener
        );

        assertEquals(0L, reader.baseOffset());
        assertEquals(OptionalLong.of(8L), reader.lastOffset());

        assertTrue(reader.hasNext());
        assertEquals(batch1, reader.next());

        assertTrue(reader.hasNext());
        assertEquals(batch2, reader.next());

        assertTrue(reader.hasNext());
        assertEquals(batch3, reader.next());

        assertFalse(reader.hasNext());

        reader.close();
        Mockito.verify(listener).onClose(reader);
    }

}
