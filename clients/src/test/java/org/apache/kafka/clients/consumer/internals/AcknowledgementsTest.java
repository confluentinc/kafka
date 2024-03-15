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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AcknowledgementsTest {

    private final Acknowledgements acks = Acknowledgements.empty();
    @BeforeEach
    public void setup() {

    }

    @Test
    public void testSingleBatchSingleRecord() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        List<ShareFetchRequestData.AcknowledgementBatch> ackList = acks.getAcknowledgmentBatches();
        assertEquals(1, ackList.size());
        assertEquals(0L, ackList.get(0).baseOffset());
        assertEquals(0L, ackList.get(0).lastOffset());
        assertEquals(AcknowledgeType.ACCEPT, ackList.get(0).acknowledgeType());
        assertTrue(ackList.get(0).gapOffsets().isEmpty());
    }

    @Test
    public void testSingleBatchMultiRecord() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.add(1L, AcknowledgeType.ACCEPT);
        acks.add(2L, AcknowledgeType.ACCEPT);
        acks.add(3L, AcknowledgeType.ACCEPT);
        acks.add(4L, AcknowledgeType.ACCEPT);
    }

    @Test
    public void testMultiBatchMultiRecord() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.add(1L, AcknowledgeType.ACCEPT);
        acks.add(2L, AcknowledgeType.ACCEPT);
        acks.add(3L, AcknowledgeType.RELEASE);
        acks.add(4L, AcknowledgeType.RELEASE);
    }

    @Test
    public void testMultiBatchSingleMultiRecord() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.add(1L, AcknowledgeType.RELEASE);
        acks.add(2L, AcknowledgeType.RELEASE);
        acks.add(3L, AcknowledgeType.RELEASE);
        acks.add(4L, AcknowledgeType.RELEASE);
    }

    @Test
    public void testMultiBatchMultiSingleRecord() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.add(1L, AcknowledgeType.ACCEPT);
        acks.add(2L, AcknowledgeType.ACCEPT);
        acks.add(3L, AcknowledgeType.ACCEPT);
        acks.add(4L, AcknowledgeType.RELEASE);
    }

    @Test
    public void testSingleBatchSingleGap() {
        acks.addGap(0L);
    }

    @Test
    public void testSingleBatchMultiGap() {
        acks.addGap(0L);
        acks.addGap(1L);
    }

    @Test
    public void testSingleBatchSingleGapSingleRecord() {
        acks.addGap(0L);
        acks.add(1L, AcknowledgeType.ACCEPT);
    }

    @Test
    public void testSingleBatchSingleRecordSingleGap() {
        acks.add(0L, AcknowledgeType.ACCEPT);
        acks.addGap(1L);
    }
}