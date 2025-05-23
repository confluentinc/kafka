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

import org.apache.kafka.server.common.OffsetAndEpoch;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ValidOffsetAndEpochTest {

    @Test
    void diverging() {
        ValidOffsetAndEpoch validOffsetAndEpoch = ValidOffsetAndEpoch.diverging(new OffsetAndEpoch(0, 0));

        assertEquals(ValidOffsetAndEpoch.Kind.DIVERGING, validOffsetAndEpoch.kind());
    }

    @Test
    void snapshot() {
        ValidOffsetAndEpoch validOffsetAndEpoch = ValidOffsetAndEpoch.snapshot(new OffsetAndEpoch(0, 0));

        assertEquals(ValidOffsetAndEpoch.Kind.SNAPSHOT, validOffsetAndEpoch.kind());
    }

    @Test
    void valid() {
        ValidOffsetAndEpoch validOffsetAndEpoch = ValidOffsetAndEpoch.valid(new OffsetAndEpoch(0, 0));

        assertEquals(ValidOffsetAndEpoch.Kind.VALID, validOffsetAndEpoch.kind());
    }

    @Test
    void testValidWithoutSpecifyingOffsetAndEpoch() {
        ValidOffsetAndEpoch validOffsetAndEpoch = ValidOffsetAndEpoch.valid();

        assertEquals(ValidOffsetAndEpoch.Kind.VALID, validOffsetAndEpoch.kind());
        assertEquals(new OffsetAndEpoch(-1, -1), validOffsetAndEpoch.offsetAndEpoch());
    }
}