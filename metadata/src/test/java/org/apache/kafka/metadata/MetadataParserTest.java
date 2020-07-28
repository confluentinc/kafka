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

package org.apache.kafka.metadata;

import org.apache.kafka.common.metadata.BrokerRecord;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class MetadataParserTest {
    private static final Logger log =
        LoggerFactory.getLogger(MetadataParserTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.seconds(40);

    @Test
    public void testRoundTrips() throws Throwable {
        testRoundTrip(new BrokerRecord().setBrokerId(1).setBrokerEpoch(2), (short) 0);
        testRoundTrip(new ConfigRecord().setName("my.config.value").
            setResourceName("foo").setResourceType((byte) 0).setValue("bar"), (short) 0);
    }

    private static void testRoundTrip(ApiMessage message, short version) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = MetadataParser.size(message, version, cache);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        MetadataParser.write(message, version, cache, buffer);
        buffer.flip();
        ApiMessage message2 = MetadataParser.read(buffer.duplicate());
        assertEquals(message, message2);
        assertEquals(message2, message);

        ObjectSerializationCache cache2 = new ObjectSerializationCache();
        int size2 = MetadataParser.size(message2, version, cache2);
        assertEquals(size, size2);
        ByteBuffer buffer2 = ByteBuffer.allocate(size);
        MetadataParser.write(message2, version, cache2, buffer2);
        buffer2.flip();
        assertEquals(buffer.duplicate(), buffer2.duplicate());
    }
}
