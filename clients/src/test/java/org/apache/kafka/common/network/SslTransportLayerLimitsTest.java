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

import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class SslTransportLayerLimitsTest {

    @Test
    void testDefaults() {
        SslTransportLayer.Limits limits = SslTransportLayer.Limits.fromConfigs(ConnectionMode.SERVER, Map.of());
        assertEquals(BrokerSecurityConfigs.DEFAULT_SSL_MAX_NET_WRITE_BUFFER_BYTES, limits.maxNetWriteBufferBytes);
        assertEquals(BrokerSecurityConfigs.DEFAULT_SSL_HELD_POST_HANDSHAKE_MESSAGES_MAX_READ_BYTES,
            limits.heldMessagesMaxReadBytes);
    }

    @Test
    void testConfiguredLimits() {
        SslTransportLayer.Limits limits = SslTransportLayer.Limits.fromConfigs(ConnectionMode.SERVER, Map.of(
            BrokerSecurityConfigs.SSL_MAX_NET_WRITE_BUFFER_BYTES_CONFIG, BrokerSecurityConfigs.MIN_SSL_MAX_NET_WRITE_BUFFER_BYTES,
            BrokerSecurityConfigs.SSL_HELD_POST_HANDSHAKE_MESSAGES_MAX_READ_BYTES_CONFIG, 0L));
        assertEquals(BrokerSecurityConfigs.MIN_SSL_MAX_NET_WRITE_BUFFER_BYTES, limits.maxNetWriteBufferBytes);
        assertEquals(0L, limits.heldMessagesMaxReadBytes);
    }

    @Test
    void testClientsUseDefaults() {
        // Clients don't define the configs, so they ignore values set there, which are unparsed.
        assertSame(SslTransportLayer.Limits.DEFAULT, SslTransportLayer.Limits.fromConfigs(ConnectionMode.CLIENT, Map.of(
            BrokerSecurityConfigs.SSL_MAX_NET_WRITE_BUFFER_BYTES_CONFIG, "abc",
            BrokerSecurityConfigs.SSL_HELD_POST_HANDSHAKE_MESSAGES_MAX_READ_BYTES_CONFIG, "abc")));
    }
}
