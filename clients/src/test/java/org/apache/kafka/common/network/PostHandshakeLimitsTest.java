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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PostHandshakeLimitsTest {

    @Test
    void testDefaults() {
        SslTransportLayer.PostHandshakeLimits limits = SslTransportLayer.PostHandshakeLimits.fromConfigs(ConnectionMode.SERVER, Map.of());
        assertEquals(BrokerSecurityConfigs.DEFAULT_SSL_MAX_UNSENT_POST_HANDSHAKE_BYTES, limits.maxUnsentBytes);
        assertEquals(BrokerSecurityConfigs.DEFAULT_SSL_HELD_POST_HANDSHAKE_MESSAGES_MAX_READ_BYTES,
            limits.heldMessagesMaxReadBytes);
    }

    @Test
    void testMaxUnsentBytes() {
        String config = BrokerSecurityConfigs.SSL_MAX_UNSENT_POST_HANDSHAKE_BYTES_CONFIG;
        assertEquals(10, maxUnsentBytes(Map.of(config, 10)));
        assertThrows(ConfigException.class, () -> maxUnsentBytes(Map.of(config, 0)));
        int max = BrokerSecurityConfigs.MAX_SSL_MAX_UNSENT_POST_HANDSHAKE_BYTES;
        assertEquals(max, maxUnsentBytes(Map.of(config, max)));
        assertThrows(ConfigException.class, () -> maxUnsentBytes(Map.of(config, max + 1)));
    }

    @Test
    void testHeldMessagesMaxReadBytes() {
        String config = BrokerSecurityConfigs.SSL_HELD_POST_HANDSHAKE_MESSAGES_MAX_READ_BYTES_CONFIG;
        assertEquals(0L, SslTransportLayer.PostHandshakeLimits.fromConfigs(ConnectionMode.SERVER, Map.of(config, 0L)).heldMessagesMaxReadBytes);
        assertThrows(ConfigException.class,
            () -> SslTransportLayer.PostHandshakeLimits.fromConfigs(ConnectionMode.SERVER, Map.of(config, -1L)));
    }

    @Test
    void testClientsUseDefaults() {
        // Clients don't define the configs, so they ignore values set there, which are unparsed.
        assertSame(SslTransportLayer.PostHandshakeLimits.DEFAULT, SslTransportLayer.PostHandshakeLimits.fromConfigs(ConnectionMode.CLIENT, Map.of(
            BrokerSecurityConfigs.SSL_MAX_UNSENT_POST_HANDSHAKE_BYTES_CONFIG, "abc",
            BrokerSecurityConfigs.SSL_HELD_POST_HANDSHAKE_MESSAGES_MAX_READ_BYTES_CONFIG, "abc")));
    }

    private static int maxUnsentBytes(Map<String, ?> configs) {
        return SslTransportLayer.PostHandshakeLimits.fromConfigs(ConnectionMode.SERVER, configs).maxUnsentBytes;
    }
}
