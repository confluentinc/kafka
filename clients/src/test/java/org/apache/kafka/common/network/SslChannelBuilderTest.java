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
import static org.junit.jupiter.api.Assertions.assertThrows;

class SslChannelBuilderTest {

    @Test
    void testMaxUnsentPostHandshakeMessages() {
        String config = BrokerSecurityConfigs.SSL_MAX_UNSENT_POST_HANDSHAKE_MESSAGES_CONFIG;
        assertEquals(BrokerSecurityConfigs.DEFAULT_SSL_MAX_UNSENT_POST_HANDSHAKE_MESSAGES,
            SslChannelBuilder.maxUnsentPostHandshakeMessages(ConnectionMode.SERVER, Map.of()));
        assertEquals(10, SslChannelBuilder.maxUnsentPostHandshakeMessages(ConnectionMode.SERVER, Map.of(config, 10)));
        assertThrows(ConfigException.class,
            () -> SslChannelBuilder.maxUnsentPostHandshakeMessages(ConnectionMode.SERVER, Map.of(config, 0)));
        int max = BrokerSecurityConfigs.MAX_SSL_MAX_UNSENT_POST_HANDSHAKE_MESSAGES;
        assertEquals(max, SslChannelBuilder.maxUnsentPostHandshakeMessages(ConnectionMode.SERVER, Map.of(config, max)));
        assertThrows(ConfigException.class,
            () -> SslChannelBuilder.maxUnsentPostHandshakeMessages(ConnectionMode.SERVER, Map.of(config, max + 1)));
        // Clients don't define the config, so they ignore a value set there.
        assertEquals(BrokerSecurityConfigs.DEFAULT_SSL_MAX_UNSENT_POST_HANDSHAKE_MESSAGES,
            SslChannelBuilder.maxUnsentPostHandshakeMessages(ConnectionMode.CLIENT, Map.of(config, "abc")));
    }

    @Test
    void testHeldPostHandshakeMessagesMaxReadBytes() {
        String config = BrokerSecurityConfigs.SSL_HELD_POST_HANDSHAKE_MESSAGES_MAX_READ_BYTES_CONFIG;
        assertEquals(BrokerSecurityConfigs.DEFAULT_SSL_HELD_POST_HANDSHAKE_MESSAGES_MAX_READ_BYTES,
            SslChannelBuilder.heldPostHandshakeMessagesMaxReadBytes(ConnectionMode.SERVER, Map.of()));
        assertEquals(0L, SslChannelBuilder.heldPostHandshakeMessagesMaxReadBytes(ConnectionMode.SERVER, Map.of(config, 0L)));
        assertThrows(ConfigException.class,
            () -> SslChannelBuilder.heldPostHandshakeMessagesMaxReadBytes(ConnectionMode.SERVER, Map.of(config, -1L)));
        // Clients don't define the config, so they ignore a value set there.
        assertEquals(BrokerSecurityConfigs.DEFAULT_SSL_HELD_POST_HANDSHAKE_MESSAGES_MAX_READ_BYTES,
            SslChannelBuilder.heldPostHandshakeMessagesMaxReadBytes(ConnectionMode.CLIENT, Map.of(config, "abc")));
    }
}
