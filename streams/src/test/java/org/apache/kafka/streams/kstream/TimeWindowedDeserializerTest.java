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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TimeWindowedDeserializerTest {
    private final long windowSize = 5000000;
    private final TimeWindowedDeserializer<?> timeWindowedDeserializer = new TimeWindowedDeserializer<>(new StringDeserializer(), windowSize);
    private final Map<String, String> props = new HashMap<>();

    @Test
    public void testTimeWindowedDeserializerConstructor() {
        timeWindowedDeserializer.configure(props, true);
        final Deserializer<?> inner = timeWindowedDeserializer.innerDeserializer();
        assertNotNull(inner, "Inner deserializer should be not null");
        assertInstanceOf(StringDeserializer.class, inner, "Inner deserializer type should be StringDeserializer");
        assertThat(timeWindowedDeserializer.getWindowSize(), is(5000000L));
    }

    @Deprecated
    @Test
    public void shouldSetWindowSizeAndDeserializerThroughWindowSizeMsAndWindowedInnerClassSerdeConfigs() {
        props.put(StreamsConfig.WINDOW_SIZE_MS_CONFIG, "500");
        props.put(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, Serdes.ByteArraySerde.class.getName());
        try (final TimeWindowedDeserializer<?> deserializer = new TimeWindowedDeserializer<>()) {
            deserializer.configure(props, false);
            assertThat(deserializer.getWindowSize(), is(500L));
            assertInstanceOf(ByteArrayDeserializer.class, deserializer.innerDeserializer());
        }
    }

    @Test
    public void shouldSetWindowSizeAndDeserializerThroughWindowSizeMsAndWindowedInnerDeserializerClassConfigs() {
        props.put(TimeWindowedDeserializer.WINDOW_SIZE_MS_CONFIG, "500");
        props.put(TimeWindowedDeserializer.WINDOWED_INNER_DESERIALIZER_CLASS, Serdes.ByteArraySerde.class.getName());
        try (final TimeWindowedDeserializer<?> deserializer = new TimeWindowedDeserializer<>()) {
            deserializer.configure(props, false);
            assertThat(deserializer.getWindowSize(), is(500L));
            assertInstanceOf(ByteArrayDeserializer.class, deserializer.innerDeserializer());
        }
    }

    @Deprecated
    @Test
    public void shouldHaveSameConfigNameForWindowSizeMs() {
        assertEquals(TimeWindowedDeserializer.WINDOW_SIZE_MS_CONFIG, StreamsConfig.WINDOW_SIZE_MS_CONFIG);
    }

    @Deprecated
    @Test
    public void shouldIgnoreWindowedInnerClassSerdeConfigIfWindowedInnerDeserializerClassConfigIsSet() {
        props.put(TimeWindowedDeserializer.WINDOW_SIZE_MS_CONFIG, "500");
        props.put(TimeWindowedDeserializer.WINDOWED_INNER_DESERIALIZER_CLASS, Serdes.ByteArraySerde.class.getName());
        props.put(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, "some.non.existent.class");
        try (final TimeWindowedDeserializer<?> deserializer = new TimeWindowedDeserializer<>()) {
            deserializer.configure(props, false);
            assertThat(deserializer.getWindowSize(), is(500L));
            assertInstanceOf(ByteArrayDeserializer.class, deserializer.innerDeserializer());
        }
    }

    @Deprecated
    @Test
    public void shouldThrowErrorIfWindowSizeSetInStreamsConfigAndConstructor() {
        props.put(StreamsConfig.WINDOW_SIZE_MS_CONFIG, "500");
        assertThrows(IllegalArgumentException.class, () -> timeWindowedDeserializer.configure(props, false));
    }

    @Test
    public void shouldThrowErrorIfWindowSizeSetInConstructorConfigAndConstructor() {
        props.put(TimeWindowedDeserializer.WINDOW_SIZE_MS_CONFIG, "500");
        assertThrows(IllegalArgumentException.class, () -> timeWindowedDeserializer.configure(props, false));
    }

    @Deprecated
    @Test
    public void shouldThrowErrorIfWindowSizeIsNotSetAndWindowedInnerClassSerdeIsSet() {
        props.put(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, Serdes.ByteArraySerde.class.getName());
        try (final TimeWindowedDeserializer<?> deserializer = new TimeWindowedDeserializer<>()) {
            assertThrows(IllegalArgumentException.class, () -> deserializer.configure(props, false));
        }
    }

    @Test
    public void shouldThrowErrorIfWindowSizeIsNotSetAndWindowedInnerDeserializerClassIsSet() {
        props.put(TimeWindowedDeserializer.WINDOWED_INNER_DESERIALIZER_CLASS, Serdes.ByteArraySerde.class.getName());
        try (final TimeWindowedDeserializer<?> deserializer = new TimeWindowedDeserializer<>()) {
            assertThrows(IllegalArgumentException.class, () -> deserializer.configure(props, false));
        }
    }

    @Deprecated
    @Test
    public void shouldThrowErrorIfWindowedInnerClassSerdeIsNotSetAndWindowSizeMsInStreamsConfigIsSet() {
        props.put(StreamsConfig.WINDOW_SIZE_MS_CONFIG, "500");
        try (final TimeWindowedDeserializer<?> deserializer = new TimeWindowedDeserializer<>()) {
            assertThrows(IllegalArgumentException.class, () -> deserializer.configure(props, false));
        }
    }

    @Test
    public void shouldThrowErrorIfWindowedInnerClassSerdeIsNotSetAndWindowSizeMsInConstructorConfigIsSet() {
        props.put(TimeWindowedDeserializer.WINDOW_SIZE_MS_CONFIG, "500");
        try (final TimeWindowedDeserializer<?> deserializer = new TimeWindowedDeserializer<>()) {
            assertThrows(IllegalArgumentException.class, () -> deserializer.configure(props, false));
        }
    }

    @Deprecated
    @Test
    public void shouldThrowErrorIfDeserializerConflictInConstructorAndWindowedInnerClassSerdeConfig() {
        props.put(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, Serdes.ByteArraySerde.class.getName());
        assertThrows(IllegalArgumentException.class, () -> timeWindowedDeserializer.configure(props, false));
    }

    @Test
    public void shouldThrowErrorIfDeserializerConflictInConstructorAndWindowedInnerDeserializerClassConfig() {
        props.put(TimeWindowedDeserializer.WINDOWED_INNER_DESERIALIZER_CLASS, Serdes.ByteArraySerde.class.getName());
        assertThrows(IllegalArgumentException.class, () -> timeWindowedDeserializer.configure(props, false));
    }

    @Deprecated
    @Test
    public void shouldThrowConfigExceptionWhenInvalidWindowedInnerClassSerdeSupplied() {
        props.put(StreamsConfig.WINDOWED_INNER_CLASS_SERDE, "some.non.existent.class");
        assertThrows(ConfigException.class, () -> timeWindowedDeserializer.configure(props, false));
    }

    @Test
    public void shouldThrowConfigExceptionWhenInvalidWindowedInnerDeserializerClassSupplied() {
        props.put(TimeWindowedDeserializer.WINDOWED_INNER_DESERIALIZER_CLASS, "some.non.existent.class");
        assertThrows(ConfigException.class, () -> timeWindowedDeserializer.configure(props, false));
    }
}
