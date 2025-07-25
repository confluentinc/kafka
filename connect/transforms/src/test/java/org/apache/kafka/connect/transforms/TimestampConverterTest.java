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

package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Stream;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TimestampConverterTest {
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private static final Calendar EPOCH;
    private static final Calendar TIME;
    private static final Calendar DATE;
    private static final Calendar DATE_PLUS_TIME;
    private static final long DATE_PLUS_TIME_UNIX;
    private static final long DATE_PLUS_TIME_UNIX_MICROS;
    private static final long DATE_PLUS_TIME_UNIX_NANOS;
    private static final long DATE_PLUS_TIME_UNIX_SECONDS;
    private static final String STRING_DATE_FMT = "yyyy MM dd HH mm ss SSS z";
    private static final String DATE_PLUS_TIME_STRING;

    private final TimestampConverter<SourceRecord> xformKey = new TimestampConverter.Key<>();
    private final TimestampConverter<SourceRecord> xformValue = new TimestampConverter.Value<>();

    static {
        EPOCH = GregorianCalendar.getInstance(UTC);
        EPOCH.setTimeInMillis(0L);

        TIME = GregorianCalendar.getInstance(UTC);
        TIME.setTimeInMillis(0L);
        TIME.add(Calendar.MILLISECOND, 1234);

        DATE = GregorianCalendar.getInstance(UTC);
        DATE.setTimeInMillis(0L);
        DATE.set(1970, Calendar.JANUARY, 1, 0, 0, 0);
        DATE.add(Calendar.DATE, 1);

        DATE_PLUS_TIME = GregorianCalendar.getInstance(UTC);
        DATE_PLUS_TIME.setTimeInMillis(0L);
        DATE_PLUS_TIME.add(Calendar.DATE, 1);
        DATE_PLUS_TIME.add(Calendar.MILLISECOND, 1234);
        // 86 401 234 milliseconds
        DATE_PLUS_TIME_UNIX = DATE_PLUS_TIME.getTime().getTime();
        // 86 401 234 123 microseconds
        DATE_PLUS_TIME_UNIX_MICROS = DATE_PLUS_TIME_UNIX * 1000 + 123;
        // 86 401 234 123 456 nanoseconds
        DATE_PLUS_TIME_UNIX_NANOS = DATE_PLUS_TIME_UNIX_MICROS * 1000 + 456;
        // 86401 seconds
        DATE_PLUS_TIME_UNIX_SECONDS = DATE_PLUS_TIME.getTimeInMillis() / 1000;
        DATE_PLUS_TIME_STRING = "1970 01 02 00 00 01 234 UTC";
    }

    public static Stream<Arguments> data() {
        return Stream.of(
                Arguments.of(false, null),
                Arguments.of(true, EPOCH.getTime())
        );
    }

    // Configuration

    @AfterEach
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }

    @Test
    public void testConfigNoTargetType() {
        assertThrows(ConfigException.class, () -> xformValue.configure(Map.of()));
    }

    @Test
    public void testConfigInvalidTargetType() {
        assertThrows(ConfigException.class,
            () -> xformValue.configure(Map.of(TimestampConverter.TARGET_TYPE_CONFIG, "invalid")));
    }

    @Test
    public void testConfigInvalidUnixPrecision() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "unix");
        config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "invalid");
        assertThrows(ConfigException.class, () -> xformValue.configure(config));
    }

    @Test
    public void testConfigValidUnixPrecision() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "unix");
        config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "seconds");
        assertDoesNotThrow(() -> xformValue.configure(config));
    }

    @Test
    public void testConfigMissingFormat() {
        assertThrows(ConfigException.class,
            () -> xformValue.configure(Map.of(TimestampConverter.TARGET_TYPE_CONFIG, "string")));
    }

    @Test
    public void testConfigInvalidFormat() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "string");
        config.put(TimestampConverter.FORMAT_CONFIG, "bad-format");
        assertThrows(ConfigException.class, () -> xformValue.configure(config));
    }

    // Conversions without schemas (most flexible Timestamp -> other types)

    @Test
    public void testSchemalessIdentity() {
        xformValue.configure(Map.of(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME.getTime()));

        assertNull(transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.value());
    }

    @Test
    public void testSchemalessTimestampToDate() {
        xformValue.configure(Map.of(TimestampConverter.TARGET_TYPE_CONFIG, "Date"));
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME.getTime()));

        assertNull(transformed.valueSchema());
        assertEquals(DATE.getTime(), transformed.value());
    }

    @Test
    public void testSchemalessTimestampToTime() {
        xformValue.configure(Map.of(TimestampConverter.TARGET_TYPE_CONFIG, "Time"));
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME.getTime()));

        assertNull(transformed.valueSchema());
        assertEquals(TIME.getTime(), transformed.value());
    }

    @Test
    public void testSchemalessTimestampToUnix() {
        xformValue.configure(Map.of(TimestampConverter.TARGET_TYPE_CONFIG, "unix"));
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME.getTime()));

        assertNull(transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME_UNIX, transformed.value());
    }

    @Test
    public void testSchemalessTimestampToString() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "string");
        config.put(TimestampConverter.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME.getTime()));

        assertNull(transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME_STRING, transformed.value());
    }


    // Conversions without schemas (core types -> most flexible Timestamp format)

    @Test
    public void testSchemalessDateToTimestamp() {
        xformValue.configure(Map.of(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE.getTime()));

        assertNull(transformed.valueSchema());
        // No change expected since the source type is coarser-grained
        assertEquals(DATE.getTime(), transformed.value());
    }

    @Test
    public void testSchemalessTimeToTimestamp() {
        xformValue.configure(Map.of(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(TIME.getTime()));

        assertNull(transformed.valueSchema());
        // No change expected since the source type is coarser-grained
        assertEquals(TIME.getTime(), transformed.value());
    }

    @Test
    public void testSchemalessUnixToTimestamp() {
        xformValue.configure(Map.of(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME_UNIX));

        assertNull(transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.value());
    }

    @Test
    public void testSchemalessStringToTimestamp() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(TimestampConverter.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME_STRING));

        assertNull(transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.value());
    }


    // Conversions with schemas (most flexible Timestamp -> other types)

    @Test
    public void testWithSchemaIdentity() {
        xformValue.configure(Map.of(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

        assertEquals(Timestamp.SCHEMA, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.value());
    }

    @Test
    public void testWithSchemaTimestampToDate() {
        xformValue.configure(Map.of(TimestampConverter.TARGET_TYPE_CONFIG, "Date"));
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

        assertEquals(Date.SCHEMA, transformed.valueSchema());
        assertEquals(DATE.getTime(), transformed.value());
    }

    @Test
    public void testWithSchemaTimestampToTime() {
        xformValue.configure(Map.of(TimestampConverter.TARGET_TYPE_CONFIG, "Time"));
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

        assertEquals(Time.SCHEMA, transformed.valueSchema());
        assertEquals(TIME.getTime(), transformed.value());
    }

    @Test
    public void testWithSchemaTimestampToUnix() {
        xformValue.configure(Map.of(TimestampConverter.TARGET_TYPE_CONFIG, "unix"));
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

        assertEquals(Schema.INT64_SCHEMA, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME_UNIX, transformed.value());
    }

    @Test
    public void testWithSchemaTimestampToString() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "string");
        config.put(TimestampConverter.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(Timestamp.SCHEMA, DATE_PLUS_TIME.getTime()));

        assertEquals(Schema.STRING_SCHEMA, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME_STRING, transformed.value());
    }

    // Null-value conversions schemaless

    @Test
    public void testSchemalessNullValueToString() {
        testSchemalessNullValueConversion("string");
        testSchemalessNullFieldConversion("string");
    }
    @Test
    public void testSchemalessNullValueToDate() {
        testSchemalessNullValueConversion("Date");
        testSchemalessNullFieldConversion("Date");
    }
    @Test
    public void testSchemalessNullValueToTimestamp() {
        testSchemalessNullValueConversion("Timestamp");
        testSchemalessNullFieldConversion("Timestamp");
    }
    @Test
    public void testSchemalessNullValueToUnix() {
        testSchemalessNullValueConversion("unix");
        testSchemalessNullFieldConversion("unix");
    }

    @Test
    public void testSchemalessNullValueToTime() {
        testSchemalessNullValueConversion("Time");
        testSchemalessNullFieldConversion("Time");
    }

    private void testSchemalessNullValueConversion(String targetType) {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, targetType);
        config.put(TimestampConverter.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(null));

        assertNull(transformed.valueSchema());
        assertNull(transformed.value());
    }

    private void testSchemalessNullFieldConversion(String targetType) {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, targetType);
        config.put(TimestampConverter.FORMAT_CONFIG, STRING_DATE_FMT);
        config.put(TimestampConverter.FIELD_CONFIG, "ts");
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(null));

        assertNull(transformed.valueSchema());
        assertNull(transformed.value());
    }

    // Conversions with schemas (core types -> most flexible Timestamp format)

    @Test
    public void testWithSchemaDateToTimestamp() {
        xformValue.configure(Map.of(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(Date.SCHEMA, DATE.getTime()));

        assertEquals(Timestamp.SCHEMA, transformed.valueSchema());
        // No change expected since the source type is coarser-grained
        assertEquals(DATE.getTime(), transformed.value());
    }

    @Test
    public void testWithSchemaTimeToTimestamp() {
        xformValue.configure(Map.of(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(Time.SCHEMA, TIME.getTime()));

        assertEquals(Timestamp.SCHEMA, transformed.valueSchema());
        // No change expected since the source type is coarser-grained
        assertEquals(TIME.getTime(), transformed.value());
    }

    @Test
    public void testWithSchemaUnixToTimestamp() {
        xformValue.configure(Map.of(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(Schema.INT64_SCHEMA, DATE_PLUS_TIME_UNIX));

        assertEquals(Timestamp.SCHEMA, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.value());
    }

    @Test
    public void testWithSchemaStringToTimestamp() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(TimestampConverter.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(Schema.STRING_SCHEMA, DATE_PLUS_TIME_STRING));

        assertEquals(Timestamp.SCHEMA, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.value());
    }

    // Null-value conversions with schema

    @Test
    public void testWithSchemaNullValueToTimestamp() {
        testWithSchemaNullValueConversion("Timestamp", Schema.OPTIONAL_INT64_SCHEMA, TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
        testWithSchemaNullValueConversion("Timestamp", TimestampConverter.OPTIONAL_TIME_SCHEMA, TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
        testWithSchemaNullValueConversion("Timestamp", TimestampConverter.OPTIONAL_DATE_SCHEMA, TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
        testWithSchemaNullValueConversion("Timestamp", Schema.OPTIONAL_STRING_SCHEMA, TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
        testWithSchemaNullValueConversion("Timestamp", TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA, TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
    }

    @Test
    public void testWithSchemaNullFieldToTimestamp() {
        testWithSchemaNullFieldConversion("Timestamp", Schema.OPTIONAL_INT64_SCHEMA, TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
        testWithSchemaNullFieldConversion("Timestamp", TimestampConverter.OPTIONAL_TIME_SCHEMA, TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
        testWithSchemaNullFieldConversion("Timestamp", TimestampConverter.OPTIONAL_DATE_SCHEMA, TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
        testWithSchemaNullFieldConversion("Timestamp", Schema.OPTIONAL_STRING_SCHEMA, TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
        testWithSchemaNullFieldConversion("Timestamp", TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA, TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA);
    }

    @Test
    public void testWithSchemaNullValueToUnix() {
        testWithSchemaNullValueConversion("unix", Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
        testWithSchemaNullValueConversion("unix", TimestampConverter.OPTIONAL_TIME_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
        testWithSchemaNullValueConversion("unix", TimestampConverter.OPTIONAL_DATE_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
        testWithSchemaNullValueConversion("unix", Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
        testWithSchemaNullValueConversion("unix", TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
    }

    @Test
    public void testWithSchemaNullFieldToUnix() {
        testWithSchemaNullFieldConversion("unix", Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
        testWithSchemaNullFieldConversion("unix", TimestampConverter.OPTIONAL_TIME_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
        testWithSchemaNullFieldConversion("unix", TimestampConverter.OPTIONAL_DATE_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
        testWithSchemaNullFieldConversion("unix", Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
        testWithSchemaNullFieldConversion("unix", TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA);
    }

    @Test
    public void testWithSchemaNullValueToTime() {
        testWithSchemaNullValueConversion("Time", Schema.OPTIONAL_INT64_SCHEMA, TimestampConverter.OPTIONAL_TIME_SCHEMA);
        testWithSchemaNullValueConversion("Time", TimestampConverter.OPTIONAL_TIME_SCHEMA, TimestampConverter.OPTIONAL_TIME_SCHEMA);
        testWithSchemaNullValueConversion("Time", TimestampConverter.OPTIONAL_DATE_SCHEMA, TimestampConverter.OPTIONAL_TIME_SCHEMA);
        testWithSchemaNullValueConversion("Time", Schema.OPTIONAL_STRING_SCHEMA, TimestampConverter.OPTIONAL_TIME_SCHEMA);
        testWithSchemaNullValueConversion("Time", TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA, TimestampConverter.OPTIONAL_TIME_SCHEMA);
    }

    @Test
    public void testWithSchemaNullFieldToTime() {
        testWithSchemaNullFieldConversion("Time", Schema.OPTIONAL_INT64_SCHEMA, TimestampConverter.OPTIONAL_TIME_SCHEMA);
        testWithSchemaNullFieldConversion("Time", TimestampConverter.OPTIONAL_TIME_SCHEMA, TimestampConverter.OPTIONAL_TIME_SCHEMA);
        testWithSchemaNullFieldConversion("Time", TimestampConverter.OPTIONAL_DATE_SCHEMA, TimestampConverter.OPTIONAL_TIME_SCHEMA);
        testWithSchemaNullFieldConversion("Time", Schema.OPTIONAL_STRING_SCHEMA, TimestampConverter.OPTIONAL_TIME_SCHEMA);
        testWithSchemaNullFieldConversion("Time", TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA, TimestampConverter.OPTIONAL_TIME_SCHEMA);
    }

    @Test
    public void testWithSchemaNullValueToDate() {
        testWithSchemaNullValueConversion("Date", Schema.OPTIONAL_INT64_SCHEMA, TimestampConverter.OPTIONAL_DATE_SCHEMA);
        testWithSchemaNullValueConversion("Date", TimestampConverter.OPTIONAL_TIME_SCHEMA, TimestampConverter.OPTIONAL_DATE_SCHEMA);
        testWithSchemaNullValueConversion("Date", TimestampConverter.OPTIONAL_DATE_SCHEMA, TimestampConverter.OPTIONAL_DATE_SCHEMA);
        testWithSchemaNullValueConversion("Date", Schema.OPTIONAL_STRING_SCHEMA, TimestampConverter.OPTIONAL_DATE_SCHEMA);
        testWithSchemaNullValueConversion("Date", TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA, TimestampConverter.OPTIONAL_DATE_SCHEMA);
    }

    @Test
    public void testWithSchemaNullFieldToDate() {
        testWithSchemaNullFieldConversion("Date", Schema.OPTIONAL_INT64_SCHEMA, TimestampConverter.OPTIONAL_DATE_SCHEMA);
        testWithSchemaNullFieldConversion("Date", TimestampConverter.OPTIONAL_TIME_SCHEMA, TimestampConverter.OPTIONAL_DATE_SCHEMA);
        testWithSchemaNullFieldConversion("Date", TimestampConverter.OPTIONAL_DATE_SCHEMA, TimestampConverter.OPTIONAL_DATE_SCHEMA);
        testWithSchemaNullFieldConversion("Date", Schema.OPTIONAL_STRING_SCHEMA, TimestampConverter.OPTIONAL_DATE_SCHEMA);
        testWithSchemaNullFieldConversion("Date", TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA, TimestampConverter.OPTIONAL_DATE_SCHEMA);
    }

    @Test
    public void testWithSchemaNullValueToString() {
        testWithSchemaNullValueConversion("string", Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
        testWithSchemaNullValueConversion("string", TimestampConverter.OPTIONAL_TIME_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
        testWithSchemaNullValueConversion("string", TimestampConverter.OPTIONAL_DATE_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
        testWithSchemaNullValueConversion("string", Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
        testWithSchemaNullValueConversion("string", TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
    }

    @Test
    public void testWithSchemaNullFieldToString() {
        testWithSchemaNullFieldConversion("string", Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
        testWithSchemaNullFieldConversion("string", TimestampConverter.OPTIONAL_TIME_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
        testWithSchemaNullFieldConversion("string", TimestampConverter.OPTIONAL_DATE_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
        testWithSchemaNullFieldConversion("string", Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
        testWithSchemaNullFieldConversion("string", TimestampConverter.OPTIONAL_TIMESTAMP_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA);
    }

    private void testWithSchemaNullValueConversion(String targetType, Schema originalSchema, Schema expectedSchema) {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, targetType);
        config.put(TimestampConverter.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(originalSchema, null));

        assertEquals(expectedSchema, transformed.valueSchema());
        assertNull(transformed.value());
    }

    private void testWithSchemaNullFieldConversion(String targetType, Schema originalSchema, Schema expectedSchema) {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, targetType);
        config.put(TimestampConverter.FORMAT_CONFIG, STRING_DATE_FMT);
        config.put(TimestampConverter.FIELD_CONFIG, "ts");
        xformValue.configure(config);
        SchemaBuilder structSchema = SchemaBuilder.struct()
                .field("ts", originalSchema)
                .field("other", Schema.STRING_SCHEMA);

        SchemaBuilder expectedStructSchema = SchemaBuilder.struct()
                .field("ts", expectedSchema)
                .field("other", Schema.STRING_SCHEMA);

        Struct original = new Struct(structSchema);
        original.put("ts", null);
        original.put("other", "test");

        // Struct field is null
        SourceRecord transformed = xformValue.apply(createRecordWithSchema(structSchema.build(), original));

        assertEquals(expectedStructSchema.build(), transformed.valueSchema());
        assertNull(requireStruct(transformed.value(), "").get("ts"));

        // entire Struct is null
        transformed = xformValue.apply(createRecordWithSchema(structSchema.optional().build(), null));

        assertEquals(expectedStructSchema.optional().build(), transformed.valueSchema());
        assertNull(transformed.value());
    }

    // Convert field instead of entire key/value

    @Test
    public void testSchemalessFieldConversion() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Date");
        config.put(TimestampConverter.FIELD_CONFIG, "ts");
        xformValue.configure(config);

        Object value = Map.of("ts", DATE_PLUS_TIME.getTime());
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(value));

        assertNull(transformed.valueSchema());
        assertEquals(Map.of("ts", DATE.getTime()), transformed.value());
    }

    @Test
    public void testWithSchemaFieldConversion() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(TimestampConverter.FIELD_CONFIG, "ts");
        xformValue.configure(config);

        // ts field is a unix timestamp
        Schema structWithTimestampFieldSchema = SchemaBuilder.struct()
                .field("ts", Schema.INT64_SCHEMA)
                .field("other", Schema.STRING_SCHEMA)
                .build();
        Struct original = new Struct(structWithTimestampFieldSchema);
        original.put("ts", DATE_PLUS_TIME_UNIX);
        original.put("other", "test");

        SourceRecord transformed = xformValue.apply(createRecordWithSchema(structWithTimestampFieldSchema, original));

        Schema expectedSchema = SchemaBuilder.struct()
                .field("ts", Timestamp.SCHEMA)
                .field("other", Schema.STRING_SCHEMA)
                .build();
        assertEquals(expectedSchema, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), ((Struct) transformed.value()).get("ts"));
        assertEquals("test", ((Struct) transformed.value()).get("other"));
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testWithSchemaNullFieldWithDefaultConversion(boolean replaceNullWithDefault, Object expectedValue) {
        Map<String, Object> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(TimestampConverter.FIELD_CONFIG, "ts");
        config.put(TimestampConverter.REPLACE_NULL_WITH_DEFAULT_CONFIG, false);
        xformValue.configure(config);

        // ts field is a unix timestamp
        Schema structWithTimestampFieldSchema = SchemaBuilder.struct()
                .field("ts", SchemaBuilder.int64().optional().defaultValue(0L).build())
                .field("other", Schema.STRING_SCHEMA)
                .build();
        Struct original = new Struct(structWithTimestampFieldSchema);
        original.put("ts", null);
        original.put("other", "test");

        SourceRecord transformed = xformValue.apply(createRecordWithSchema(structWithTimestampFieldSchema, original));

        Schema expectedSchema = SchemaBuilder.struct()
                .field("ts", Timestamp.builder().optional().build())
                .field("other", Schema.STRING_SCHEMA)
                .build();

        assertEquals(expectedSchema, transformed.valueSchema());
        assertNull(((Struct) transformed.value()).get("ts"));
        assertEquals("test", ((Struct) transformed.value()).get("other"));
    }

    @Test
    public void testWithSchemaFieldConversion_Micros() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(TimestampConverter.FIELD_CONFIG, "ts");
        config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "microseconds");
        xformValue.configure(config);

        // ts field is a unix timestamp with microseconds precision
        Schema structWithTimestampFieldSchema = SchemaBuilder.struct()
                .field("ts", Schema.INT64_SCHEMA)
                .build();
        Struct original = new Struct(structWithTimestampFieldSchema);
        original.put("ts", DATE_PLUS_TIME_UNIX_MICROS);

        SourceRecord transformed = xformValue.apply(createRecordWithSchema(structWithTimestampFieldSchema, original));

        Schema expectedSchema = SchemaBuilder.struct()
                .field("ts", Timestamp.SCHEMA)
                .build();
        assertEquals(expectedSchema, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), ((Struct) transformed.value()).get("ts"));
    }

    @Test
    public void testWithSchemaFieldConversion_Nanos() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(TimestampConverter.FIELD_CONFIG, "ts");
        config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "nanoseconds");
        xformValue.configure(config);

        // ts field is a unix timestamp with microseconds precision
        Schema structWithTimestampFieldSchema = SchemaBuilder.struct()
                .field("ts", Schema.INT64_SCHEMA)
                .build();
        Struct original = new Struct(structWithTimestampFieldSchema);
        original.put("ts", DATE_PLUS_TIME_UNIX_NANOS);

        SourceRecord transformed = xformValue.apply(createRecordWithSchema(structWithTimestampFieldSchema, original));

        Schema expectedSchema = SchemaBuilder.struct()
                .field("ts", Timestamp.SCHEMA)
                .build();
        assertEquals(expectedSchema, transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), ((Struct) transformed.value()).get("ts"));
    }

    @Test
    public void testWithSchemaFieldConversion_Seconds() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
        config.put(TimestampConverter.FIELD_CONFIG, "ts");
        config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "seconds");
        xformValue.configure(config);

        // ts field is a unix timestamp with seconds precision
        Schema structWithTimestampFieldSchema = SchemaBuilder.struct()
                .field("ts", Schema.INT64_SCHEMA)
                .build();
        Struct original = new Struct(structWithTimestampFieldSchema);
        original.put("ts", DATE_PLUS_TIME_UNIX_SECONDS);

        SourceRecord transformed = xformValue.apply(createRecordWithSchema(structWithTimestampFieldSchema, original));

        Calendar expectedDate = GregorianCalendar.getInstance(UTC);
        expectedDate.setTimeInMillis(0L);
        expectedDate.add(Calendar.DATE, 1);
        expectedDate.add(Calendar.SECOND, 1);

        Schema expectedSchema = SchemaBuilder.struct()
                .field("ts", Timestamp.SCHEMA)
                .build();
        assertEquals(expectedSchema, transformed.valueSchema());
        assertEquals(expectedDate.getTime(), ((Struct) transformed.value()).get("ts"));
    }

    @Test
    public void testSchemalessStringToUnix_Micros() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "unix");
        config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "microseconds");
        config.put(TimestampConverter.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME_STRING));

        assertNull(transformed.valueSchema());
        // Conversion loss as expected, sub-millisecond part is not stored in pivot java.util.Date
        assertEquals(86401234000L, transformed.value());
    }

    @Test
    public void testSchemalessStringToUnix_Nanos() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "unix");
        config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "nanoseconds");
        config.put(TimestampConverter.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME_STRING));

        assertNull(transformed.valueSchema());
        // Conversion loss as expected, sub-millisecond part is not stored in pivot java.util.Date
        assertEquals(86401234000000L, transformed.value());
    }

    @Test
    public void testSchemalessStringToUnix_Seconds() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.TARGET_TYPE_CONFIG, "unix");
        config.put(TimestampConverter.UNIX_PRECISION_CONFIG, "seconds");
        config.put(TimestampConverter.FORMAT_CONFIG, STRING_DATE_FMT);
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(createRecordSchemaless(DATE_PLUS_TIME_STRING));

        assertNull(transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME_UNIX_SECONDS, transformed.value());
    }

    // Validate Key implementation in addition to Value

    @Test
    public void testKey() {
        xformKey.configure(Map.of(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp"));
        SourceRecord transformed = xformKey.apply(new SourceRecord(null, null, "topic", 0, null, DATE_PLUS_TIME.getTime(), null, null));

        assertNull(transformed.keySchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.key());
    }

    @Test
    public void testTimestampConverterVersionRetrievedFromAppInfoParser() {
        assertEquals(AppInfoParser.getVersion(), xformKey.version());
        assertEquals(AppInfoParser.getVersion(), xformValue.version());
        assertEquals(xformKey.version(), xformValue.version());
    }

    private SourceRecord createRecordWithSchema(Schema schema, Object value) {
        return new SourceRecord(null, null, "topic", 0, schema, value);
    }

    private SourceRecord createRecordSchemaless(Object value) {
        return createRecordWithSchema(null, value);
    }
}
