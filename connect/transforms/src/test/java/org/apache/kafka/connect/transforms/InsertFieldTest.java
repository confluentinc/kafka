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

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class InsertFieldTest {
    private final InsertField<SourceRecord> xformKey = new InsertField.Key<>();
    private final InsertField<SourceRecord> xformValue = new InsertField.Value<>();

    public static Stream<Arguments> data() {
        return Stream.of(
                Arguments.of(false, null),
                Arguments.of(true, 42L)
        );
    }

    @AfterEach
    public void teardown() {
        xformValue.close();
    }

    @Test
    public void topLevelStructRequired() {
        xformValue.configure(Map.of("topic.field", "topic_field"));
        assertThrows(DataException.class,
            () -> xformValue.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42)));
    }

    @Test
    public void copySchemaAndInsertConfiguredFields() {
        final Map<String, Object> props = new HashMap<>();
        props.put("topic.field", "topic_field!");
        props.put("partition.field", "partition_field");
        props.put("timestamp.field", "timestamp_field?");
        props.put("static.field", "instance_id");
        props.put("static.value", "my-instance-id");

        xformValue.configure(props);

        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).build();
        final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L);

        final SourceRecord record = new SourceRecord(null, null, "test", 0, null, null, simpleStructSchema, simpleStruct, 789L);
        final SourceRecord transformedRecord = xformValue.apply(record);

        assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

        assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("magic").schema());
        assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("magic").longValue());

        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("topic_field").schema());
        assertEquals("test", ((Struct) transformedRecord.value()).getString("topic_field"));

        assertEquals(Schema.OPTIONAL_INT32_SCHEMA, transformedRecord.valueSchema().field("partition_field").schema());
        assertEquals(0, ((Struct) transformedRecord.value()).getInt32("partition_field").intValue());

        assertEquals(Timestamp.builder().optional().build(), transformedRecord.valueSchema().field("timestamp_field").schema());
        assertEquals(789L, ((Date) ((Struct) transformedRecord.value()).get("timestamp_field")).getTime());

        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("instance_id").schema());
        assertEquals("my-instance-id", ((Struct) transformedRecord.value()).getString("instance_id"));

        // Exercise caching
        final SourceRecord transformedRecord2 = xformValue.apply(
                new SourceRecord(null, null, "test", 1, simpleStructSchema, new Struct(simpleStructSchema)));
        assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());
    }

    @Test
    public void schemalessInsertConfiguredFields() {
        final Map<String, Object> props = new HashMap<>();
        props.put("topic.field", "topic_field!");
        props.put("partition.field", "partition_field");
        props.put("timestamp.field", "timestamp_field?");
        props.put("static.field", "instance_id");
        props.put("static.value", "my-instance-id");

        xformValue.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, null, null, Map.of("magic", 42L), 123L);

        final SourceRecord transformedRecord = xformValue.apply(record);

        assertEquals(42L, ((Map<?, ?>) transformedRecord.value()).get("magic"));
        assertEquals("test", ((Map<?, ?>) transformedRecord.value()).get("topic_field"));
        assertEquals(0, ((Map<?, ?>) transformedRecord.value()).get("partition_field"));
        assertEquals(123L, ((Map<?, ?>) transformedRecord.value()).get("timestamp_field"));
        assertEquals("my-instance-id", ((Map<?, ?>) transformedRecord.value()).get("instance_id"));
    }

    @Test
    public void insertConfiguredFieldsIntoTombstoneEventWithoutSchemaLeavesValueUnchanged() {
        final Map<String, Object> props = new HashMap<>();
        props.put("topic.field", "topic_field!");
        props.put("partition.field", "partition_field");
        props.put("timestamp.field", "timestamp_field?");
        props.put("static.field", "instance_id");
        props.put("static.value", "my-instance-id");

        xformValue.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, null);

        final SourceRecord transformedRecord = xformValue.apply(record);

        assertNull(transformedRecord.value());
        assertNull(transformedRecord.valueSchema());
    }

    @Test
    public void insertConfiguredFieldsIntoTombstoneEventWithSchemaLeavesValueUnchanged() {
        final Map<String, Object> props = new HashMap<>();
        props.put("topic.field", "topic_field!");
        props.put("partition.field", "partition_field");
        props.put("timestamp.field", "timestamp_field?");
        props.put("static.field", "instance_id");
        props.put("static.value", "my-instance-id");

        xformValue.configure(props);

        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).build();

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                simpleStructSchema, null);

        final SourceRecord transformedRecord = xformValue.apply(record);

        assertNull(transformedRecord.value());
        assertEquals(simpleStructSchema, transformedRecord.valueSchema());
    }

    @Test
    public void insertKeyFieldsIntoTombstoneEvent() {
        final Map<String, Object> props = new HashMap<>();
        props.put("topic.field", "topic_field!");
        props.put("partition.field", "partition_field");
        props.put("timestamp.field", "timestamp_field?");
        props.put("static.field", "instance_id");
        props.put("static.value", "my-instance-id");

        xformKey.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
            null, Map.of("magic", 42L), null, null);

        final SourceRecord transformedRecord = xformKey.apply(record);

        assertEquals(42L, ((Map<?, ?>) transformedRecord.key()).get("magic"));
        assertEquals("test", ((Map<?, ?>) transformedRecord.key()).get("topic_field"));
        assertEquals(0, ((Map<?, ?>) transformedRecord.key()).get("partition_field"));
        assertNull(((Map<?, ?>) transformedRecord.key()).get("timestamp_field"));
        assertEquals("my-instance-id", ((Map<?, ?>) transformedRecord.key()).get("instance_id"));
        assertNull(transformedRecord.value());
    }

    @Test
    public void insertIntoNullKeyLeavesRecordUnchanged() {
        final Map<String, Object> props = new HashMap<>();
        props.put("topic.field", "topic_field!");
        props.put("partition.field", "partition_field");
        props.put("timestamp.field", "timestamp_field?");
        props.put("static.field", "instance_id");
        props.put("static.value", "my-instance-id");

        xformKey.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
            null, null, null, Map.of("magic", 42L));

        final SourceRecord transformedRecord = xformKey.apply(record);

        assertSame(record, transformedRecord);
    }

    @Test
    public void testInsertFieldVersionRetrievedFromAppInfoParser() {
        assertEquals(AppInfoParser.getVersion(), xformKey.version());
        assertEquals(AppInfoParser.getVersion(), xformValue.version());

        assertEquals(xformKey.version(), xformValue.version());
    }

    @ParameterizedTest
    @MethodSource("data")
    public void testUnsetOptionalField(boolean replaceNullWithDefault, Object expectedValue) {

        final Map<String, Object> props = new HashMap<>();
        props.put("topic.field", "topic_field!");
        props.put("partition.field", "partition_field");
        props.put("timestamp.field", "timestamp_field?");
        props.put("static.field", "instance_id");
        props.put("static.value", "my-instance-id");
        props.put("replace.null.with.default", replaceNullWithDefault);

        xformValue.configure(props);

        Schema magicFieldSchema = SchemaBuilder.int64().optional().defaultValue(42L).build();
        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic_with_default", magicFieldSchema).build();
        final Struct simpleStruct = new Struct(simpleStructSchema).put("magic_with_default", null);

        final SourceRecord record = new SourceRecord(null, null, "test", 0, null, null, simpleStructSchema, simpleStruct, 789L);
        final SourceRecord transformedRecord = xformValue.apply(record);

        assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

        assertEquals(magicFieldSchema, transformedRecord.valueSchema().field("magic_with_default").schema());
        assertEquals(expectedValue, ((Struct) transformedRecord.value()).getInt64("magic_with_default"));

    }

}
