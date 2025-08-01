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
package org.apache.kafka.connect.data;

import org.apache.kafka.connect.errors.SchemaBuilderException;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SchemaBuilderTest {
    private static final String NAME = "name";
    private static final Integer VERSION = 2;
    private static final String DOC = "doc";
    private static final Map<String, String> NO_PARAMS = null;

    @Test
    public void testInt8Builder() {
        Schema schema = SchemaBuilder.int8().build();
        assertTypeAndDefault(schema, Schema.Type.INT8, false, null);
        assertNoMetadata(schema);

        schema = SchemaBuilder.int8().name(NAME).optional().defaultValue((byte) 12)
                .version(VERSION).doc(DOC).build();
        assertTypeAndDefault(schema, Schema.Type.INT8, true, (byte) 12);
        assertMetadata(schema, NAME, VERSION, DOC, NO_PARAMS);
    }

    @Test
    public void testInt8BuilderInvalidDefault() {
        assertThrows(SchemaBuilderException.class, () -> SchemaBuilder.int8().defaultValue("invalid"));
    }

    @Test
    public void testInt16Builder() {
        Schema schema = SchemaBuilder.int16().build();
        assertTypeAndDefault(schema, Schema.Type.INT16, false, null);
        assertNoMetadata(schema);

        schema = SchemaBuilder.int16().name(NAME).optional().defaultValue((short) 12)
                .version(VERSION).doc(DOC).build();
        assertTypeAndDefault(schema, Schema.Type.INT16, true, (short) 12);
        assertMetadata(schema, NAME, VERSION, DOC, NO_PARAMS);
    }

    @Test
    public void testInt16BuilderInvalidDefault() {
        assertThrows(SchemaBuilderException.class, () -> SchemaBuilder.int16().defaultValue("invalid"));
    }

    @Test
    public void testInt32Builder() {
        Schema schema = SchemaBuilder.int32().build();
        assertTypeAndDefault(schema, Schema.Type.INT32, false, null);
        assertNoMetadata(schema);

        schema = SchemaBuilder.int32().name(NAME).optional().defaultValue(12)
                .version(VERSION).doc(DOC).build();
        assertTypeAndDefault(schema, Schema.Type.INT32, true, 12);
        assertMetadata(schema, NAME, VERSION, DOC, NO_PARAMS);
    }

    @Test
    public void testInt32BuilderInvalidDefault() {
        assertThrows(SchemaBuilderException.class, () -> SchemaBuilder.int32().defaultValue("invalid"));
    }

    @Test
    public void testInt64Builder() {
        Schema schema = SchemaBuilder.int64().build();
        assertTypeAndDefault(schema, Schema.Type.INT64, false, null);
        assertNoMetadata(schema);

        schema = SchemaBuilder.int64().name(NAME).optional().defaultValue((long) 12)
                .version(VERSION).doc(DOC).build();
        assertTypeAndDefault(schema, Schema.Type.INT64, true, (long) 12);
        assertMetadata(schema, NAME, VERSION, DOC, NO_PARAMS);
    }

    @Test
    public void testInt64BuilderInvalidDefault() {
        assertThrows(SchemaBuilderException.class, () -> SchemaBuilder.int64().defaultValue("invalid"));
    }

    @Test
    public void testFloatBuilder() {
        Schema schema = SchemaBuilder.float32().build();
        assertTypeAndDefault(schema, Schema.Type.FLOAT32, false, null);
        assertNoMetadata(schema);

        schema = SchemaBuilder.float32().name(NAME).optional().defaultValue(12.f)
                .version(VERSION).doc(DOC).build();
        assertTypeAndDefault(schema, Schema.Type.FLOAT32, true, 12.f);
        assertMetadata(schema, NAME, VERSION, DOC, NO_PARAMS);
    }

    @Test
    public void testFloatBuilderInvalidDefault() {
        assertThrows(SchemaBuilderException.class, () -> SchemaBuilder.float32().defaultValue("invalid"));
    }

    @Test
    public void testDoubleBuilder() {
        Schema schema = SchemaBuilder.float64().build();
        assertTypeAndDefault(schema, Schema.Type.FLOAT64, false, null);
        assertNoMetadata(schema);

        schema = SchemaBuilder.float64().name(NAME).optional().defaultValue(12.0)
                .version(VERSION).doc(DOC).build();
        assertTypeAndDefault(schema, Schema.Type.FLOAT64, true, 12.0);
        assertMetadata(schema, NAME, VERSION, DOC, NO_PARAMS);
    }

    @Test
    public void testDoubleBuilderInvalidDefault() {
        assertThrows(SchemaBuilderException.class, () -> SchemaBuilder.float64().defaultValue("invalid"));
    }

    @Test
    public void testBooleanBuilder() {
        Schema schema = SchemaBuilder.bool().build();
        assertTypeAndDefault(schema, Schema.Type.BOOLEAN, false, null);
        assertNoMetadata(schema);

        schema = SchemaBuilder.bool().name(NAME).optional().defaultValue(true)
                .version(VERSION).doc(DOC).build();
        assertTypeAndDefault(schema, Schema.Type.BOOLEAN, true, true);
        assertMetadata(schema, NAME, VERSION, DOC, NO_PARAMS);
    }

    @Test
    public void testBooleanBuilderInvalidDefault() {
        assertThrows(SchemaBuilderException.class, () -> SchemaBuilder.bool().defaultValue("invalid"));
    }

    @Test
    public void testStringBuilder() {
        Schema schema = SchemaBuilder.string().build();
        assertTypeAndDefault(schema, Schema.Type.STRING, false, null);
        assertNoMetadata(schema);

        schema = SchemaBuilder.string().name(NAME).optional().defaultValue("a default string")
                .version(VERSION).doc(DOC).build();
        assertTypeAndDefault(schema, Schema.Type.STRING, true, "a default string");
        assertMetadata(schema, NAME, VERSION, DOC, NO_PARAMS);
    }

    @Test
    public void testStringBuilderInvalidDefault() {
        assertThrows(SchemaBuilderException.class, () -> SchemaBuilder.string().defaultValue(true));
    }

    @Test
    public void testBytesBuilder() {
        Schema schema = SchemaBuilder.bytes().build();
        assertTypeAndDefault(schema, Schema.Type.BYTES, false, null);
        assertNoMetadata(schema);

        schema = SchemaBuilder.bytes().name(NAME).optional().defaultValue("a default byte array".getBytes())
                .version(VERSION).doc(DOC).build();
        assertTypeAndDefault(schema, Schema.Type.BYTES, true, "a default byte array".getBytes());
        assertMetadata(schema, NAME, VERSION, DOC, NO_PARAMS);
    }

    @Test
    public void testBytesBuilderInvalidDefault() {
        assertThrows(SchemaBuilderException.class, () -> SchemaBuilder.bytes().defaultValue("a string, not bytes"));
    }


    @Test
    public void testParameters() {
        Map<String, String> expectedParameters = new HashMap<>();
        expectedParameters.put("foo", "val");
        expectedParameters.put("bar", "baz");

        Schema schema = SchemaBuilder.string().parameter("foo", "val").parameter("bar", "baz").build();
        assertTypeAndDefault(schema, Schema.Type.STRING, false, null);
        assertMetadata(schema, null, null, null, expectedParameters);

        schema = SchemaBuilder.string().parameters(expectedParameters).build();
        assertTypeAndDefault(schema, Schema.Type.STRING, false, null);
        assertMetadata(schema, null, null, null, expectedParameters);
    }


    @Test
    public void testStructBuilder() {
        Schema schema = SchemaBuilder.struct()
                .field("field1", Schema.INT8_SCHEMA)
                .field("field2", Schema.INT8_SCHEMA)
                .build();
        assertTypeAndDefault(schema, Schema.Type.STRUCT, false, null);
        assertEquals(2, schema.fields().size());
        assertEquals("field1", schema.fields().get(0).name());
        assertEquals(0, schema.fields().get(0).index());
        assertEquals(Schema.INT8_SCHEMA, schema.fields().get(0).schema());
        assertEquals("field2", schema.fields().get(1).name());
        assertEquals(1, schema.fields().get(1).index());
        assertEquals(Schema.INT8_SCHEMA, schema.fields().get(1).schema());
        assertNoMetadata(schema);
    }

    @Test
    public void testNonStructCantHaveFields() {
        assertThrows(SchemaBuilderException.class, () -> SchemaBuilder.int8().field("field", SchemaBuilder.int8().build()));
    }


    @Test
    public void testArrayBuilder() {
        Schema schema = SchemaBuilder.array(Schema.INT8_SCHEMA).build();
        assertTypeAndDefault(schema, Schema.Type.ARRAY, false, null);
        assertEquals(Schema.INT8_SCHEMA, schema.valueSchema());
        assertNoMetadata(schema);

        // Default value
        List<Byte> defArray = List.of((byte) 1, (byte) 2);
        schema = SchemaBuilder.array(Schema.INT8_SCHEMA).defaultValue(defArray).build();
        assertTypeAndDefault(schema, Schema.Type.ARRAY, false, defArray);
        assertEquals(Schema.INT8_SCHEMA, schema.valueSchema());
        assertNoMetadata(schema);
    }

    @Test
    public void testArrayBuilderInvalidDefault() {
        // Array, but wrong embedded type
        assertThrows(SchemaBuilderException.class,
            () -> SchemaBuilder.array(Schema.INT8_SCHEMA).defaultValue(List.of("string")).build());
    }

    @Test
    public void testMapBuilder() {
        // SchemaBuilder should also pass the check
        Schema schema = SchemaBuilder.map(Schema.INT8_SCHEMA, Schema.INT8_SCHEMA);
        assertTypeAndDefault(schema, Schema.Type.MAP, false, null);
        assertEquals(Schema.INT8_SCHEMA, schema.keySchema());
        assertEquals(Schema.INT8_SCHEMA, schema.valueSchema());
        assertNoMetadata(schema);

        schema = SchemaBuilder.map(Schema.INT8_SCHEMA, Schema.INT8_SCHEMA).build();
        assertTypeAndDefault(schema, Schema.Type.MAP, false, null);
        assertEquals(Schema.INT8_SCHEMA, schema.keySchema());
        assertEquals(Schema.INT8_SCHEMA, schema.valueSchema());
        assertNoMetadata(schema);

        // Default value
        Map<Byte, Byte> defMap = Map.of((byte) 5, (byte) 10);
        schema = SchemaBuilder.map(Schema.INT8_SCHEMA, Schema.INT8_SCHEMA)
                .defaultValue(defMap).build();
        assertTypeAndDefault(schema, Schema.Type.MAP, false, defMap);
        assertEquals(Schema.INT8_SCHEMA, schema.keySchema());
        assertEquals(Schema.INT8_SCHEMA, schema.valueSchema());
        assertNoMetadata(schema);
    }

    @Test
    public void testMapBuilderInvalidDefault() {
        // Map, but wrong embedded type
        Map<Byte, String> defMap = Map.of((byte) 5, "foo");
        assertThrows(SchemaBuilderException.class, () -> SchemaBuilder.map(Schema.INT8_SCHEMA, Schema.INT8_SCHEMA)
                .defaultValue(defMap).build());
    }

    @Test
    public void testEmptyStruct() {
        final SchemaBuilder emptyStructSchemaBuilder = SchemaBuilder.struct();
        assertEquals(0, emptyStructSchemaBuilder.fields().size());
        new Struct(emptyStructSchemaBuilder);

        final Schema emptyStructSchema = emptyStructSchemaBuilder.build();
        assertEquals(0, emptyStructSchema.fields().size());
        new Struct(emptyStructSchema);
    }

    @Test
    public void testDuplicateFields() {
        assertThrows(SchemaBuilderException.class, () -> SchemaBuilder.struct()
            .name("testing")
            .field("id", SchemaBuilder.string().doc("").build())
            .field("id", SchemaBuilder.string().doc("").build())
            .build());
    }

    @Test
    public void testDefaultFieldsSameValueOverwriting() {
        final SchemaBuilder schemaBuilder = SchemaBuilder.string().name("testing").version(123);

        schemaBuilder.name("testing");
        schemaBuilder.version(123);

        assertEquals("testing", schemaBuilder.name());
    }

    @Test
    public void testDefaultFieldsDifferentValueOverwriting() {
        final SchemaBuilder schemaBuilder = SchemaBuilder.string().name("testing").version(123);

        schemaBuilder.name("testing");
        assertThrows(SchemaBuilderException.class, () -> schemaBuilder.version(456));
    }

    @Test
    public void testFieldNameNull() {
        assertThrows(SchemaBuilderException.class,
            () -> SchemaBuilder.struct().field(null, Schema.STRING_SCHEMA).build());
    }

    @Test
    public void testFieldSchemaNull() {
        assertThrows(SchemaBuilderException.class,
            () -> SchemaBuilder.struct().field("fieldName", null).build());
    }

    @Test
    public void testArraySchemaNull() {
        assertThrows(SchemaBuilderException.class, () -> SchemaBuilder.array(null).build());
    }

    @Test
    public void testMapKeySchemaNull() {
        assertThrows(SchemaBuilderException.class, () -> SchemaBuilder.map(null, Schema.STRING_SCHEMA).build());
    }

    @Test
    public void testMapValueSchemaNull() {
        assertThrows(SchemaBuilderException.class, () -> SchemaBuilder.map(Schema.STRING_SCHEMA, null).build());
    }

    @Test
    public void testTypeNotNull() {
        assertThrows(SchemaBuilderException.class, () -> SchemaBuilder.type(null));
    }

    private void assertTypeAndDefault(Schema schema, Schema.Type type, boolean optional, Object defaultValue) {
        assertEquals(type, schema.type());
        assertEquals(optional, schema.isOptional());
        if (type == Schema.Type.BYTES) {
            // byte[] is not comparable, need to wrap to check correctly
            if (defaultValue == null)
                assertNull(schema.defaultValue());
            else
                assertEquals(ByteBuffer.wrap((byte[]) defaultValue), ByteBuffer.wrap((byte[]) schema.defaultValue()));
        } else {
            assertEquals(defaultValue, schema.defaultValue());
        }
    }

    private void assertMetadata(Schema schema, String name, Integer version, String doc, Map<String, String> parameters) {
        assertEquals(name, schema.name());
        assertEquals(version, schema.version());
        assertEquals(doc, schema.doc());
        assertEquals(parameters, schema.parameters());
    }

    private void assertNoMetadata(Schema schema) {
        assertMetadata(schema, null, null, null, null);
    }
}
