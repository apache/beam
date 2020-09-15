/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.protobuf;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Collection of standard tests for Protobuf Schema support. */
@RunWith(JUnit4.class)
public class ProtoSchemaTranslatorTest {
  @Test
  public void testPrimitiveSchema() {
    assertEquals(
        TestProtoSchemas.PRIMITIVE_SCHEMA,
        ProtoSchemaTranslator.getSchema(Proto3SchemaMessages.Primitive.class));
  }

  @Test
  public void testOptionalPrimitiveSchema() {
    assertEquals(
        TestProtoSchemas.OPTIONAL_PRIMITIVE_SCHEMA,
        ProtoSchemaTranslator.getSchema(Proto2SchemaMessages.OptionalPrimitive.class));
  }

  @Test
  public void testRequiredPrimitiveSchema() {
    assertEquals(
        TestProtoSchemas.REQUIRED_PRIMITIVE_SCHEMA,
        ProtoSchemaTranslator.getSchema(Proto2SchemaMessages.RequiredPrimitive.class));
  }

  @Test
  public void testRepeatedSchema() {
    assertEquals(
        TestProtoSchemas.REPEATED_SCHEMA,
        ProtoSchemaTranslator.getSchema(Proto3SchemaMessages.RepeatPrimitive.class));
  }

  @Test
  public void testMapPrimitiveSchema() {
    assertEquals(
        TestProtoSchemas.MAP_PRIMITIVE_SCHEMA,
        ProtoSchemaTranslator.getSchema(Proto3SchemaMessages.MapPrimitive.class));
  }

  @Test
  public void testNestedSchema() {
    assertEquals(
        TestProtoSchemas.NESTED_SCHEMA,
        ProtoSchemaTranslator.getSchema(Proto3SchemaMessages.Nested.class));
  }

  @Test
  public void testOneOfSchema() {
    assertEquals(
        TestProtoSchemas.ONEOF_SCHEMA,
        ProtoSchemaTranslator.getSchema(Proto3SchemaMessages.OneOf.class));
  }

  @Test
  public void testNestedOneOfSchema() {
    assertEquals(
        TestProtoSchemas.OUTER_ONEOF_SCHEMA,
        ProtoSchemaTranslator.getSchema(Proto3SchemaMessages.OuterOneOf.class));
  }

  @Test
  public void testWrapperMessagesSchema() {
    assertEquals(
        TestProtoSchemas.WKT_MESSAGE_SCHEMA,
        ProtoSchemaTranslator.getSchema(Proto3SchemaMessages.WktMessage.class));
  }

  @Test
  public void testOptionalNestedSchema() {
    assertEquals(
        TestProtoSchemas.OPTIONAL_NESTED_SCHEMA,
        ProtoSchemaTranslator.getSchema(Proto2SchemaMessages.OptionalNested.class));
  }

  @Test
  public void testRequiredNestedSchema() {
    assertEquals(
        TestProtoSchemas.REQUIRED_NESTED_SCHEMA,
        ProtoSchemaTranslator.getSchema(Proto2SchemaMessages.RequiredNested.class));
  }

  @Test
  public void testOptionsInt32OnMessage() {
    Schema schema = ProtoSchemaTranslator.getSchema(Proto3SchemaMessages.OptionMessage.class);
    assertEquals(
        Integer.valueOf(42),
        schema
            .getOptions()
            .getValue("beam:option:proto:message:proto3_schema_options.message_option_int"));
  }

  @Test
  public void testOptionsStringOnMessage() {
    Schema schema = ProtoSchemaTranslator.getSchema(Proto3SchemaMessages.OptionMessage.class);
    assertEquals(
        "this is a message string",
        schema
            .getOptions()
            .getValue("beam:option:proto:message:proto3_schema_options.message_option_string"));
  }

  @Test
  public void testOptionsMessageOnMessage() {
    Schema schema = ProtoSchemaTranslator.getSchema(Proto3SchemaMessages.OptionMessage.class);
    Row optionMessage =
        schema
            .getOptions()
            .getValue("beam:option:proto:message:proto3_schema_options.message_option_message");
    assertEquals("foobar in message", optionMessage.getString("single_string"));
    assertEquals(Integer.valueOf(12), optionMessage.getInt32("single_int32"));
    assertEquals(Long.valueOf(34L), optionMessage.getInt64("single_int64"));
  }

  @Test
  public void testOptionArrayOnMessage() {
    Schema schema = ProtoSchemaTranslator.getSchema(Proto3SchemaMessages.OptionMessage.class);
    List verify =
        new ArrayList(
            (Collection)
                schema
                    .getOptions()
                    .getValue(
                        "beam:option:proto:message:proto3_schema_options.message_option_repeated"));
    assertEquals("string_1", verify.get(0));
    assertEquals("string_2", verify.get(1));
    assertEquals("string_3", verify.get(2));
  }

  @Test
  public void testOptionArrayOfMessagesOnMessage() {
    Schema schema = ProtoSchemaTranslator.getSchema(Proto3SchemaMessages.OptionMessage.class);
    List verify =
        new ArrayList(
            (Collection)
                schema
                    .getOptions()
                    .getValue(
                        "beam:option:proto:message:proto3_schema_options.message_option_repeated_message"));
    assertEquals(
        "string in message in option in message", ((Row) verify.get(0)).getString("single_string"));
    assertEquals(Integer.valueOf(1), ((Row) verify.get(1)).getInt32("single_int32"));
    assertEquals(Long.valueOf(2L), ((Row) verify.get(2)).getInt64("single_int64"));
  }

  @Test
  public void testOptionsInt32OnField() {
    Schema schema = ProtoSchemaTranslator.getSchema(Proto3SchemaMessages.OptionMessage.class);
    Schema.Options options = schema.getField("field_one").getOptions();
    assertEquals(
        Integer.valueOf(13),
        options.getValue("beam:option:proto:field:proto3_schema_options.field_option_int"));
  }

  @Test
  public void testOptionsStringOnField() {
    Schema schema = ProtoSchemaTranslator.getSchema(Proto3SchemaMessages.OptionMessage.class);
    Schema.Options options = schema.getField("field_one").getOptions();
    assertEquals(
        "this is a field string",
        options.getValue("beam:option:proto:field:proto3_schema_options.field_option_string"));
  }

  @Test
  public void testOptionsMessageOnField() {
    Schema schema = ProtoSchemaTranslator.getSchema(Proto3SchemaMessages.OptionMessage.class);
    Schema.Options options = schema.getField("field_one").getOptions();
    Row optionMessage =
        options.getValue("beam:option:proto:field:proto3_schema_options.field_option_message");
    assertEquals("foobar in field", optionMessage.getString("single_string"));
    assertEquals(Integer.valueOf(56), optionMessage.getInt32("single_int32"));
    assertEquals(Long.valueOf(78L), optionMessage.getInt64("single_int64"));
  }

  @Test
  public void testOptionArrayOnField() {
    Schema schema = ProtoSchemaTranslator.getSchema(Proto3SchemaMessages.OptionMessage.class);
    Schema.Options options = schema.getField("field_one").getOptions();
    List verify =
        new ArrayList(
            (Collection)
                options.getValue(
                    "beam:option:proto:field:proto3_schema_options.field_option_repeated"));
    assertEquals("field_string_1", verify.get(0));
    assertEquals("field_string_2", verify.get(1));
    assertEquals("field_string_3", verify.get(2));
  }

  @Test
  public void testOptionArrayOfMessagesOnField() {
    Schema schema = ProtoSchemaTranslator.getSchema(Proto3SchemaMessages.OptionMessage.class);
    Schema.Options options = schema.getField("field_one").getOptions();
    List verify =
        new ArrayList(
            (Collection)
                options.getValue(
                    "beam:option:proto:field:proto3_schema_options.field_option_repeated_message"));
    assertEquals(
        "string in message in option in field", ((Row) verify.get(0)).getString("single_string"));
    assertEquals(Integer.valueOf(77), ((Row) verify.get(1)).getInt32("single_int32"));
    assertEquals(Long.valueOf(88L), ((Row) verify.get(2)).getInt64("single_int64"));
  }
}
