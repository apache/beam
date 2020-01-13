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
}
