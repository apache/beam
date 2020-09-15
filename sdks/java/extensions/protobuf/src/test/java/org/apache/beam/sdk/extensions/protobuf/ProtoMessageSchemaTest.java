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

import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.MAP_PRIMITIVE_PROTO;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.MAP_PRIMITIVE_ROW;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.MAP_PRIMITIVE_SCHEMA;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.NESTED_PROTO;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.NESTED_ROW;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.NESTED_SCHEMA;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.NULL_MAP_PRIMITIVE_PROTO;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.NULL_MAP_PRIMITIVE_ROW;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.NULL_REPEATED_PROTO;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.NULL_REPEATED_ROW;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.ONEOF_PROTO_BOOL;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.ONEOF_PROTO_INT32;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.ONEOF_PROTO_PRIMITIVE;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.ONEOF_PROTO_STRING;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.ONEOF_ROW_BOOL;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.ONEOF_ROW_INT32;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.ONEOF_ROW_PRIMITIVE;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.ONEOF_ROW_STRING;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.ONEOF_SCHEMA;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.OPTIONAL_PRIMITIVE_PROTO;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.OPTIONAL_PRIMITIVE_ROW;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.OPTIONAL_PRIMITIVE_SCHEMA;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.OUTER_ONEOF_PROTO;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.OUTER_ONEOF_ROW;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.OUTER_ONEOF_SCHEMA;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.PRIMITIVE_PROTO;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.PRIMITIVE_ROW;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.PRIMITIVE_SCHEMA;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.REPEATED_PROTO;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.REPEATED_ROW;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.REPEATED_SCHEMA;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.REQUIRED_PRIMITIVE_PROTO;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.REQUIRED_PRIMITIVE_ROW;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.REQUIRED_PRIMITIVE_SCHEMA;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.WKT_MESSAGE_PROTO;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.WKT_MESSAGE_ROW;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.WKT_MESSAGE_SCHEMA;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.withFieldNumber;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.withTypeName;
import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.extensions.protobuf.Proto2SchemaMessages.OptionalPrimitive;
import org.apache.beam.sdk.extensions.protobuf.Proto2SchemaMessages.RequiredPrimitive;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.EnumMessage;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.MapPrimitive;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.Nested;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.OneOf;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.OuterOneOf;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.Primitive;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.RepeatPrimitive;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.WktMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ProtoMessageSchemaTest {

  @Test
  public void testPrimitiveSchema() {
    Schema schema = new ProtoMessageSchema().schemaFor(TypeDescriptor.of(Primitive.class));
    assertEquals(PRIMITIVE_SCHEMA, schema);
  }

  @Test
  public void testPrimitiveProtoToRow() {
    SerializableFunction<Primitive, Row> toRow =
        new ProtoMessageSchema().toRowFunction(TypeDescriptor.of(Primitive.class));
    assertEquals(PRIMITIVE_ROW, toRow.apply(PRIMITIVE_PROTO));
  }

  @Test
  public void testPrimitiveRowToProto() {
    SerializableFunction<Row, Primitive> fromRow =
        new ProtoMessageSchema().fromRowFunction(TypeDescriptor.of(Primitive.class));
    assertEquals(PRIMITIVE_PROTO, fromRow.apply(PRIMITIVE_ROW));
  }

  @Test
  public void testOptionalPrimitiveSchema() {
    Schema schema = new ProtoMessageSchema().schemaFor(TypeDescriptor.of(OptionalPrimitive.class));
    assertEquals(OPTIONAL_PRIMITIVE_SCHEMA, schema);
  }

  @Test
  public void testOptionalPrimitiveProtoToRow() {
    SerializableFunction<OptionalPrimitive, Row> toRow =
        new ProtoMessageSchema().toRowFunction(TypeDescriptor.of(OptionalPrimitive.class));
    assertEquals(OPTIONAL_PRIMITIVE_ROW, toRow.apply(OPTIONAL_PRIMITIVE_PROTO));
  }

  @Test
  public void testOptionalPrimitiveRowToProto() {
    SerializableFunction<Row, OptionalPrimitive> fromRow =
        new ProtoMessageSchema().fromRowFunction(TypeDescriptor.of(OptionalPrimitive.class));
    assertEquals(OPTIONAL_PRIMITIVE_PROTO, fromRow.apply(OPTIONAL_PRIMITIVE_ROW));
  }

  @Test
  public void testRequiredPrimitiveSchema() {
    Schema schema = new ProtoMessageSchema().schemaFor(TypeDescriptor.of(RequiredPrimitive.class));
    assertEquals(REQUIRED_PRIMITIVE_SCHEMA, schema);
  }

  @Test
  public void testRequiredPrimitiveProtoToRow() {
    SerializableFunction<RequiredPrimitive, Row> toRow =
        new ProtoMessageSchema().toRowFunction(TypeDescriptor.of(RequiredPrimitive.class));
    assertEquals(REQUIRED_PRIMITIVE_ROW, toRow.apply(REQUIRED_PRIMITIVE_PROTO));
  }

  @Test
  public void testRequiredPrimitiveRowToProto() {
    SerializableFunction<Row, RequiredPrimitive> fromRow =
        new ProtoMessageSchema().fromRowFunction(TypeDescriptor.of(RequiredPrimitive.class));
    assertEquals(REQUIRED_PRIMITIVE_PROTO, fromRow.apply(REQUIRED_PRIMITIVE_ROW));
  }

  @Test
  public void testRepeatedSchema() {
    Schema schema = new ProtoMessageSchema().schemaFor(TypeDescriptor.of(RepeatPrimitive.class));
    assertEquals(REPEATED_SCHEMA, schema);
  }

  @Test
  public void testRepeatedProtoToRow() {
    SerializableFunction<RepeatPrimitive, Row> toRow =
        new ProtoMessageSchema().toRowFunction(TypeDescriptor.of(RepeatPrimitive.class));
    assertEquals(REPEATED_ROW, toRow.apply(REPEATED_PROTO));
  }

  @Test
  public void testRepeatedRowToProto() {
    SerializableFunction<Row, RepeatPrimitive> fromRow =
        new ProtoMessageSchema().fromRowFunction(TypeDescriptor.of(RepeatPrimitive.class));
    assertEquals(REPEATED_PROTO, fromRow.apply(REPEATED_ROW));
  }

  @Test
  public void testNullRepeatedProtoToRow() {
    SerializableFunction<RepeatPrimitive, Row> toRow =
        new ProtoMessageSchema().toRowFunction(TypeDescriptor.of(RepeatPrimitive.class));
    assertEquals(NULL_REPEATED_ROW, toRow.apply(NULL_REPEATED_PROTO));
  }

  @Test
  public void testNullRepeatedRowToProto() {
    SerializableFunction<Row, RepeatPrimitive> fromRow =
        new ProtoMessageSchema().fromRowFunction(TypeDescriptor.of(RepeatPrimitive.class));
    assertEquals(NULL_REPEATED_PROTO, fromRow.apply(NULL_REPEATED_ROW));
  }

  // Test map type
  @Test
  public void testMapSchema() {
    Schema schema = new ProtoMessageSchema().schemaFor(TypeDescriptor.of(MapPrimitive.class));
    assertEquals(MAP_PRIMITIVE_SCHEMA, schema);
  }

  @Test
  public void testMapProtoToRow() {
    SerializableFunction<MapPrimitive, Row> toRow =
        new ProtoMessageSchema().toRowFunction(TypeDescriptor.of(MapPrimitive.class));
    assertEquals(MAP_PRIMITIVE_ROW, toRow.apply(MAP_PRIMITIVE_PROTO));
  }

  @Test
  public void testMapRowToProto() {
    SerializableFunction<Row, MapPrimitive> fromRow =
        new ProtoMessageSchema().fromRowFunction(TypeDescriptor.of(MapPrimitive.class));
    assertEquals(MAP_PRIMITIVE_PROTO, fromRow.apply(MAP_PRIMITIVE_ROW));
  }

  @Test
  public void testNullMapProtoToRow() {
    SerializableFunction<MapPrimitive, Row> toRow =
        new ProtoMessageSchema().toRowFunction(TypeDescriptor.of(MapPrimitive.class));
    assertEquals(NULL_MAP_PRIMITIVE_ROW, toRow.apply(NULL_MAP_PRIMITIVE_PROTO));
  }

  @Test
  public void testNullMapRowToProto() {
    SerializableFunction<Row, MapPrimitive> fromRow =
        new ProtoMessageSchema().fromRowFunction(TypeDescriptor.of(MapPrimitive.class));
    assertEquals(NULL_MAP_PRIMITIVE_PROTO, fromRow.apply(NULL_MAP_PRIMITIVE_ROW));
  }

  @Test
  public void testNestedSchema() {
    Schema schema = new ProtoMessageSchema().schemaFor(TypeDescriptor.of(Nested.class));
    assertEquals(NESTED_SCHEMA, schema);
  }

  @Test
  public void testNestedProtoToRow() {
    SerializableFunction<Nested, Row> toRow =
        new ProtoMessageSchema().toRowFunction(TypeDescriptor.of(Nested.class));
    assertEquals(NESTED_ROW, toRow.apply(NESTED_PROTO));
  }

  @Test
  public void testNestedRowToProto() {
    SerializableFunction<Row, Nested> fromRow =
        new ProtoMessageSchema().fromRowFunction(TypeDescriptor.of(Nested.class));
    assertEquals(NESTED_PROTO, fromRow.apply(NESTED_ROW));
  }

  @Test
  public void testOneOfSchema() {
    Schema schema = new ProtoMessageSchema().schemaFor(TypeDescriptor.of(OneOf.class));
    assertEquals(ONEOF_SCHEMA, schema);
  }

  @Test
  public void testOneOfProtoToRow() {
    SerializableFunction<OneOf, Row> toRow =
        new ProtoMessageSchema().toRowFunction(TypeDescriptor.of(OneOf.class));
    assertEquals(ONEOF_ROW_INT32, toRow.apply(ONEOF_PROTO_INT32));
    assertEquals(ONEOF_ROW_BOOL, toRow.apply(ONEOF_PROTO_BOOL));
    assertEquals(ONEOF_ROW_STRING, toRow.apply(ONEOF_PROTO_STRING));
    assertEquals(ONEOF_ROW_PRIMITIVE, toRow.apply(ONEOF_PROTO_PRIMITIVE));
  }

  @Test
  public void testOneOfRowToProto() {
    SerializableFunction<Row, OneOf> fromRow =
        new ProtoMessageSchema().fromRowFunction(TypeDescriptor.of(OneOf.class));
    assertEquals(ONEOF_PROTO_INT32, fromRow.apply(ONEOF_ROW_INT32));
    assertEquals(ONEOF_PROTO_BOOL, fromRow.apply(ONEOF_ROW_BOOL));
    assertEquals(ONEOF_PROTO_STRING, fromRow.apply(ONEOF_ROW_STRING));
    assertEquals(ONEOF_PROTO_PRIMITIVE, fromRow.apply(ONEOF_ROW_PRIMITIVE));
  }

  @Test
  public void testOuterOneOfSchema() {
    Schema schema = new ProtoMessageSchema().schemaFor(TypeDescriptor.of(OuterOneOf.class));
    assertEquals(OUTER_ONEOF_SCHEMA, schema);
  }

  @Test
  public void testOuterOneOfProtoToRow() {
    SerializableFunction<OuterOneOf, Row> toRow =
        new ProtoMessageSchema().toRowFunction(TypeDescriptor.of(OuterOneOf.class));
    assertEquals(OUTER_ONEOF_ROW, toRow.apply(OUTER_ONEOF_PROTO));
  }

  @Test
  public void testOuterOneOfRowToProto() {
    SerializableFunction<Row, OuterOneOf> fromRow =
        new ProtoMessageSchema().fromRowFunction(TypeDescriptor.of(OuterOneOf.class));
    assertEquals(OUTER_ONEOF_PROTO, fromRow.apply(OUTER_ONEOF_ROW));
  }

  private static final EnumerationType ENUM_TYPE =
      EnumerationType.create(ImmutableMap.of("ZERO", 0, "TWO", 2, "THREE", 3));
  private static final Schema ENUM_SCHEMA =
      Schema.builder()
          .addField(withFieldNumber("enum", FieldType.logicalType(ENUM_TYPE), 1))
          .setOptions(withTypeName("proto3_schema_messages.EnumMessage"))
          .build();
  private static final Row ENUM_ROW =
      Row.withSchema(ENUM_SCHEMA).addValues(ENUM_TYPE.valueOf("TWO")).build();
  private static final EnumMessage ENUM_PROTO =
      EnumMessage.newBuilder().setEnum(EnumMessage.Enum.TWO).build();

  @Test
  public void testEnumSchema() {
    Schema schema = new ProtoMessageSchema().schemaFor(TypeDescriptor.of(EnumMessage.class));
    assertEquals(ENUM_SCHEMA, schema);
  }

  @Test
  public void testEnumProtoToRow() {
    SerializableFunction<EnumMessage, Row> toRow =
        new ProtoMessageSchema().toRowFunction(TypeDescriptor.of(EnumMessage.class));
    assertEquals(ENUM_ROW, toRow.apply(ENUM_PROTO));
  }

  @Test
  public void testEnumRowToProto() {
    SerializableFunction<Row, EnumMessage> fromRow =
        new ProtoMessageSchema().fromRowFunction(TypeDescriptor.of(EnumMessage.class));
    assertEquals(ENUM_PROTO, fromRow.apply(ENUM_ROW));
  }

  @Test
  public void testWktMessageSchema() {
    Schema schema = new ProtoMessageSchema().schemaFor(TypeDescriptor.of(WktMessage.class));
    assertEquals(WKT_MESSAGE_SCHEMA, schema);
  }

  @Test
  public void testWktProtoToRow() {
    SerializableFunction<WktMessage, Row> toRow =
        new ProtoMessageSchema().toRowFunction(TypeDescriptor.of(WktMessage.class));
    assertEquals(WKT_MESSAGE_ROW, toRow.apply(WKT_MESSAGE_PROTO));
  }

  @Test
  public void testWktRowToProto() {
    SerializableFunction<Row, WktMessage> fromRow =
        new ProtoMessageSchema().fromRowFunction(TypeDescriptor.of(WktMessage.class));
    assertEquals(WKT_MESSAGE_PROTO, fromRow.apply(WKT_MESSAGE_ROW));
  }
}
