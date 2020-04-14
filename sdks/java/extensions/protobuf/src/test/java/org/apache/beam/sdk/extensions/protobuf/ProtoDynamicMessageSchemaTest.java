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
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.OUTER_ONEOF_PROTO;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.OUTER_ONEOF_ROW;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.OUTER_ONEOF_SCHEMA;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.PRIMITIVE_PROTO;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.PRIMITIVE_ROW;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.PRIMITIVE_SCHEMA;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.REPEATED_PROTO;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.REPEATED_ROW;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.REPEATED_SCHEMA;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.WKT_MESSAGE_PROTO;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.WKT_MESSAGE_ROW;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.WKT_MESSAGE_SCHEMA;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.withFieldNumber;
import static org.apache.beam.sdk.extensions.protobuf.TestProtoSchemas.withTypeName;
import static org.junit.Assert.assertEquals;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.EnumMessage;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.MapPrimitive;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.Nested;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.OneOf;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.OuterOneOf;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.Primitive;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.RepeatPrimitive;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.WktMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Collection of tests for values on Protobuf Messages and Rows. */
@RunWith(JUnit4.class)
public class ProtoDynamicMessageSchemaTest {

  private ProtoDynamicMessageSchema schemaFromDescriptor(Descriptors.Descriptor descriptor) {
    ProtoDomain domain = ProtoDomain.buildFrom(descriptor);
    return ProtoDynamicMessageSchema.forDescriptor(domain, descriptor);
  }

  private DynamicMessage toDynamic(Message message) throws InvalidProtocolBufferException {
    return DynamicMessage.parseFrom(message.getDescriptorForType(), message.toByteArray());
  }

  @Test
  public void testPrimitiveSchema() {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(Primitive.getDescriptor());
    Schema schema = schemaProvider.getSchema();
    assertEquals(PRIMITIVE_SCHEMA, schema);
  }

  @Test
  public void testPrimitiveProtoToRow() throws InvalidProtocolBufferException {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(Primitive.getDescriptor());
    SerializableFunction<DynamicMessage, Row> toRow = schemaProvider.getToRowFunction();
    assertEquals(PRIMITIVE_ROW, toRow.apply(toDynamic(PRIMITIVE_PROTO)));
  }

  @Test
  public void testPrimitiveRowToProto() {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(Primitive.getDescriptor());
    SerializableFunction<Row, DynamicMessage> fromRow = schemaProvider.getFromRowFunction();
    assertEquals(PRIMITIVE_PROTO.toString(), fromRow.apply(PRIMITIVE_ROW).toString());
  }

  @Test
  public void testRepeatedSchema() {
    ProtoDynamicMessageSchema schemaProvider =
        schemaFromDescriptor(RepeatPrimitive.getDescriptor());
    Schema schema = schemaProvider.getSchema();
    assertEquals(REPEATED_SCHEMA, schema);
  }

  @Test
  public void testRepeatedProtoToRow() throws InvalidProtocolBufferException {
    ProtoDynamicMessageSchema schemaProvider =
        schemaFromDescriptor(RepeatPrimitive.getDescriptor());
    SerializableFunction<DynamicMessage, Row> toRow = schemaProvider.getToRowFunction();
    assertEquals(REPEATED_ROW, toRow.apply(toDynamic(REPEATED_PROTO)));
  }

  @Test
  public void testRepeatedRowToProto() {
    ProtoDynamicMessageSchema schemaProvider =
        schemaFromDescriptor(RepeatPrimitive.getDescriptor());
    SerializableFunction<Row, DynamicMessage> fromRow = schemaProvider.getFromRowFunction();
    assertEquals(REPEATED_PROTO.toString(), fromRow.apply(REPEATED_ROW).toString());
  }

  @Test
  public void testNullRepeatedProtoToRow() throws InvalidProtocolBufferException {
    ProtoDynamicMessageSchema schemaProvider =
        schemaFromDescriptor(RepeatPrimitive.getDescriptor());
    SerializableFunction<DynamicMessage, Row> toRow = schemaProvider.getToRowFunction();
    assertEquals(NULL_REPEATED_ROW, toRow.apply(toDynamic(NULL_REPEATED_PROTO)));
  }

  @Test
  public void testNullRepeatedRowToProto() {
    ProtoDynamicMessageSchema schemaProvider =
        schemaFromDescriptor(RepeatPrimitive.getDescriptor());
    SerializableFunction<Row, DynamicMessage> fromRow = schemaProvider.getFromRowFunction();
    assertEquals(NULL_REPEATED_PROTO.toString(), fromRow.apply(NULL_REPEATED_ROW).toString());
  }

  // Test map type
  @Test
  public void testMapSchema() {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(MapPrimitive.getDescriptor());
    Schema schema = schemaProvider.getSchema();
    assertEquals(MAP_PRIMITIVE_SCHEMA, schema);
  }

  @Test
  public void testMapProtoToRow() throws InvalidProtocolBufferException {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(MapPrimitive.getDescriptor());
    SerializableFunction<DynamicMessage, Row> toRow = schemaProvider.getToRowFunction();
    assertEquals(MAP_PRIMITIVE_ROW, toRow.apply(toDynamic(MAP_PRIMITIVE_PROTO)));
  }

  @Test
  public void testMapRowToProto() {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(MapPrimitive.getDescriptor());
    SerializableFunction<Row, DynamicMessage> fromRow = schemaProvider.getFromRowFunction();
    assertEquals(MAP_PRIMITIVE_PROTO.toString(), fromRow.apply(MAP_PRIMITIVE_ROW).toString());
  }

  @Test
  public void testNullMapProtoToRow() throws InvalidProtocolBufferException {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(MapPrimitive.getDescriptor());
    SerializableFunction<DynamicMessage, Row> toRow = schemaProvider.getToRowFunction();
    assertEquals(NULL_MAP_PRIMITIVE_ROW, toRow.apply(toDynamic(NULL_MAP_PRIMITIVE_PROTO)));
  }

  @Test
  public void testNullMapRowToProto() {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(MapPrimitive.getDescriptor());
    SerializableFunction<Row, DynamicMessage> fromRow = schemaProvider.getFromRowFunction();
    assertEquals(
        NULL_MAP_PRIMITIVE_PROTO.toString(), fromRow.apply(NULL_MAP_PRIMITIVE_ROW).toString());
  }

  @Test
  public void testNestedSchema() {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(Nested.getDescriptor());
    Schema schema = schemaProvider.getSchema();
    assertEquals(NESTED_SCHEMA, schema);
  }

  @Test
  public void testNestedProtoToRow() throws InvalidProtocolBufferException {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(Nested.getDescriptor());
    SerializableFunction<DynamicMessage, Row> toRow = schemaProvider.getToRowFunction();
    assertEquals(NESTED_ROW, toRow.apply(toDynamic(NESTED_PROTO)));
  }

  @Test
  public void testNestedRowToProto() throws InvalidProtocolBufferException {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(Nested.getDescriptor());
    SerializableFunction<Row, DynamicMessage> fromRow = schemaProvider.getFromRowFunction();
    // equality doesn't work between dynamic messages and other,
    // so we compare string representation
    assertEquals(NESTED_PROTO.toString(), fromRow.apply(NESTED_ROW).toString());
  }

  @Test
  public void testOneOfSchema() {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(OneOf.getDescriptor());
    Schema schema = schemaProvider.getSchema();
    assertEquals(ONEOF_SCHEMA, schema);
  }

  @Test
  public void testOneOfProtoToRow() throws InvalidProtocolBufferException {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(OneOf.getDescriptor());
    SerializableFunction<DynamicMessage, Row> toRow = schemaProvider.getToRowFunction();
    // equality doesn't work between dynamic messages and other,
    // so we compare string representation
    assertEquals(ONEOF_ROW_INT32.toString(), toRow.apply(toDynamic(ONEOF_PROTO_INT32)).toString());
    assertEquals(ONEOF_ROW_BOOL.toString(), toRow.apply(toDynamic(ONEOF_PROTO_BOOL)).toString());
    assertEquals(
        ONEOF_ROW_STRING.toString(), toRow.apply(toDynamic(ONEOF_PROTO_STRING)).toString());
    assertEquals(
        ONEOF_ROW_PRIMITIVE.toString(), toRow.apply(toDynamic(ONEOF_PROTO_PRIMITIVE)).toString());
  }

  @Test
  public void testOneOfRowToProto() {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(OneOf.getDescriptor());
    SerializableFunction<Row, DynamicMessage> fromRow = schemaProvider.getFromRowFunction();
    assertEquals(ONEOF_PROTO_INT32.toString(), fromRow.apply(ONEOF_ROW_INT32).toString());
    assertEquals(ONEOF_PROTO_BOOL.toString(), fromRow.apply(ONEOF_ROW_BOOL).toString());
    assertEquals(ONEOF_PROTO_STRING.toString(), fromRow.apply(ONEOF_ROW_STRING).toString());
    assertEquals(ONEOF_PROTO_PRIMITIVE.toString(), fromRow.apply(ONEOF_ROW_PRIMITIVE).toString());
  }

  @Test
  public void testOuterOneOfSchema() {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(OuterOneOf.getDescriptor());
    Schema schema = schemaProvider.getSchema();
    assertEquals(OUTER_ONEOF_SCHEMA, schema);
  }

  @Test
  public void testOuterOneOfProtoToRow() throws InvalidProtocolBufferException {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(OuterOneOf.getDescriptor());
    SerializableFunction<DynamicMessage, Row> toRow = schemaProvider.getToRowFunction();
    // equality doesn't work between dynamic messages and other,
    // so we compare string representation
    assertEquals(OUTER_ONEOF_ROW.toString(), toRow.apply(toDynamic(OUTER_ONEOF_PROTO)).toString());
  }

  @Test
  public void testOuterOneOfRowToProto() {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(OuterOneOf.getDescriptor());
    SerializableFunction<Row, DynamicMessage> fromRow = schemaProvider.getFromRowFunction();
    assertEquals(OUTER_ONEOF_PROTO.toString(), fromRow.apply(OUTER_ONEOF_ROW).toString());
  }

  private static final EnumerationType ENUM_TYPE =
      EnumerationType.create(ImmutableMap.of("ZERO", 0, "TWO", 2, "THREE", 3));
  private static final Schema ENUM_SCHEMA =
      Schema.builder()
          .addField(
              withFieldNumber("enum", Schema.FieldType.logicalType(ENUM_TYPE), 1)
                  .withNullable(false))
          .setOptions(withTypeName("proto3_schema_messages.EnumMessage"))
          .build();
  private static final Row ENUM_ROW =
      Row.withSchema(ENUM_SCHEMA).addValues(ENUM_TYPE.valueOf("TWO")).build();
  private static final EnumMessage ENUM_PROTO =
      EnumMessage.newBuilder().setEnum(EnumMessage.Enum.TWO).build();

  @Test
  public void testEnumSchema() {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(EnumMessage.getDescriptor());
    Schema schema = schemaProvider.getSchema();
    assertEquals(ENUM_SCHEMA, schema);
  }

  @Test
  public void testEnumProtoToRow() throws InvalidProtocolBufferException {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(EnumMessage.getDescriptor());
    SerializableFunction<DynamicMessage, Row> toRow = schemaProvider.getToRowFunction();
    assertEquals(ENUM_ROW, toRow.apply(toDynamic(ENUM_PROTO)));
  }

  @Test
  public void testEnumRowToProto() {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(EnumMessage.getDescriptor());
    SerializableFunction<Row, DynamicMessage> fromRow = schemaProvider.getFromRowFunction();
    assertEquals(ENUM_PROTO.toString(), fromRow.apply(ENUM_ROW).toString());
  }

  @Test
  public void testWktMessageSchema() {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(WktMessage.getDescriptor());
    Schema schema = schemaProvider.getSchema();
    assertEquals(WKT_MESSAGE_SCHEMA, schema);
  }

  @Test
  public void testWktProtoToRow() throws InvalidProtocolBufferException {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(WktMessage.getDescriptor());
    SerializableFunction<DynamicMessage, Row> toRow = schemaProvider.getToRowFunction();
    assertEquals(WKT_MESSAGE_ROW, toRow.apply(toDynamic(WKT_MESSAGE_PROTO)));
  }

  @Test
  public void testWktRowToProto() {
    ProtoDynamicMessageSchema schemaProvider = schemaFromDescriptor(WktMessage.getDescriptor());
    SerializableFunction<Row, DynamicMessage> fromRow = schemaProvider.getFromRowFunction();
    assertEquals(WKT_MESSAGE_PROTO.toString(), fromRow.apply(WKT_MESSAGE_ROW).toString());
  }
}
