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

import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ProtoBeamConverterTest {
  private static final Schema PROTO3_PRIMITIVE_SCHEMA =
      Schema.builder()
          .addField("primitive_double", Schema.FieldType.DOUBLE)
          .addField("primitive_float", Schema.FieldType.FLOAT)
          .addField("primitive_int32", Schema.FieldType.INT32)
          .addField("primitive_int64", Schema.FieldType.INT64)
          .addField(
              "primitive_uint32",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.UInt32()))
          .addField(
              "primitive_uint64",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.UInt64()))
          .addField(
              "primitive_sint32",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.SInt32()))
          .addField(
              "primitive_sint64",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.SInt64()))
          .addField(
              "primitive_fixed32",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.Fixed32()))
          .addField(
              "primitive_fixed64",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.Fixed64()))
          .addField(
              "primitive_sfixed32",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.SFixed32()))
          .addField(
              "primitive_sfixed64",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.SFixed64()))
          .addField("primitive_bool", Schema.FieldType.BOOLEAN)
          .addField("primitive_string", Schema.FieldType.STRING)
          .addField("primitive_bytes", Schema.FieldType.BYTES)
          .build();
  private static final Schema PROTO3_PRIMITIVE_SCHEMA_SHUFFLED =
      Schema.builder()
          .addField("primitive_bytes", Schema.FieldType.BYTES)
          .addField("primitive_string", Schema.FieldType.STRING)
          .addField("primitive_bool", Schema.FieldType.BOOLEAN)
          .addField(
              "primitive_sfixed64",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.SFixed64()))
          .addField(
              "primitive_sfixed32",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.SFixed32()))
          .addField(
              "primitive_fixed64",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.Fixed64()))
          .addField(
              "primitive_fixed32",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.Fixed32()))
          .addField(
              "primitive_sint64",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.SInt64()))
          .addField(
              "primitive_sint32",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.SInt32()))
          .addField(
              "primitive_uint64",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.UInt64()))
          .addField(
              "primitive_uint32",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.UInt32()))
          .addField("primitive_int64", Schema.FieldType.INT64)
          .addField("primitive_int32", Schema.FieldType.INT32)
          .addField("primitive_float", Schema.FieldType.FLOAT)
          .addField("primitive_double", Schema.FieldType.DOUBLE)
          .build();
  private static final Proto3SchemaMessages.Primitive PROTO3_PRIMITIVE_DEFAULT_MESSAGE =
      Proto3SchemaMessages.Primitive.newBuilder().build();
  private static final Row PROTO3_PRIMITIVE_DEFAULT_ROW =
      Row.withSchema(PROTO3_PRIMITIVE_SCHEMA)
          .addValue(0.0) // double
          .addValue(0f) // float
          .addValue(0) // int32
          .addValue(0L) // int64
          .addValue(0) // uint32
          .addValue(0L) // uint64
          .addValue(0) // sint32
          .addValue(0L) // sint64
          .addValue(0) // fixed32
          .addValue(0L) // fixed64
          .addValue(0) // sfixed32
          .addValue(0L) // sfixed64
          .addValue(false) // bool
          .addValue("") // string
          .addValue(new byte[0]) // bytes
          .build();
  private static final Row PROTO3_PRIMITIVE_DEFAULT_ROW_SHUFFLED =
      Row.withSchema(PROTO3_PRIMITIVE_SCHEMA_SHUFFLED)
          .addValue(new byte[0]) // bytes
          .addValue("") // string
          .addValue(false) // bool
          .addValue(0L) // sfixed64
          .addValue(0) // sfixed32
          .addValue(0L) // fixed64
          .addValue(0) // fixed32
          .addValue(0L) // sint64
          .addValue(0) // sint32
          .addValue(0L) // uint64
          .addValue(0) // uint32
          .addValue(0L) // int64
          .addValue(0) // int32
          .addValue(0f) // float
          .addValue(0.0) // double
          .build();

  private static final Schema PROTO3_OPTIONAL_PRIMITIVE2_SCHEMA =
      Schema.builder()
          .addField("primitive_double", Schema.FieldType.DOUBLE.withNullable(true))
          .addField("primitive_float", Schema.FieldType.FLOAT.withNullable(true))
          .addField("primitive_int32", Schema.FieldType.INT32.withNullable(true))
          .addField("primitive_int64", Schema.FieldType.INT64.withNullable(true))
          .addField(
              "primitive_uint32",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.UInt32()).withNullable(true))
          .addField(
              "primitive_uint64",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.UInt64()).withNullable(true))
          .addField(
              "primitive_sint32",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.SInt32()).withNullable(true))
          .addField(
              "primitive_sint64",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.SInt64()).withNullable(true))
          .addField(
              "primitive_fixed32",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.Fixed32())
                  .withNullable(true))
          .addField(
              "primitive_fixed64",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.Fixed64())
                  .withNullable(true))
          .addField(
              "primitive_sfixed32",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.SFixed32())
                  .withNullable(true))
          .addField(
              "primitive_sfixed64",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.SFixed64())
                  .withNullable(true))
          .addField("primitive_bool", Schema.FieldType.BOOLEAN.withNullable(true))
          .addField("primitive_string", Schema.FieldType.STRING.withNullable(true))
          .addField("primitive_bytes", Schema.FieldType.BYTES.withNullable(true))
          .build();
  private static final Message PROTO3_OPTIONAL_PRIMITIVE2_EMPTY_MESSAGE =
      Proto3SchemaMessages.OptionalPrimitive2.newBuilder().build();
  private static final Message PROTO3_OPTIONAL_PRIMITIVE2_DEFAULT_MESSAGE =
      Proto3SchemaMessages.OptionalPrimitive2.newBuilder()
          .setPrimitiveDouble(0.0)
          .setPrimitiveFloat(0f)
          .setPrimitiveInt32(0)
          .setPrimitiveInt64(0L)
          .setPrimitiveUint32(0)
          .setPrimitiveUint64(0L)
          .setPrimitiveSint32(0)
          .setPrimitiveSint64(0L)
          .setPrimitiveFixed32(0)
          .setPrimitiveFixed64(0L)
          .setPrimitiveSfixed32(0)
          .setPrimitiveSfixed64(0L)
          .setPrimitiveBool(false)
          .setPrimitiveString("")
          .setPrimitiveBytes(ByteString.EMPTY)
          .build();
  private static final Row PROTO3_OPTIONAL_PRIMITIVE2_EMPTY_ROW =
      Row.nullRow(PROTO3_OPTIONAL_PRIMITIVE2_SCHEMA);
  private static final Row PROTO3_OPTIONAL_PRIMITIVE2_DEFAULT_ROW =
      Row.withSchema(PROTO3_OPTIONAL_PRIMITIVE2_SCHEMA)
          .addValue(0.0) // double
          .addValue(0f) // float
          .addValue(0) // int32
          .addValue(0L) // int64
          .addValue(0) // uint32
          .addValue(0L) // uint64
          .addValue(0) // sint32
          .addValue(0L) // sint64
          .addValue(0) // fixed32
          .addValue(0L) // fixed64
          .addValue(0) // sfixed32
          .addValue(0L) // sfixed64
          .addValue(false) // bool
          .addValue("") // string
          .addValue(new byte[0]) // bytes
          .build();

  private static final Message PROTO3_SIMPLE_ONEOF_EMPTY_MESSAGE =
      Proto3SchemaMessages.SimpleOneof.getDefaultInstance();
  private static final Message PROTO3_SIMPLE_ONEOF_INT32_MESSAGE =
      Proto3SchemaMessages.SimpleOneof.newBuilder().setInt32(13).build();
  private static final OneOfType PROTO3_SIMPLE_ONEOF_SCHEMA_GROUP =
      OneOfType.create(
          Schema.Field.of("int32", Schema.FieldType.INT32),
          Schema.Field.of("string", Schema.FieldType.STRING));
  private static final OneOfType PROTO3_SIMPLE_ONEOF_SCHEMA_GROUP_SHUFFLED =
      OneOfType.create(
          Schema.Field.of("string", Schema.FieldType.STRING),
          Schema.Field.of("int32", Schema.FieldType.INT32));
  private static final Schema PROTO3_SIMPLE_ONEOF_SCHEMA =
      Schema.builder()
          .addField(
              "group",
              Schema.FieldType.logicalType(PROTO3_SIMPLE_ONEOF_SCHEMA_GROUP).withNullable(true))
          .build();
  private static final Schema PROTO3_SIMPLE_ONEOF_SCHEMA_SHUFFLED =
      Schema.builder()
          .addField(
              "group",
              Schema.FieldType.logicalType(PROTO3_SIMPLE_ONEOF_SCHEMA_GROUP_SHUFFLED)
                  .withNullable(true))
          .build();
  private static final Row PROTO3_SIMPLE_ONEOF_EMPTY_ROW = Row.nullRow(PROTO3_SIMPLE_ONEOF_SCHEMA);
  private static final Row PROTO3_SIMPLE_ONEOF_INT32_ROW =
      Row.withSchema(PROTO3_SIMPLE_ONEOF_SCHEMA)
          .addValue(PROTO3_SIMPLE_ONEOF_SCHEMA_GROUP.createValue("int32", 13))
          .build();
  private static final Row PROTO3_SIMPLE_ONEOF_INT32_ROW_SHUFFLED =
      Row.withSchema(PROTO3_SIMPLE_ONEOF_SCHEMA_SHUFFLED)
          .addValue(PROTO3_SIMPLE_ONEOF_SCHEMA_GROUP_SHUFFLED.createValue("int32", 13))
          .build();

  private static final Schema PROTO3_WRAP_PRIMITIVE_SCHEMA =
      Schema.builder()
          .addField("double", Schema.FieldType.DOUBLE.withNullable(true))
          .addField("float", Schema.FieldType.FLOAT.withNullable(true))
          .addField("int32", Schema.FieldType.INT32.withNullable(true))
          .addField("int64", Schema.FieldType.INT64.withNullable(true))
          .addField(
              "uint32",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.UInt32()).withNullable(true))
          .addField(
              "uint64",
              Schema.FieldType.logicalType(new ProtoSchemaLogicalTypes.UInt64()).withNullable(true))
          .addField("bool", Schema.FieldType.BOOLEAN.withNullable(true))
          .addField("string", Schema.FieldType.STRING.withNullable(true))
          .addField("bytes", Schema.FieldType.BYTES.withNullable(true))
          .build();
  private static final Message PROTO3_WRAP_PRIMITIVE_EMPTY_MESSAGE =
      Proto3SchemaMessages.WrapPrimitive.getDefaultInstance();
  private static final Message PROTO3_WRAP_PRIMITIVE_DEFAULT_MESSAGE =
      Proto3SchemaMessages.WrapPrimitive.newBuilder()
          .setDouble(DoubleValue.getDefaultInstance())
          .setFloat(FloatValue.getDefaultInstance())
          .setInt32(Int32Value.getDefaultInstance())
          .setInt64(Int64Value.getDefaultInstance())
          .setUint32(UInt32Value.getDefaultInstance())
          .setUint64(UInt64Value.getDefaultInstance())
          .setBool(BoolValue.getDefaultInstance())
          .setString(StringValue.getDefaultInstance())
          .setBytes(BytesValue.getDefaultInstance())
          .build();
  private static final Row PROTO3_WRAP_PRIMITIVE_EMPTY_ROW =
      Row.nullRow(PROTO3_WRAP_PRIMITIVE_SCHEMA);
  private static final Row PROTO3_WRAP_PRIMITIVE_DEFAULT_ROW =
      Row.withSchema(PROTO3_WRAP_PRIMITIVE_SCHEMA)
          .addValue(0.0)
          .addValue(0f)
          .addValue(0)
          .addValue(0L)
          .addValue(0)
          .addValue(0L)
          .addValue(false)
          .addValue("")
          .addValue(new byte[0])
          .build();

  private static final Message PROTO3_NOWRAP_PRIMITIVE_EMPTY_MESSAGE =
      Proto3SchemaMessages.NoWrapPrimitive.getDefaultInstance();
  private static final Message PROTO3_NOWRAP_PRIMITIVE_DEFAULT_MESSAGE =
      Proto3SchemaMessages.NoWrapPrimitive.newBuilder()
          .setDouble(0.0)
          .setFloat(0f)
          .setInt32(0)
          .setInt64(0L)
          .setUint32(0)
          .setUint64(0L)
          .setBool(false)
          .setString("")
          .setBytes(ByteString.EMPTY)
          .build();
  private static final Row PROTO3_NOWRAP_PRIMITIVE_EMPTY_ROW = PROTO3_WRAP_PRIMITIVE_EMPTY_ROW;
  private static final Row PROTO3_NOWRAP_PRIMITIVE_DEFAULT_ROW = PROTO3_WRAP_PRIMITIVE_DEFAULT_ROW;
  private static final Schema PROTO3_NOWRAP_PRIMITIVE_SCHEMA = PROTO3_WRAP_PRIMITIVE_SCHEMA;

  private static final Message PROTO3_ENUM_DEFAULT_MESSAGE =
      Proto3SchemaMessages.EnumMessage.getDefaultInstance();
  private static final Message PROTO3_ENUM_TWO_MESSAGE =
      Proto3SchemaMessages.EnumMessage.newBuilder()
          .setEnum(Proto3SchemaMessages.EnumMessage.Enum.TWO)
          .build();
  private static final EnumerationType PROTO3_ENUM_SCHEMA_ENUM =
      EnumerationType.create(ImmutableMap.of("ZERO", 0, "TWO", 2, "THREE", 3));
  private static final EnumerationType PROTO3_ENUM_SCHEMA_HACKED_ENUM =
      EnumerationType.create(ImmutableMap.of("TEN", 10, "ELEVEN", 11));
  private static final Schema PROTO3_ENUM_SCHEMA =
      Schema.builder()
          .addField("enum", Schema.FieldType.logicalType(PROTO3_ENUM_SCHEMA_ENUM))
          .build();
  private static final Schema PROTO3_ENUM_SCHEMA_HACKED =
      Schema.builder()
          .addField("enum", Schema.FieldType.logicalType(PROTO3_ENUM_SCHEMA_HACKED_ENUM))
          .build();
  private static final Row PROTO3_ENUM_DEFAULT_ROW =
      Row.withSchema(PROTO3_ENUM_SCHEMA).addValue(PROTO3_ENUM_SCHEMA_ENUM.valueOf(0)).build();
  private static final Row PROTO3_ENUM_TWO_ROW =
      Row.withSchema(PROTO3_ENUM_SCHEMA).addValue(PROTO3_ENUM_SCHEMA_ENUM.valueOf("TWO")).build();
  private static final Row PROTO3_ENUM_HACKED_ROW =
      Row.withSchema(PROTO3_ENUM_SCHEMA_HACKED).addValue(new EnumerationType.Value(0)).build();

  @Test
  public void testToProto_Proto3EnumDescriptor_Proto3EnumDefaultRow() {
    Message message =
        ProtoBeamConverter.toProto(Proto3SchemaMessages.EnumMessage.getDescriptor())
            .apply(PROTO3_ENUM_DEFAULT_ROW);
    assertEquals(PROTO3_ENUM_DEFAULT_MESSAGE, message);
  }

  @Test
  public void testToProto_Proto3EnumDescriptor_Proto3EnumHackedRow() {
    Message message =
        ProtoBeamConverter.toProto(Proto3SchemaMessages.EnumMessage.getDescriptor())
            .apply(PROTO3_ENUM_HACKED_ROW);
    assertEquals(PROTO3_ENUM_DEFAULT_MESSAGE, message);
  }

  @Test
  public void testToProto_Proto3EnumDescriptor_Proto3EnumTwoRow() {
    Message message =
        ProtoBeamConverter.toProto(Proto3SchemaMessages.EnumMessage.getDescriptor())
            .apply(PROTO3_ENUM_TWO_ROW);
    assertEquals(PROTO3_ENUM_TWO_MESSAGE, message);
  }

  @Test
  public void testToProto_Proto3NoWrapPrimitiveDescriptor_Proto3NoWrapPrimitiveDefaultRow() {
    Message message =
        ProtoBeamConverter.toProto(Proto3SchemaMessages.NoWrapPrimitive.getDescriptor())
            .apply(PROTO3_NOWRAP_PRIMITIVE_DEFAULT_ROW);
    assertEquals(PROTO3_NOWRAP_PRIMITIVE_DEFAULT_MESSAGE, message);
  }

  @Test
  public void testToProto_Proto3NoWrapPrimitiveDescriptor_Proto3NoWrapPrimitiveEmptyRow() {
    Message message =
        ProtoBeamConverter.toProto(Proto3SchemaMessages.NoWrapPrimitive.getDescriptor())
            .apply(PROTO3_NOWRAP_PRIMITIVE_EMPTY_ROW);
    assertEquals(PROTO3_NOWRAP_PRIMITIVE_EMPTY_MESSAGE, message);
  }

  @Test
  public void testToProto_Proto3OptionalPrimitive2Descriptor_OptionalPrimitive2DefaultRow() {
    Message message =
        ProtoBeamConverter.toProto(Proto3SchemaMessages.OptionalPrimitive2.getDescriptor())
            .apply(PROTO3_OPTIONAL_PRIMITIVE2_DEFAULT_ROW);
    assertEquals(PROTO3_OPTIONAL_PRIMITIVE2_DEFAULT_MESSAGE, message);
  }

  @Test
  public void testToProto_Proto3OptionalPrimitive2Descriptor_OptionalPrimitive2EmptyRow() {
    Message message =
        ProtoBeamConverter.toProto(Proto3SchemaMessages.OptionalPrimitive2.getDescriptor())
            .apply(PROTO3_OPTIONAL_PRIMITIVE2_EMPTY_ROW);
    assertEquals(PROTO3_OPTIONAL_PRIMITIVE2_EMPTY_MESSAGE, message);
  }

  @Test
  public void testToProto_Proto3OptionalPrimitive2Descriptor_Proto3PrimitiveDefaultRow() {
    Message message =
        ProtoBeamConverter.toProto(Proto3SchemaMessages.OptionalPrimitive2.getDescriptor())
            .apply(PROTO3_PRIMITIVE_DEFAULT_ROW);
    assertEquals(PROTO3_OPTIONAL_PRIMITIVE2_DEFAULT_MESSAGE, message);
  }

  @Test
  public void testToProto_Proto3PrimitiveDescriptor_PrimitiveDefaultRowShuffled() {
    Message message =
        ProtoBeamConverter.toProto(Proto3SchemaMessages.Primitive.getDescriptor())
            .apply(PROTO3_PRIMITIVE_DEFAULT_ROW_SHUFFLED);
    assertEquals(PROTO3_PRIMITIVE_DEFAULT_MESSAGE, message);
  }

  @Test
  public void testToProto_Proto3PrimitiveDescriptor_Proto3OptionalPrimitive2DefaultRow() {
    Message message =
        ProtoBeamConverter.toProto(Proto3SchemaMessages.Primitive.getDescriptor())
            .apply(PROTO3_OPTIONAL_PRIMITIVE2_EMPTY_ROW);
    assertEquals(PROTO3_PRIMITIVE_DEFAULT_MESSAGE, message);
  }

  @Test
  public void testToProto_Proto3PrimitiveDescriptor_Proto3OptionalPrimitive2EmptyRow() {
    Message message =
        ProtoBeamConverter.toProto(Proto3SchemaMessages.Primitive.getDescriptor())
            .apply(PROTO3_OPTIONAL_PRIMITIVE2_EMPTY_ROW);
    assertEquals(PROTO3_PRIMITIVE_DEFAULT_MESSAGE, message);
  }

  @Test
  public void testToProto_Proto3PrimitiveDescriptor_Proto3PrimitiveDefaultRow() {
    Message message =
        ProtoBeamConverter.toProto(Proto3SchemaMessages.Primitive.getDescriptor())
            .apply(PROTO3_PRIMITIVE_DEFAULT_ROW);
    assertEquals(PROTO3_PRIMITIVE_DEFAULT_MESSAGE, message);
  }

  @Test
  public void testToProto_Proto3SimpleOneofDescriptor_Proto3SimpleOneofInt32RowShuffled() {
    Message message =
        ProtoBeamConverter.toProto(Proto3SchemaMessages.SimpleOneof.getDescriptor())
            .apply(PROTO3_SIMPLE_ONEOF_INT32_ROW_SHUFFLED);
    assertEquals(PROTO3_SIMPLE_ONEOF_INT32_MESSAGE, message);
  }

  @Test
  public void testToProto_Proto3SimpleOneofDiscriptor_Proto3SimpleOneofEmptyRow() {
    Message message =
        ProtoBeamConverter.toProto(Proto3SchemaMessages.SimpleOneof.getDescriptor())
            .apply(PROTO3_SIMPLE_ONEOF_EMPTY_ROW);

    assertEquals(PROTO3_SIMPLE_ONEOF_EMPTY_MESSAGE, message);
  }

  @Test
  public void testToProto_Proto3SimpleOneofDiscriptor_Proto3SimpleOneofInt32Row() {
    Message message =
        ProtoBeamConverter.toProto(Proto3SchemaMessages.SimpleOneof.getDescriptor())
            .apply(PROTO3_SIMPLE_ONEOF_INT32_ROW);

    assertEquals(PROTO3_SIMPLE_ONEOF_INT32_MESSAGE, message);
  }

  @Test
  public void testToProto_Proto3WrapPrimitiveDescriptor_Proto3WrapPrimitiveDefaultRow() {
    Message message =
        ProtoBeamConverter.toProto(Proto3SchemaMessages.WrapPrimitive.getDescriptor())
            .apply(PROTO3_WRAP_PRIMITIVE_DEFAULT_ROW);
    assertEquals(PROTO3_WRAP_PRIMITIVE_DEFAULT_MESSAGE, message);
  }

  @Test
  public void testToProto_Proto3WrapPrimitiveDescriptor_Proto3WrapPrimitiveEmptyRow() {
    Message message =
        ProtoBeamConverter.toProto(Proto3SchemaMessages.WrapPrimitive.getDescriptor())
            .apply(PROTO3_WRAP_PRIMITIVE_EMPTY_ROW);
    assertEquals(PROTO3_WRAP_PRIMITIVE_EMPTY_MESSAGE, message);
  }

  @Test
  public void testToRow_Prot3EnumSchemaHacked_Prot3EnumDefaultMessage() {
    Row row =
        ProtoBeamConverter.toRow(PROTO3_ENUM_SCHEMA_HACKED).apply(PROTO3_ENUM_DEFAULT_MESSAGE);
    assertEquals(PROTO3_ENUM_HACKED_ROW, row);
  }

  @Test
  public void testToRow_Proto3EnumSchema_Proto3EnumDefaultMessage() {
    Row row = ProtoBeamConverter.toRow(PROTO3_ENUM_SCHEMA).apply(PROTO3_ENUM_DEFAULT_MESSAGE);
    assertEquals(PROTO3_ENUM_DEFAULT_ROW, row);
  }

  @Test
  public void testToRow_Proto3EnumSchema_Proto3EnumTwoMessage() {
    Row row = ProtoBeamConverter.toRow(PROTO3_ENUM_SCHEMA).apply(PROTO3_ENUM_TWO_MESSAGE);
    assertEquals(PROTO3_ENUM_TWO_ROW, row);
  }

  @Test
  public void testToRow_Proto3NoWrapPrimitiveSchema_Proto3NoWrapPrimitiveDefaultMessage() {
    Row row =
        ProtoBeamConverter.toRow(PROTO3_NOWRAP_PRIMITIVE_SCHEMA)
            .apply(PROTO3_NOWRAP_PRIMITIVE_DEFAULT_MESSAGE);
    assertEquals(PROTO3_NOWRAP_PRIMITIVE_DEFAULT_ROW, row);
  }

  @Test
  public void testToRow_Proto3NoWrapPrimitiveSchema_Proto3NoWrapPrimitiveEmptyMessage() {
    Row row =
        ProtoBeamConverter.toRow(PROTO3_NOWRAP_PRIMITIVE_SCHEMA)
            .apply(PROTO3_NOWRAP_PRIMITIVE_EMPTY_MESSAGE);
    assertEquals(PROTO3_NOWRAP_PRIMITIVE_EMPTY_ROW, row);
  }

  @Test
  public void testToRow_Proto3OptionalPrimitive2Schema_OptionalPrimitive2DefaultMessage() {
    Row row =
        ProtoBeamConverter.toRow(PROTO3_OPTIONAL_PRIMITIVE2_SCHEMA)
            .apply(PROTO3_OPTIONAL_PRIMITIVE2_DEFAULT_MESSAGE);
    assertEquals(PROTO3_OPTIONAL_PRIMITIVE2_DEFAULT_ROW, row);
  }

  @Test
  public void testToRow_Proto3OptionalPrimitive2Schema_OptionalPrimitive2EmptyMessage() {
    Row row =
        ProtoBeamConverter.toRow(PROTO3_OPTIONAL_PRIMITIVE2_SCHEMA)
            .apply(PROTO3_OPTIONAL_PRIMITIVE2_EMPTY_MESSAGE);
    assertEquals(PROTO3_OPTIONAL_PRIMITIVE2_EMPTY_ROW, row);
  }

  @Test
  public void testToRow_Proto3OptionalPrimitive2Schema_Proto3PrimitiveDefaultMessage() {
    Row row =
        ProtoBeamConverter.toRow(PROTO3_OPTIONAL_PRIMITIVE2_SCHEMA)
            .apply(PROTO3_PRIMITIVE_DEFAULT_MESSAGE);
    assertEquals(PROTO3_OPTIONAL_PRIMITIVE2_DEFAULT_ROW, row);
  }

  @Test
  public void testToRow_Proto3PrimitiveSchemaShuffle_PrimitiveDefaultMessage() {
    Row row =
        ProtoBeamConverter.toRow(PROTO3_PRIMITIVE_SCHEMA_SHUFFLED)
            .apply(PROTO3_PRIMITIVE_DEFAULT_MESSAGE);
    assertEquals(PROTO3_PRIMITIVE_DEFAULT_ROW_SHUFFLED, row);
  }

  @Test
  public void testToRow_Proto3PrimitiveSchema_Proto3OptionalPrimitive2DefaultMessage() {
    Row row =
        ProtoBeamConverter.toRow(PROTO3_PRIMITIVE_SCHEMA)
            .apply(PROTO3_OPTIONAL_PRIMITIVE2_DEFAULT_MESSAGE);
    assertEquals(PROTO3_PRIMITIVE_DEFAULT_ROW, row);
  }

  @Test
  public void testToRow_Proto3PrimitiveSchema_Proto3OptionalPrimitive2EmtpyMessage() {
    Row row =
        ProtoBeamConverter.toRow(PROTO3_PRIMITIVE_SCHEMA)
            .apply(PROTO3_OPTIONAL_PRIMITIVE2_EMPTY_MESSAGE);
    assertEquals(PROTO3_PRIMITIVE_DEFAULT_ROW, row);
  }

  @Test
  public void testToRow_Proto3PrimitiveSchema_Proto3PrimitiveDefaultMessage() {
    Row row =
        ProtoBeamConverter.toRow(PROTO3_PRIMITIVE_SCHEMA).apply(PROTO3_PRIMITIVE_DEFAULT_MESSAGE);
    assertEquals(PROTO3_PRIMITIVE_DEFAULT_ROW, row);
  }

  @Test
  public void testToRow_Proto3SimpleOneofSchemaShuffled_Proto3SimpleOneofInt32Messsage() {
    Row row =
        ProtoBeamConverter.toRow(PROTO3_SIMPLE_ONEOF_SCHEMA_SHUFFLED)
            .apply(PROTO3_SIMPLE_ONEOF_INT32_MESSAGE);
    assertEquals(PROTO3_SIMPLE_ONEOF_INT32_ROW_SHUFFLED, row);
  }

  @Test
  public void testToRow_Proto3SimpleOneofSchema_Proto3SimpleOneofEmptyMessage() {
    Row row =
        ProtoBeamConverter.toRow(PROTO3_SIMPLE_ONEOF_SCHEMA)
            .apply(PROTO3_SIMPLE_ONEOF_EMPTY_MESSAGE);
    assertEquals(PROTO3_SIMPLE_ONEOF_EMPTY_ROW, row);
  }

  @Test
  public void testToRow_Proto3SimpleOneofSchema_Proto3SimpleOneofInt32Message() {
    Row row =
        ProtoBeamConverter.toRow(PROTO3_SIMPLE_ONEOF_SCHEMA)
            .apply(PROTO3_SIMPLE_ONEOF_INT32_MESSAGE);
    assertEquals(PROTO3_SIMPLE_ONEOF_INT32_ROW, row);
  }

  @Test
  public void testToRow_Proto3WrapPrimitiveSchema_Proto3WrapPrimitiveDefaultMessage() {
    Row row =
        ProtoBeamConverter.toRow(PROTO3_WRAP_PRIMITIVE_SCHEMA)
            .apply(PROTO3_WRAP_PRIMITIVE_DEFAULT_MESSAGE);
    assertEquals(PROTO3_WRAP_PRIMITIVE_DEFAULT_ROW, row);
  }

  @Test
  public void testToRow_Proto3WrapPrimitiveSchema_Proto3WrapPrimitiveEmptyMessage() {
    Row row =
        ProtoBeamConverter.toRow(PROTO3_WRAP_PRIMITIVE_SCHEMA)
            .apply(PROTO3_WRAP_PRIMITIVE_EMPTY_MESSAGE);
    assertEquals(PROTO3_WRAP_PRIMITIVE_EMPTY_ROW, row);
  }
}
