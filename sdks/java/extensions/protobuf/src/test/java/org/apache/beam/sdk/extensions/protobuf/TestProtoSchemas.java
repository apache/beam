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

import static org.apache.beam.sdk.extensions.protobuf.ProtoSchemaTranslator.SCHEMA_OPTION_META_NUMBER;
import static org.apache.beam.sdk.extensions.protobuf.ProtoSchemaTranslator.SCHEMA_OPTION_META_TYPE_NAME;
import static org.apache.beam.sdk.extensions.protobuf.ProtoSchemaTranslator.getFieldNumber;

import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Duration;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.protobuf.Proto2SchemaMessages.OptionalNested;
import org.apache.beam.sdk.extensions.protobuf.Proto2SchemaMessages.OptionalPrimitive;
import org.apache.beam.sdk.extensions.protobuf.Proto2SchemaMessages.RequiredNested;
import org.apache.beam.sdk.extensions.protobuf.Proto2SchemaMessages.RequiredPrimitive;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.MapPrimitive;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.Nested;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.NonContiguousOneOf;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.OneOf;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.OuterOneOf;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.Primitive;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.RepeatPrimitive;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.ReversedOneOf;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.WktMessage;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.Fixed32;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.Fixed64;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.SFixed32;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.SFixed64;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.SInt32;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.SInt64;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.UInt32;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.UInt64;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.NanosDuration;
import org.apache.beam.sdk.schemas.logicaltypes.NanosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

class TestProtoSchemas {

  static Field withFieldNumber(String name, FieldType fieldType, int fieldNumber) {
    return Field.of(name, fieldType)
        .withOptions(
            Schema.Options.builder()
                .setOption(SCHEMA_OPTION_META_NUMBER, FieldType.INT32, fieldNumber));
  }

  static Schema.Options withTypeName(String typeName) {
    return Schema.Options.builder()
        .setOption(SCHEMA_OPTION_META_TYPE_NAME, FieldType.STRING, typeName)
        .build();
  }

  // The schema we expect from the Primitive proto.
  static final Schema PRIMITIVE_SCHEMA =
      Schema.builder()
          .addField(withFieldNumber("primitive_double", FieldType.DOUBLE, 1))
          .addField(withFieldNumber("primitive_float", FieldType.FLOAT, 2))
          .addField(withFieldNumber("primitive_int32", FieldType.INT32, 3))
          .addField(withFieldNumber("primitive_int64", FieldType.INT64, 4))
          .addField(withFieldNumber("primitive_uint32", FieldType.logicalType(new UInt32()), 5))
          .addField(withFieldNumber("primitive_uint64", FieldType.logicalType(new UInt64()), 6))
          .addField(withFieldNumber("primitive_sint32", FieldType.logicalType(new SInt32()), 7))
          .addField(withFieldNumber("primitive_sint64", FieldType.logicalType(new SInt64()), 8))
          .addField(withFieldNumber("primitive_fixed32", FieldType.logicalType(new Fixed32()), 9))
          .addField(withFieldNumber("primitive_fixed64", FieldType.logicalType(new Fixed64()), 10))
          .addField(
              withFieldNumber("primitive_sfixed32", FieldType.logicalType(new SFixed32()), 11))
          .addField(
              withFieldNumber("primitive_sfixed64", FieldType.logicalType(new SFixed64()), 12))
          .addField(withFieldNumber("primitive_bool", FieldType.BOOLEAN, 13))
          .addField(withFieldNumber("primitive_string", FieldType.STRING, 14))
          .addField(withFieldNumber("primitive_bytes", FieldType.BYTES, 15))
          .setOptions(
              Schema.Options.builder()
                  .setOption(
                      SCHEMA_OPTION_META_TYPE_NAME,
                      FieldType.STRING,
                      "proto3_schema_messages.Primitive"))
          .build();

  static final Schema OPTIONAL_PRIMITIVE_SCHEMA =
      Schema.builder()
          .addField(withFieldNumber("primitive_int32", FieldType.INT32, 1))
          .addField(withFieldNumber("primitive_bool", FieldType.BOOLEAN, 2))
          .addField(withFieldNumber("primitive_string", FieldType.STRING, 3))
          .addField(withFieldNumber("primitive_bytes", FieldType.BYTES, 4))
          .setOptions(
              Schema.Options.builder()
                  .setOption(
                      SCHEMA_OPTION_META_TYPE_NAME,
                      FieldType.STRING,
                      "proto2_schema_messages.OptionalPrimitive"))
          .build();

  static final Schema REQUIRED_PRIMITIVE_SCHEMA =
      Schema.builder()
          .addField(withFieldNumber("primitive_int32", FieldType.INT32, 1))
          .addField(withFieldNumber("primitive_bool", FieldType.BOOLEAN, 2))
          .addField(withFieldNumber("primitive_string", FieldType.STRING, 3))
          .addField(withFieldNumber("primitive_bytes", FieldType.BYTES, 4))
          .setOptions(
              Schema.Options.builder()
                  .setOption(
                      SCHEMA_OPTION_META_TYPE_NAME,
                      FieldType.STRING,
                      "proto2_schema_messages.RequiredPrimitive"))
          .build();

  // A sample instance of the  row.
  private static final byte[] BYTE_ARRAY = new byte[] {1, 2, 3, 4};
  static final Row PRIMITIVE_ROW =
      Row.withSchema(PRIMITIVE_SCHEMA)
          .addValues(
              1.1, 2.2F, 32, 64L, 33, 65L, 123, 124L, 30, 62L, 31, 63L, true, "horsey", BYTE_ARRAY)
          .build();

  // A sample instance of the proto.
  static final Primitive PRIMITIVE_PROTO =
      Primitive.newBuilder()
          .setPrimitiveDouble(1.1)
          .setPrimitiveFloat(2.2F)
          .setPrimitiveInt32(32)
          .setPrimitiveInt64(64)
          .setPrimitiveUint32(33)
          .setPrimitiveUint64(65)
          .setPrimitiveSint32(123)
          .setPrimitiveSint64(124)
          .setPrimitiveFixed32(30)
          .setPrimitiveFixed64(62)
          .setPrimitiveSfixed32(31)
          .setPrimitiveSfixed64(63)
          .setPrimitiveBool(true)
          .setPrimitiveString("horsey")
          .setPrimitiveBytes(ByteString.copyFrom(BYTE_ARRAY))
          .build();

  // A sample instance of the  row.
  static final Row OPTIONAL_PRIMITIVE_ROW =
      Row.withSchema(OPTIONAL_PRIMITIVE_SCHEMA).addValues(32, true, "horsey", BYTE_ARRAY).build();

  // A sample instance of the proto.
  static final OptionalPrimitive OPTIONAL_PRIMITIVE_PROTO =
      OptionalPrimitive.newBuilder()
          .setPrimitiveInt32(32)
          .setPrimitiveBool(true)
          .setPrimitiveString("horsey")
          .setPrimitiveBytes(ByteString.copyFrom(BYTE_ARRAY))
          .build();

  // A sample instance of the  row.
  static final Row REQUIRED_PRIMITIVE_ROW =
      Row.withSchema(REQUIRED_PRIMITIVE_SCHEMA).addValues(32, true, "horsey", BYTE_ARRAY).build();

  // A sample instance of the proto.
  static final RequiredPrimitive REQUIRED_PRIMITIVE_PROTO =
      RequiredPrimitive.newBuilder()
          .setPrimitiveInt32(32)
          .setPrimitiveBool(true)
          .setPrimitiveString("horsey")
          .setPrimitiveBytes(ByteString.copyFrom(BYTE_ARRAY))
          .build();

  // The schema for the RepeatedPrimitive proto.
  static final Schema REPEATED_SCHEMA =
      Schema.builder()
          .addField(withFieldNumber("repeated_double", FieldType.array(FieldType.DOUBLE), 1))
          .addField(withFieldNumber("repeated_float", FieldType.array(FieldType.FLOAT), 2))
          .addField(withFieldNumber("repeated_int32", FieldType.array(FieldType.INT32), 3))
          .addField(withFieldNumber("repeated_int64", FieldType.array(FieldType.INT64), 4))
          .addField(
              withFieldNumber(
                  "repeated_uint32", FieldType.array(FieldType.logicalType(new UInt32())), 5))
          .addField(
              withFieldNumber(
                  "repeated_uint64", FieldType.array(FieldType.logicalType(new UInt64())), 6))
          .addField(
              withFieldNumber(
                  "repeated_sint32", FieldType.array(FieldType.logicalType(new SInt32())), 7))
          .addField(
              withFieldNumber(
                  "repeated_sint64", FieldType.array(FieldType.logicalType(new SInt64())), 8))
          .addField(
              withFieldNumber(
                  "repeated_fixed32", FieldType.array(FieldType.logicalType(new Fixed32())), 9))
          .addField(
              withFieldNumber(
                  "repeated_fixed64", FieldType.array(FieldType.logicalType(new Fixed64())), 10))
          .addField(
              withFieldNumber(
                  "repeated_sfixed32", FieldType.array(FieldType.logicalType(new SFixed32())), 11))
          .addField(
              withFieldNumber(
                  "repeated_sfixed64", FieldType.array(FieldType.logicalType(new SFixed64())), 12))
          .addField(withFieldNumber("repeated_bool", FieldType.array(FieldType.BOOLEAN), 13))
          .addField(withFieldNumber("repeated_string", FieldType.array(FieldType.STRING), 14))
          .addField(withFieldNumber("repeated_bytes", FieldType.array(FieldType.BYTES), 15))
          .setOptions(withTypeName("proto3_schema_messages.RepeatPrimitive"))
          .build();

  // A sample instance of the row.
  static final Row REPEATED_ROW =
      Row.withSchema(REPEATED_SCHEMA)
          .addArray(1.1, 1.1)
          .addArray(2.2F, 2.2F)
          .addArray(32, 32)
          .addArray(64L, 64L)
          .addArray(33, 33)
          .addArray(65L, 65L)
          .addArray(123, 123)
          .addArray(124L, 124L)
          .addArray(30, 30)
          .addArray(62L, 62L)
          .addArray(31, 31)
          .addArray(63L, 63L)
          .addArray(true, true)
          .addArray("horsey", "horsey")
          .addArray(BYTE_ARRAY, BYTE_ARRAY)
          .build();

  // A sample instance of the proto.
  static final RepeatPrimitive REPEATED_PROTO =
      RepeatPrimitive.newBuilder()
          .addAllRepeatedDouble(ImmutableList.of(1.1, 1.1))
          .addAllRepeatedFloat(ImmutableList.of(2.2F, 2.2F))
          .addAllRepeatedInt32(ImmutableList.of(32, 32))
          .addAllRepeatedInt64(ImmutableList.of(64L, 64L))
          .addAllRepeatedUint32(ImmutableList.of(33, 33))
          .addAllRepeatedUint64(ImmutableList.of(65L, 65L))
          .addAllRepeatedSint32(ImmutableList.of(123, 123))
          .addAllRepeatedSint64(ImmutableList.of(124L, 124L))
          .addAllRepeatedFixed32(ImmutableList.of(30, 30))
          .addAllRepeatedFixed64(ImmutableList.of(62L, 62L))
          .addAllRepeatedSfixed32(ImmutableList.of(31, 31))
          .addAllRepeatedSfixed64(ImmutableList.of(63L, 63L))
          .addAllRepeatedBool(ImmutableList.of(true, true))
          .addAllRepeatedString(ImmutableList.of("horsey", "horsey"))
          .addAllRepeatedBytes(
              ImmutableList.of(ByteString.copyFrom(BYTE_ARRAY), ByteString.copyFrom(BYTE_ARRAY)))
          .build();

  static final Row NULL_REPEATED_ROW =
      Row.withSchema(REPEATED_SCHEMA)
          .addArray()
          .addArray()
          .addArray()
          .addArray()
          .addArray()
          .addArray()
          .addArray()
          .addArray()
          .addArray()
          .addArray()
          .addArray()
          .addArray()
          .addArray()
          .addArray()
          .addArray()
          .build();

  // A sample instance of the proto.
  static final RepeatPrimitive NULL_REPEATED_PROTO = RepeatPrimitive.newBuilder().build();

  // The schema for the MapPrimitive proto.
  static final Schema MAP_PRIMITIVE_SCHEMA =
      Schema.builder()
          .addField(
              withFieldNumber(
                  "string_string_map", FieldType.map(FieldType.STRING, FieldType.STRING), 1))
          .addField(
              withFieldNumber(
                  "string_int_map", FieldType.map(FieldType.STRING, FieldType.INT32), 2))
          .addField(
              withFieldNumber(
                  "int_string_map", FieldType.map(FieldType.INT32, FieldType.STRING), 3))
          .addField(
              withFieldNumber(
                  "string_bytes_map", FieldType.map(FieldType.STRING, FieldType.BYTES), 4))
          .setOptions(withTypeName("proto3_schema_messages.MapPrimitive"))
          .build();

  // A sample instance of the row.
  static final Row MAP_PRIMITIVE_ROW =
      Row.withSchema(MAP_PRIMITIVE_SCHEMA)
          .addValue(ImmutableMap.of("k1", "v1", "k2", "v2"))
          .addValue(ImmutableMap.of("k1", 1, "k2", 2))
          .addValue(ImmutableMap.of(1, "v1", 2, "v2"))
          .addValue(ImmutableMap.of("k1", BYTE_ARRAY, "k2", BYTE_ARRAY))
          .build();

  // A sample instance of the proto.
  static final MapPrimitive MAP_PRIMITIVE_PROTO =
      MapPrimitive.newBuilder()
          .putAllStringStringMap(ImmutableMap.of("k1", "v1", "k2", "v2"))
          .putAllStringIntMap(ImmutableMap.of("k1", 1, "k2", 2))
          .putAllIntStringMap(ImmutableMap.of(1, "v1", 2, "v2"))
          .putAllStringBytesMap(
              ImmutableMap.of(
                  "k1", ByteString.copyFrom(BYTE_ARRAY), "k2", ByteString.copyFrom(BYTE_ARRAY)))
          .build();

  // A sample instance of the row.
  static final Row NULL_MAP_PRIMITIVE_ROW =
      Row.withSchema(MAP_PRIMITIVE_SCHEMA)
          .addValue(ImmutableMap.of())
          .addValue(ImmutableMap.of())
          .addValue(ImmutableMap.of())
          .addValue(ImmutableMap.of())
          .build();

  // A sample instance of the proto.
  static final MapPrimitive NULL_MAP_PRIMITIVE_PROTO = MapPrimitive.newBuilder().build();

  // The schema for the Nested proto.
  static final Schema NESTED_SCHEMA =
      Schema.builder()
          .addField(
              withFieldNumber("nested", FieldType.row(PRIMITIVE_SCHEMA).withNullable(true), 1))
          .addField(
              withFieldNumber("nested_list", FieldType.array(FieldType.row(PRIMITIVE_SCHEMA)), 2))
          .addField(
              withFieldNumber(
                  "nested_map",
                  FieldType.map(FieldType.STRING, FieldType.row(PRIMITIVE_SCHEMA)),
                  3))
          .setOptions(withTypeName("proto3_schema_messages.Nested"))
          .build();

  // A sample instance of the row.
  static final Row NESTED_ROW =
      Row.withSchema(NESTED_SCHEMA)
          .addValue(PRIMITIVE_ROW)
          .addArray(ImmutableList.of(PRIMITIVE_ROW, PRIMITIVE_ROW))
          .addValue(ImmutableMap.of("k1", PRIMITIVE_ROW, "k2", PRIMITIVE_ROW))
          .build();

  // A sample instance of the proto.
  static final Nested NESTED_PROTO =
      Nested.newBuilder()
          .setNested(PRIMITIVE_PROTO)
          .addAllNestedList(ImmutableList.of(PRIMITIVE_PROTO, PRIMITIVE_PROTO))
          .putAllNestedMap(ImmutableMap.of("k1", PRIMITIVE_PROTO, "k2", PRIMITIVE_PROTO))
          .build();

  // The schema for the OneOf proto.
  private static final List<Field> ONEOF_FIELDS =
      ImmutableList.of(
          withFieldNumber("oneof_int32", FieldType.INT32, 2),
          withFieldNumber("oneof_bool", FieldType.BOOLEAN, 3),
          withFieldNumber("oneof_string", FieldType.STRING, 4),
          withFieldNumber("oneof_primitive", FieldType.row(PRIMITIVE_SCHEMA), 5));
  private static final Map<String, Integer> ONE_OF_ENUM_MAP =
      ONEOF_FIELDS.stream().collect(Collectors.toMap(Field::getName, f -> getFieldNumber(f)));
  static final OneOfType ONE_OF_TYPE = OneOfType.create(ONEOF_FIELDS, ONE_OF_ENUM_MAP);
  static final Schema ONEOF_SCHEMA =
      Schema.builder()
          .addField(withFieldNumber("place1", FieldType.STRING, 1))
          .addField("special_oneof", FieldType.logicalType(ONE_OF_TYPE))
          .addField(withFieldNumber("place2", FieldType.INT32, 6))
          .setOptions(withTypeName("proto3_schema_messages.OneOf"))
          .build();

  // Sample row instances for each OneOf case.
  static final Row ONEOF_ROW_INT32 =
      Row.withSchema(ONEOF_SCHEMA)
          .addValues("foo", ONE_OF_TYPE.createValue("oneof_int32", 1), 0)
          .build();
  static final Row ONEOF_ROW_BOOL =
      Row.withSchema(ONEOF_SCHEMA)
          .addValues("foo", ONE_OF_TYPE.createValue("oneof_bool", true), 0)
          .build();
  static final Row ONEOF_ROW_STRING =
      Row.withSchema(ONEOF_SCHEMA)
          .addValues("foo", ONE_OF_TYPE.createValue("oneof_string", "foo"), 0)
          .build();
  static final Row ONEOF_ROW_PRIMITIVE =
      Row.withSchema(ONEOF_SCHEMA)
          .addValues("foo", ONE_OF_TYPE.createValue("oneof_primitive", PRIMITIVE_ROW), 0)
          .build();

  // Sample proto instances for each oneof case.
  static final OneOf ONEOF_PROTO_INT32 =
      OneOf.newBuilder().setOneofInt32(1).setPlace1("foo").setPlace2(0).build();
  static final OneOf ONEOF_PROTO_BOOL =
      OneOf.newBuilder().setOneofBool(true).setPlace1("foo").setPlace2(0).build();
  static final OneOf ONEOF_PROTO_STRING =
      OneOf.newBuilder().setOneofString("foo").setPlace1("foo").setPlace2(0).build();
  static final OneOf ONEOF_PROTO_PRIMITIVE =
      OneOf.newBuilder().setOneofPrimitive(PRIMITIVE_PROTO).setPlace1("foo").setPlace2(0).build();

  // The schema for the OuterOneOf proto.
  private static final List<Field> OUTER_ONEOF_FIELDS =
      ImmutableList.of(
          withFieldNumber("oneof_oneof", FieldType.row(ONEOF_SCHEMA), 1),
          withFieldNumber("oneof_int32", FieldType.INT32, 2));
  private static final Map<String, Integer> OUTER_ONE_OF_ENUM_MAP =
      OUTER_ONEOF_FIELDS.stream().collect(Collectors.toMap(Field::getName, f -> getFieldNumber(f)));
  static final OneOfType OUTER_ONEOF_TYPE =
      OneOfType.create(OUTER_ONEOF_FIELDS, OUTER_ONE_OF_ENUM_MAP);
  static final Schema OUTER_ONEOF_SCHEMA =
      Schema.builder()
          .addField("outer_oneof", FieldType.logicalType(OUTER_ONEOF_TYPE))
          .setOptions(withTypeName("proto3_schema_messages.OuterOneOf"))
          .build();

  // A sample instance of the Row.
  static final Row OUTER_ONEOF_ROW =
      Row.withSchema(OUTER_ONEOF_SCHEMA)
          .addValues(OUTER_ONEOF_TYPE.createValue("oneof_oneof", ONEOF_ROW_PRIMITIVE))
          .build();

  // A sample instance of the proto.
  static final OuterOneOf OUTER_ONEOF_PROTO =
      OuterOneOf.newBuilder().setOneofOneof(ONEOF_PROTO_PRIMITIVE).build();

  // The schema for the ReversedOneOf proto.
  private static final List<Field> REVERSED_ONEOF_FIELDS =
      ImmutableList.of(
          withFieldNumber("oneof_int32", FieldType.INT32, 5),
          withFieldNumber("oneof_bool", FieldType.BOOLEAN, 4),
          withFieldNumber("oneof_string", FieldType.STRING, 3),
          withFieldNumber("oneof_primitive", FieldType.row(PRIMITIVE_SCHEMA), 2));

  private static final Map<String, Integer> REVERSED_ONE_OF_ENUM_MAP =
      REVERSED_ONEOF_FIELDS.stream()
          .collect(Collectors.toMap(Field::getName, f -> getFieldNumber(f)));
  static final OneOfType REVERSED_ONE_OF_TYPE =
      OneOfType.create(REVERSED_ONEOF_FIELDS, REVERSED_ONE_OF_ENUM_MAP);

  static final Schema REVERSED_ONEOF_SCHEMA =
      Schema.builder()
          .addField(withFieldNumber("place1", FieldType.STRING, 6))
          .addField("oneof_reversed", FieldType.logicalType(REVERSED_ONE_OF_TYPE))
          .addField(withFieldNumber("place2", FieldType.INT32, 1))
          .setOptions(withTypeName("proto3_schema_messages.ReversedOneOf"))
          .build();

  // Sample row instances for each ReversedOneOf case.
  static final Row REVERSED_ONEOF_ROW_INT32 =
      Row.withSchema(REVERSED_ONEOF_SCHEMA)
          .addValues("foo", REVERSED_ONE_OF_TYPE.createValue("oneof_int32", 1), 0)
          .build();
  static final Row REVERSED_ONEOF_ROW_BOOL =
      Row.withSchema(REVERSED_ONEOF_SCHEMA)
          .addValues("foo", REVERSED_ONE_OF_TYPE.createValue("oneof_bool", true), 0)
          .build();
  static final Row REVERSED_ONEOF_ROW_STRING =
      Row.withSchema(REVERSED_ONEOF_SCHEMA)
          .addValues("foo", REVERSED_ONE_OF_TYPE.createValue("oneof_string", "foo"), 0)
          .build();
  static final Row REVERSED_ONEOF_ROW_PRIMITIVE =
      Row.withSchema(REVERSED_ONEOF_SCHEMA)
          .addValues("foo", REVERSED_ONE_OF_TYPE.createValue("oneof_primitive", PRIMITIVE_ROW), 0)
          .build();

  // Sample proto instances for each ReversedOneOf case.
  static final ReversedOneOf REVERSED_ONEOF_PROTO_INT32 =
      ReversedOneOf.newBuilder().setOneofInt32(1).setPlace1("foo").setPlace2(0).build();
  static final ReversedOneOf REVERSED_ONEOF_PROTO_BOOL =
      ReversedOneOf.newBuilder().setOneofBool(true).setPlace1("foo").setPlace2(0).build();
  static final ReversedOneOf REVERSED_ONEOF_PROTO_STRING =
      ReversedOneOf.newBuilder().setOneofString("foo").setPlace1("foo").setPlace2(0).build();
  static final ReversedOneOf REVERSED_ONEOF_PROTO_PRIMITIVE =
      ReversedOneOf.newBuilder()
          .setOneofPrimitive(PRIMITIVE_PROTO)
          .setPlace1("foo")
          .setPlace2(0)
          .build();

  // The schema for the NonContiguousOneOf proto.
  private static final List<Field> NONCONTIGUOUS_ONE_ONEOF_FIELDS =
      ImmutableList.of(
          withFieldNumber("oneof_one_int32", FieldType.INT32, 55),
          withFieldNumber("oneof_one_bool", FieldType.BOOLEAN, 1),
          withFieldNumber("oneof_one_string", FieldType.STRING, 189),
          withFieldNumber("oneof_one_primitive", FieldType.row(PRIMITIVE_SCHEMA), 22));

  private static final List<Field> NONCONTIGUOUS_TWO_ONEOF_FIELDS =
      ImmutableList.of(
          withFieldNumber("oneof_two_first_string", FieldType.STRING, 981),
          withFieldNumber("oneof_two_int32", FieldType.INT32, 2),
          withFieldNumber("oneof_two_second_string", FieldType.STRING, 44));

  private static final Map<String, Integer> NONCONTIGUOUS_ONE_ONE_OF_ENUM_MAP =
      NONCONTIGUOUS_ONE_ONEOF_FIELDS.stream()
          .collect(Collectors.toMap(Field::getName, f -> getFieldNumber(f)));

  private static final Map<String, Integer> NONCONTIGUOUS_TWO_ONE_OF_ENUM_MAP =
      NONCONTIGUOUS_TWO_ONEOF_FIELDS.stream()
          .collect(Collectors.toMap(Field::getName, f -> getFieldNumber(f)));

  static final OneOfType NONCONTIGUOUS_ONE_ONE_OF_TYPE =
      OneOfType.create(NONCONTIGUOUS_ONE_ONEOF_FIELDS, NONCONTIGUOUS_ONE_ONE_OF_ENUM_MAP);

  static final OneOfType NONCONTIGUOUS_TWO_ONE_OF_TYPE =
      OneOfType.create(NONCONTIGUOUS_TWO_ONEOF_FIELDS, NONCONTIGUOUS_TWO_ONE_OF_ENUM_MAP);

  static final Schema NONCONTIGUOUS_ONEOF_SCHEMA =
      Schema.builder()
          .addField(withFieldNumber("place1", FieldType.STRING, 76))
          .addField(
              "oneof_non_contiguous_one", FieldType.logicalType(NONCONTIGUOUS_ONE_ONE_OF_TYPE))
          .addField(withFieldNumber("place2", FieldType.INT32, 33))
          .addField(
              "oneof_non_contiguous_two", FieldType.logicalType(NONCONTIGUOUS_TWO_ONE_OF_TYPE))
          .addField(withFieldNumber("place3", FieldType.INT32, 63))
          .setOptions(withTypeName("proto3_schema_messages.NonContiguousOneOf"))
          .build();

  static final Row NONCONTIGUOUS_ONEOF_ROW =
      Row.withSchema(NONCONTIGUOUS_ONEOF_SCHEMA)
          .addValues(
              "foo",
              NONCONTIGUOUS_ONE_ONE_OF_TYPE.createValue("oneof_one_int32", 1),
              0,
              NONCONTIGUOUS_TWO_ONE_OF_TYPE.createValue("oneof_two_second_string", "bar"),
              343)
          .build();

  static final NonContiguousOneOf NONCONTIGUOUS_ONEOF_PROTO =
      NonContiguousOneOf.newBuilder()
          .setOneofOneInt32(1)
          .setPlace1("foo")
          .setPlace2(0)
          .setOneofTwoSecondString("bar")
          .setPlace3(343)
          .build();

  static final Schema WKT_MESSAGE_SCHEMA =
      Schema.builder()
          .addField(withFieldNumber("double", FieldType.DOUBLE, 1).withNullable(true))
          .addField(withFieldNumber("float", FieldType.FLOAT, 2).withNullable(true))
          .addField(withFieldNumber("int32", FieldType.INT32, 3).withNullable(true))
          .addField(withFieldNumber("int64", FieldType.INT64, 4).withNullable(true))
          .addField(
              withFieldNumber("uint32", FieldType.logicalType(new UInt32()), 5).withNullable(true))
          .addField(
              withFieldNumber("uint64", FieldType.logicalType(new UInt64()), 6).withNullable(true))
          .addField(withFieldNumber("bool", FieldType.BOOLEAN, 13).withNullable(true))
          .addField(withFieldNumber("string", FieldType.STRING, 14).withNullable(true))
          .addField(withFieldNumber("bytes", FieldType.BYTES, 15).withNullable(true))
          .addField(
              withFieldNumber("timestamp", FieldType.logicalType(new NanosInstant()), 16)
                  .withNullable(true))
          .addField(
              withFieldNumber("duration", FieldType.logicalType(new NanosDuration()), 17)
                  .withNullable(true))
          .setOptions(withTypeName("proto3_schema_messages.WktMessage"))
          .build();

  static final Schema WKT_MESSAGE_SHUFFLED_SCHEMA =
      Schema.builder()
          .addField(withFieldNumber("int64", FieldType.INT64, 4).withNullable(true))
          .addField(withFieldNumber("int32", FieldType.INT32, 3).withNullable(true))
          .addField(withFieldNumber("double", FieldType.DOUBLE, 1).withNullable(true))
          .addField(
              withFieldNumber("uint32", FieldType.logicalType(new UInt32()), 5).withNullable(true))
          .addField(withFieldNumber("float", FieldType.FLOAT, 2).withNullable(true))
          .addField(
              withFieldNumber("uint64", FieldType.logicalType(new UInt64()), 6).withNullable(true))
          .addField(withFieldNumber("bytes", FieldType.BYTES, 15).withNullable(true))
          .addField(withFieldNumber("string", FieldType.STRING, 14).withNullable(true))
          .addField(withFieldNumber("bool", FieldType.BOOLEAN, 13).withNullable(true))
          .addField(
              withFieldNumber("timestamp", FieldType.logicalType(new NanosInstant()), 16)
                  .withNullable(true))
          .addField(
              withFieldNumber("duration", FieldType.logicalType(new NanosDuration()), 17)
                  .withNullable(true))
          .setOptions(withTypeName("proto3_schema_messages.WktMessage"))
          .build();

  // A sample instance of the  row.
  static final Instant JAVA_NOW = Instant.now();
  static final Timestamp PROTO_NOW =
      Timestamp.newBuilder()
          .setSeconds(JAVA_NOW.getEpochSecond())
          .setNanos(JAVA_NOW.getNano())
          .build();
  static final java.time.Duration JAVA_DURATION =
      java.time.Duration.ofSeconds(JAVA_NOW.getEpochSecond(), JAVA_NOW.getNano());
  static final Duration PROTO_DURATION =
      Duration.newBuilder()
          .setSeconds(JAVA_NOW.getEpochSecond())
          .setNanos(JAVA_NOW.getNano())
          .build();
  static final Row WKT_MESSAGE_ROW =
      Row.withSchema(WKT_MESSAGE_SCHEMA)
          .addValues(
              1.1, 2.2F, 32, 64L, 33, 65L, true, "horsey", BYTE_ARRAY, JAVA_NOW, JAVA_DURATION)
          .build();
  static final Row WKT_MESSAGE_SHUFFLED_ROW =
      Row.withSchema(WKT_MESSAGE_SHUFFLED_SCHEMA)
          .addValues(
              64L, 32, 1.1, 33, 2.2F, 65L, BYTE_ARRAY, "horsey", true, JAVA_NOW, JAVA_DURATION)
          .build();

  // A sample instance of the proto.
  static final WktMessage WKT_MESSAGE_PROTO =
      WktMessage.newBuilder()
          .setDouble(DoubleValue.of(1.1))
          .setFloat(FloatValue.of(2.2F))
          .setInt32(Int32Value.of(32))
          .setInt64(Int64Value.of(64))
          .setUint32(UInt32Value.of(33))
          .setUint64(UInt64Value.of(65))
          .setBool(BoolValue.of(true))
          .setString(StringValue.of("horsey"))
          .setBytes(BytesValue.of(ByteString.copyFrom(BYTE_ARRAY)))
          .setTimestamp(PROTO_NOW)
          .setDuration(PROTO_DURATION)
          .build();

  // The schema for the OptionalNested proto.
  static final Schema OPTIONAL_NESTED_SCHEMA =
      Schema.builder()
          .addField(
              withFieldNumber("nested", FieldType.row(OPTIONAL_PRIMITIVE_SCHEMA), 1)
                  .withNullable(true))
          .setOptions(withTypeName("proto2_schema_messages.OptionalNested"))
          .build();

  // A sample instance of the proto.
  static final OptionalNested OPTIONAL_NESTED =
      OptionalNested.newBuilder().setNested(OPTIONAL_PRIMITIVE_PROTO).build();

  // The schema for the Required Nested proto.
  static final Schema REQUIRED_NESTED_SCHEMA =
      Schema.builder()
          .addField(
              withFieldNumber("nested", FieldType.row(REQUIRED_PRIMITIVE_SCHEMA), 1)
                  .withNullable(false))
          .setOptions(withTypeName("proto2_schema_messages.RequiredNested"))
          .build();

  // A sample instance of the proto.
  static final RequiredNested REQUIRED_NESTED =
      RequiredNested.newBuilder().setNested(REQUIRED_PRIMITIVE_PROTO).build();
}
