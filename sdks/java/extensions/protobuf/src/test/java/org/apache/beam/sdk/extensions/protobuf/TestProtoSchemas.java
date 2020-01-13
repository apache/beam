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

import static org.apache.beam.sdk.extensions.protobuf.ProtoSchemaTranslator.getFieldNumber;
import static org.apache.beam.sdk.extensions.protobuf.ProtoSchemaTranslator.withFieldNumber;

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
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.MapPrimitive;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.Nested;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.OneOf;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.OuterOneOf;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.Primitive;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.RepeatPrimitive;
import org.apache.beam.sdk.extensions.protobuf.Proto3SchemaMessages.WktMessage;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.DurationNanos;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.Fixed32;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.Fixed64;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.SFixed32;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.SFixed64;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.SInt32;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.SInt64;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.TimestampNanos;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.UInt32;
import org.apache.beam.sdk.extensions.protobuf.ProtoSchemaLogicalTypes.UInt64;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

class TestProtoSchemas {
  // The schema we expect from the Primitive proto.
  static final Schema PRIMITIVE_SCHEMA =
      Schema.builder()
          .addNullableField("primitive_double", withFieldNumber(FieldType.DOUBLE, 1))
          .addNullableField("primitive_float", withFieldNumber(FieldType.FLOAT, 2))
          .addNullableField("primitive_int32", withFieldNumber(FieldType.INT32, 3))
          .addNullableField("primitive_int64", withFieldNumber(FieldType.INT64, 4))
          .addNullableField(
              "primitive_uint32", withFieldNumber(FieldType.logicalType(new UInt32()), 5))
          .addNullableField(
              "primitive_uint64", withFieldNumber(FieldType.logicalType(new UInt64()), 6))
          .addNullableField(
              "primitive_sint32", withFieldNumber(FieldType.logicalType(new SInt32()), 7))
          .addNullableField(
              "primitive_sint64", withFieldNumber(FieldType.logicalType(new SInt64()), 8))
          .addNullableField(
              "primitive_fixed32", withFieldNumber(FieldType.logicalType(new Fixed32()), 9))
          .addNullableField(
              "primitive_fixed64", withFieldNumber(FieldType.logicalType(new Fixed64()), 10))
          .addNullableField(
              "primitive_sfixed32", withFieldNumber(FieldType.logicalType(new SFixed32()), 11))
          .addNullableField(
              "primitive_sfixed64", withFieldNumber(FieldType.logicalType(new SFixed64()), 12))
          .addNullableField("primitive_bool", withFieldNumber(FieldType.BOOLEAN, 13))
          .addNullableField("primitive_string", withFieldNumber(FieldType.STRING, 14))
          .addNullableField("primitive_bytes", withFieldNumber(FieldType.BYTES, 15))
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

  // The schema for the RepeatedPrimitive proto.
  static final Schema REPEATED_SCHEMA =
      Schema.builder()
          .addField("repeated_double", withFieldNumber(FieldType.array(FieldType.DOUBLE), 1))
          .addField("repeated_float", withFieldNumber(FieldType.array(FieldType.FLOAT), 2))
          .addField("repeated_int32", withFieldNumber(FieldType.array(FieldType.INT32), 3))
          .addField("repeated_int64", withFieldNumber(FieldType.array(FieldType.INT64), 4))
          .addField(
              "repeated_uint32",
              withFieldNumber(FieldType.array(FieldType.logicalType(new UInt32())), 5))
          .addField(
              "repeated_uint64",
              withFieldNumber(FieldType.array(FieldType.logicalType(new UInt64())), 6))
          .addField(
              "repeated_sint32",
              withFieldNumber(FieldType.array(FieldType.logicalType(new SInt32())), 7))
          .addField(
              "repeated_sint64",
              withFieldNumber(FieldType.array(FieldType.logicalType(new SInt64())), 8))
          .addField(
              "repeated_fixed32",
              withFieldNumber(FieldType.array(FieldType.logicalType(new Fixed32())), 9))
          .addField(
              "repeated_fixed64",
              withFieldNumber(FieldType.array(FieldType.logicalType(new Fixed64())), 10))
          .addField(
              "repeated_sfixed32",
              withFieldNumber(FieldType.array(FieldType.logicalType(new SFixed32())), 11))
          .addField(
              "repeated_sfixed64",
              withFieldNumber(FieldType.array(FieldType.logicalType(new SFixed64())), 12))
          .addField("repeated_bool", withFieldNumber(FieldType.array(FieldType.BOOLEAN), 13))
          .addField("repeated_string", withFieldNumber(FieldType.array(FieldType.STRING), 14))
          .addField("repeated_bytes", withFieldNumber(FieldType.array(FieldType.BYTES), 15))
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

  // The schema for the MapPrimitive proto.
  static final Schema MAP_PRIMITIVE_SCHEMA =
      Schema.builder()
          .addField(
              "string_string_map",
              withFieldNumber(
                  FieldType.map(
                      FieldType.STRING.withNullable(true), FieldType.STRING.withNullable(true)),
                  1))
          .addField(
              "string_int_map",
              withFieldNumber(
                  FieldType.map(
                      FieldType.STRING.withNullable(true), FieldType.INT32.withNullable(true)),
                  2))
          .addField(
              "int_string_map",
              withFieldNumber(
                  FieldType.map(
                      FieldType.INT32.withNullable(true), FieldType.STRING.withNullable(true)),
                  3))
          .addField(
              "string_bytes_map",
              withFieldNumber(
                  FieldType.map(
                      FieldType.STRING.withNullable(true), FieldType.BYTES.withNullable(true)),
                  4))
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

  // The schema for the Nested proto.
  static final Schema NESTED_SCHEMA =
      Schema.builder()
          .addField(
              "nested", withFieldNumber(FieldType.row(PRIMITIVE_SCHEMA).withNullable(true), 1))
          .addField(
              "nested_list", withFieldNumber(FieldType.array(FieldType.row(PRIMITIVE_SCHEMA)), 2))
          .addField(
              "nested_map",
              withFieldNumber(
                  FieldType.map(
                      FieldType.STRING.withNullable(true),
                      FieldType.row(PRIMITIVE_SCHEMA).withNullable(true)),
                  3))
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
          Field.of("oneof_int32", withFieldNumber(FieldType.INT32, 2)),
          Field.of("oneof_bool", withFieldNumber(FieldType.BOOLEAN, 3)),
          Field.of("oneof_string", withFieldNumber(FieldType.STRING, 4)),
          Field.of("oneof_primitive", withFieldNumber(FieldType.row(PRIMITIVE_SCHEMA), 5)));
  private static final Map<String, Integer> ONE_OF_ENUM_MAP =
      ONEOF_FIELDS.stream()
          .collect(Collectors.toMap(Field::getName, f -> getFieldNumber(f.getType())));
  static final OneOfType ONE_OF_TYPE = OneOfType.create(ONEOF_FIELDS, ONE_OF_ENUM_MAP);
  static final Schema ONEOF_SCHEMA =
      Schema.builder()
          .addField("special_oneof", FieldType.logicalType(ONE_OF_TYPE))
          .addField("place1", withFieldNumber(FieldType.STRING.withNullable(true), 1))
          .addField("place2", withFieldNumber(FieldType.INT32.withNullable(true), 6))
          .build();

  // Sample row instances for each OneOf case.
  static final Row ONEOF_ROW_INT32 =
      Row.withSchema(ONEOF_SCHEMA)
          .addValues(ONE_OF_TYPE.createValue("oneof_int32", 1), "foo", 0)
          .build();
  static final Row ONEOF_ROW_BOOL =
      Row.withSchema(ONEOF_SCHEMA)
          .addValues(ONE_OF_TYPE.createValue("oneof_bool", true), "foo", 0)
          .build();
  static final Row ONEOF_ROW_STRING =
      Row.withSchema(ONEOF_SCHEMA)
          .addValues(ONE_OF_TYPE.createValue("oneof_string", "foo"), "foo", 0)
          .build();
  static final Row ONEOF_ROW_PRIMITIVE =
      Row.withSchema(ONEOF_SCHEMA)
          .addValues(ONE_OF_TYPE.createValue("oneof_primitive", PRIMITIVE_ROW), "foo", 0)
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
          Field.of("oneof_oneof", withFieldNumber(FieldType.row(ONEOF_SCHEMA), 1)),
          Field.of("oneof_int32", withFieldNumber(FieldType.INT32, 2)));
  private static final Map<String, Integer> OUTER_ONE_OF_ENUM_MAP =
      OUTER_ONEOF_FIELDS.stream()
          .collect(Collectors.toMap(Field::getName, f -> getFieldNumber(f.getType())));
  static final OneOfType OUTER_ONEOF_TYPE =
      OneOfType.create(OUTER_ONEOF_FIELDS, OUTER_ONE_OF_ENUM_MAP);
  static final Schema OUTER_ONEOF_SCHEMA =
      Schema.builder().addField("outer_oneof", FieldType.logicalType(OUTER_ONEOF_TYPE)).build();

  // A sample instance of the Row.
  static final Row OUTER_ONEOF_ROW =
      Row.withSchema(OUTER_ONEOF_SCHEMA)
          .addValues(OUTER_ONEOF_TYPE.createValue("oneof_oneof", ONEOF_ROW_PRIMITIVE))
          .build();

  // A sample instance of the proto.
  static final OuterOneOf OUTER_ONEOF_PROTO =
      OuterOneOf.newBuilder().setOneofOneof(ONEOF_PROTO_PRIMITIVE).build();

  static final Schema WKT_MESSAGE_SCHEMA =
      Schema.builder()
          .addNullableField("double", withFieldNumber(FieldType.DOUBLE, 1))
          .addNullableField("float", withFieldNumber(FieldType.FLOAT, 2))
          .addNullableField("int32", withFieldNumber(FieldType.INT32, 3))
          .addNullableField("int64", withFieldNumber(FieldType.INT64, 4))
          .addNullableField("uint32", withFieldNumber(FieldType.logicalType(new UInt32()), 5))
          .addNullableField("uint64", withFieldNumber(FieldType.logicalType(new UInt64()), 6))
          .addNullableField("bool", withFieldNumber(FieldType.BOOLEAN, 13))
          .addNullableField("string", withFieldNumber(FieldType.STRING, 14))
          .addNullableField("bytes", withFieldNumber(FieldType.BYTES, 15))
          .addNullableField(
              "timestamp", withFieldNumber(FieldType.logicalType(new TimestampNanos()), 16))
          .addNullableField(
              "duration", withFieldNumber(FieldType.logicalType(new DurationNanos()), 17))
          .build();
  // A sample instance of the  row.
  static final Instant JAVA_NOW = Instant.now();
  static final Timestamp PROTO_NOW =
      Timestamp.newBuilder()
          .setSeconds(JAVA_NOW.getEpochSecond())
          .setNanos(JAVA_NOW.getNano())
          .build();
  static final Duration PROTO_DURATION =
      Duration.newBuilder()
          .setSeconds(JAVA_NOW.getEpochSecond())
          .setNanos(JAVA_NOW.getNano())
          .build();
  static final Row WKT_MESSAGE_ROW =
      Row.withSchema(WKT_MESSAGE_SCHEMA)
          .addValues(
              1.1, 2.2F, 32, 64L, 33, 65L, true, "horsey", BYTE_ARRAY, PROTO_NOW, PROTO_DURATION)
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
}
