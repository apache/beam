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
package org.apache.beam.sdk.io.gcp.spanner;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.io.gcp.spanner.MutationUtils.beamRowToMutationFn;
import static org.junit.Assert.assertEquals;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import java.math.BigDecimal;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.junit.Test;

public class MutationUtilsTest {
  private static final Schema EMPTY_SCHEMA = Schema.builder().build();
  private static final Schema INT64_SCHEMA = Schema.builder().addInt64Field("int64").build();
  private static final Row EMPTY_ROW = Row.withSchema(EMPTY_SCHEMA).build();
  private static final Row INT64_ROW =
      Row.withSchema(INT64_SCHEMA).withFieldValue("int64", 3L).build();
  private static final Struct EMPTY_STRUCT = Struct.newBuilder().build();
  private static final Struct INT64_STRUCT = Struct.newBuilder().set("int64").to(3L).build();
  private static final String TABLE = "some_table";

  private static final Schema WRITE_ROW_SCHEMA =
      Schema.builder()
          .addNullableField("f_int64", Schema.FieldType.INT64)
          .addNullableField("f_float32", Schema.FieldType.FLOAT)
          .addNullableField("f_float64", Schema.FieldType.DOUBLE)
          .addNullableField("f_string", Schema.FieldType.STRING)
          .addNullableField("f_bytes", Schema.FieldType.BYTES)
          .addNullableField("f_date_time", Schema.FieldType.DATETIME)
          .addNullableField("f_bool", Schema.FieldType.BOOLEAN)
          .addNullableField("f_struct", Schema.FieldType.row(EMPTY_SCHEMA))
          .addNullableField("f_struct_int64", Schema.FieldType.row(INT64_SCHEMA))
          .addNullableField("f_array", Schema.FieldType.array(Schema.FieldType.INT64))
          .addNullableField("f_float_array", Schema.FieldType.array(Schema.FieldType.FLOAT))
          .addNullableField("f_double_array", Schema.FieldType.array(Schema.FieldType.DOUBLE))
          .addNullableField("f_decimal_array", Schema.FieldType.array(Schema.FieldType.DECIMAL))
          .addNullableField("f_boolean_array", Schema.FieldType.array(Schema.FieldType.BOOLEAN))
          .addNullableField("f_string_array", Schema.FieldType.array(Schema.FieldType.STRING))
          .addNullableField("f_bytes_array", Schema.FieldType.array(Schema.FieldType.BYTES))
          .addNullableField("f_datetime_array", Schema.FieldType.array(Schema.FieldType.DATETIME))
          .addNullableField(
              "f_struct_array", Schema.FieldType.array(Schema.FieldType.row(INT64_SCHEMA)))
          .addNullableField("f_int16", Schema.FieldType.INT16)
          .addNullableField("f_int32", Schema.FieldType.INT32)
          .addNullableField("f_decimal", Schema.FieldType.DECIMAL)
          .addNullableField("f_byte", Schema.FieldType.BYTE)
          .addNullableField("f_iterable", Schema.FieldType.iterable(Schema.FieldType.INT64))
          .build();

  private static final Row WRITE_ROW =
      Row.withSchema(WRITE_ROW_SCHEMA)
          .withFieldValue("f_int64", 1L)
          .withFieldValue("f_float32", 2.1f)
          .withFieldValue("f_float64", 1.1)
          .withFieldValue("f_string", "donald_duck")
          .withFieldValue("f_bytes", "some_bytes".getBytes(UTF_8))
          .withFieldValue("f_date_time", DateTime.parse("2077-10-15T00:00:00+00:00"))
          .withFieldValue("f_bool", false)
          .withFieldValue("f_struct", EMPTY_ROW)
          .withFieldValue("f_struct_int64", INT64_ROW)
          .withFieldValue("f_array", ImmutableList.of(2L, 3L))
          .withFieldValue("f_float_array", ImmutableList.of(3.0f, 4.0f))
          .withFieldValue("f_double_array", ImmutableList.of(1., 2.))
          .withFieldValue(
              "f_decimal_array",
              ImmutableList.of(
                  BigDecimal.valueOf(Long.MIN_VALUE), BigDecimal.valueOf(Long.MAX_VALUE)))
          .withFieldValue("f_boolean_array", ImmutableList.of(false, true))
          .withFieldValue("f_string_array", ImmutableList.of("donald_duck", "micky_mouse"))
          .withFieldValue(
              "f_bytes_array",
              ImmutableList.of("some_bytes".getBytes(UTF_8), "some_bytes".getBytes(UTF_8)))
          .withFieldValue(
              "f_datetime_array",
              ImmutableList.of(
                  DateTime.parse("2077-10-15T00:00:00+00:00"),
                  DateTime.parse("2077-10-15T00:00:00+00:00")))
          .withFieldValue("f_struct_array", ImmutableList.of(INT64_ROW, INT64_ROW))
          .withFieldValue("f_int16", (short) 2)
          .withFieldValue("f_int32", 0x7fffffff)
          .withFieldValue("f_decimal", BigDecimal.valueOf(Long.MIN_VALUE))
          .withFieldValue("f_byte", Byte.parseByte("127"))
          .withFieldValue("f_iterable", ImmutableList.of(2L, 3L))
          .build();

  private static final Schema WRITE_ROW_SCHEMA_NULLS =
      Schema.builder()
          .addNullableField("f_int64", Schema.FieldType.INT64)
          .addNullableField("f_float32", Schema.FieldType.FLOAT)
          .addNullableField("f_float64", Schema.FieldType.DOUBLE)
          .addNullableField("f_string", Schema.FieldType.STRING)
          .addNullableField("f_bytes", Schema.FieldType.BYTES)
          .addNullableField("f_date_time", Schema.FieldType.DATETIME)
          .addNullableField("f_bool", Schema.FieldType.BOOLEAN)
          .addNullableField("f_struct", Schema.FieldType.row(EMPTY_SCHEMA))
          .addNullableField("f_struct_int64", Schema.FieldType.row(INT64_SCHEMA))
          .addNullableField("f_array", Schema.FieldType.array(Schema.FieldType.INT64))
          .addNullableField(
              "f_struct_array", Schema.FieldType.array(Schema.FieldType.row(INT64_SCHEMA)))
          .build();

  private static final Row WRITE_ROW_NULLS =
      Row.withSchema(WRITE_ROW_SCHEMA_NULLS)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .build();

  private static final Schema KEY_SCHEMA =
      Schema.builder()
          .addNullableField("f_int64", Schema.FieldType.INT64)
          .addNullableField("f_float32", Schema.FieldType.FLOAT)
          .addNullableField("f_float64", Schema.FieldType.DOUBLE)
          .addNullableField("f_string", Schema.FieldType.STRING)
          .addNullableField("f_bytes", Schema.FieldType.BYTES)
          .addNullableField("f_date_time", Schema.FieldType.DATETIME)
          .addNullableField("f_bool", Schema.FieldType.BOOLEAN)
          .addNullableField("f_int16", Schema.FieldType.INT16)
          .addNullableField("f_int32", Schema.FieldType.INT32)
          .addNullableField("f_decimal", Schema.FieldType.DECIMAL)
          .addNullableField("f_byte", Schema.FieldType.BYTE)
          .build();

  private static final Row KEY_ROW =
      Row.withSchema(KEY_SCHEMA)
          .withFieldValue("f_int64", 1L)
          .withFieldValue("f_float32", 2.1f)
          .withFieldValue("f_float64", 1.1)
          .withFieldValue("f_string", "donald_duck")
          .withFieldValue("f_bytes", "some_bytes".getBytes(UTF_8))
          .withFieldValue("f_date_time", DateTime.parse("2077-10-15T00:00:00+00:00"))
          .withFieldValue("f_bool", false)
          .withFieldValue("f_int16", (short) 2)
          .withFieldValue("f_int32", 0x7fffffff)
          .withFieldValue("f_decimal", BigDecimal.valueOf(Long.MIN_VALUE))
          .withFieldValue("f_byte", Byte.parseByte("127"))
          .build();

  private static final Schema KEY_SCHEMA_NULLS =
      Schema.builder()
          .addNullableField("f_int64", Schema.FieldType.INT64)
          .addNullableField("f_float64", Schema.FieldType.DOUBLE)
          .addNullableField("f_string", Schema.FieldType.STRING)
          .addNullableField("f_bytes", Schema.FieldType.BYTES)
          .addNullableField("f_date_time", Schema.FieldType.DATETIME)
          .addNullableField("f_bool", Schema.FieldType.BOOLEAN)
          .build();

  private static final Row KEY_ROW_NULLS =
      Row.withSchema(KEY_SCHEMA_NULLS)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .addValue(null)
          .build();

  @Test
  public void testCreateInsertMutationFromRow() {
    Mutation expectedMutation = createMutation(Mutation.Op.INSERT);
    Mutation mutation = beamRowToMutationFn(Mutation.Op.INSERT, TABLE).apply(WRITE_ROW);
    assertEquals(expectedMutation, mutation);
  }

  @Test
  public void testCreateUpdateMutationFromRow() {
    Mutation expectedMutation = createMutation(Mutation.Op.UPDATE);
    Mutation mutation = beamRowToMutationFn(Mutation.Op.UPDATE, TABLE).apply(WRITE_ROW);
    assertEquals(expectedMutation, mutation);
  }

  @Test
  public void testCreateReplaceMutationFromRow() {
    Mutation expectedMutation = createMutation(Mutation.Op.REPLACE);
    Mutation mutation = beamRowToMutationFn(Mutation.Op.REPLACE, TABLE).apply(WRITE_ROW);
    assertEquals(expectedMutation, mutation);
  }

  @Test
  public void testCreateInsertOrUpdateMutationFromRow() {
    Mutation expectedMutation = createMutation(Mutation.Op.INSERT_OR_UPDATE);
    Mutation mutation = beamRowToMutationFn(Mutation.Op.INSERT_OR_UPDATE, TABLE).apply(WRITE_ROW);
    assertEquals(expectedMutation, mutation);
  }

  @Test
  public void testCreateDeleteMutationFromRow() {
    Mutation expectedMutation = createDeleteMutation();
    Mutation mutation = beamRowToMutationFn(Mutation.Op.DELETE, TABLE).apply(KEY_ROW);
    assertEquals(expectedMutation, mutation);
  }

  @Test
  public void testCreateInsertMutationFromRowWithNulls() {
    Mutation expectedMutation = createMutationNulls(Mutation.Op.INSERT);
    Mutation mutation = beamRowToMutationFn(Mutation.Op.INSERT, TABLE).apply(WRITE_ROW_NULLS);
    assertEquals(expectedMutation, mutation);
  }

  @Test
  public void testCreateInsertOrUpdateMutationFromRowWithNulls() {
    Mutation expectedMutation = createMutationNulls(Mutation.Op.INSERT_OR_UPDATE);
    Mutation mutation =
        beamRowToMutationFn(Mutation.Op.INSERT_OR_UPDATE, TABLE).apply(WRITE_ROW_NULLS);
    assertEquals(expectedMutation, mutation);
  }

  @Test
  public void testCreateUpdateMutationFromRowWithNulls() {
    Mutation expectedMutation = createMutationNulls(Mutation.Op.UPDATE);
    Mutation mutation = beamRowToMutationFn(Mutation.Op.UPDATE, TABLE).apply(WRITE_ROW_NULLS);
    assertEquals(expectedMutation, mutation);
  }

  @Test
  public void testCreateReplaceMutationFromRowWithNulls() {
    Mutation expectedMutation = createMutationNulls(Mutation.Op.REPLACE);
    Mutation mutation = beamRowToMutationFn(Mutation.Op.REPLACE, TABLE).apply(WRITE_ROW_NULLS);
    assertEquals(expectedMutation, mutation);
  }

  @Test
  public void testCreateDeleteMutationFromRowWithNulls() {
    Mutation expectedMutation = createDeleteMutationNulls();
    Mutation mutation = beamRowToMutationFn(Mutation.Op.DELETE, TABLE).apply(KEY_ROW_NULLS);
    assertEquals(expectedMutation, mutation);
    DateTime d = DateTime.parse("2077-10-15T00:00:00+00:00");
    System.out.println(Timestamp.parseTimestamp(d.toString()));
  }

  private static Mutation createDeleteMutation() {
    Key key =
        Key.newBuilder()
            .append(1L)
            .append(2.1f)
            .append(1.1)
            .append("donald_duck")
            .append(ByteArray.copyFrom("some_bytes".getBytes(UTF_8)))
            .append(Timestamp.parseTimestamp("2077-10-15T00:00:00"))
            .append(false)
            .append((short) 2)
            .append(0x7fffffff)
            .append(BigDecimal.valueOf(Long.MIN_VALUE))
            .append(Byte.parseByte("127"))
            .build();
    return Mutation.delete(TABLE, key);
  }

  private static Mutation createDeleteMutationNulls() {
    Key key =
        Key.newBuilder()
            .append((Long) null)
            .append((Double) null)
            .append((String) null)
            .append((ByteArray) null)
            .append((Timestamp) null)
            .append((Boolean) null)
            .build();
    return Mutation.delete(TABLE, key);
  }

  private static Mutation createMutation(Mutation.Op operation) {
    Mutation.WriteBuilder builder = chooseBuilder(operation);
    return builder
        .set("f_int64")
        .to(1L)
        .set("f_float32")
        .to(2.1f)
        .set("f_float64")
        .to(1.1)
        .set("f_string")
        .to("donald_duck")
        .set("f_bytes")
        .to(ByteArray.copyFrom("some_bytes".getBytes(UTF_8)))
        .set("f_date_time")
        .to(Timestamp.parseTimestamp("2077-10-15T00:00:00"))
        .set("f_bool")
        .to(false)
        .set("f_struct")
        .to(EMPTY_STRUCT)
        .set("f_struct_int64")
        .to(Struct.newBuilder().set("int64").to(3L).build())
        .set("f_array")
        .toInt64Array(ImmutableList.of(2L, 3L))
        .set("f_float_array")
        .toFloat32Array(ImmutableList.of(3.0f, 4.0f))
        .set("f_double_array")
        .toFloat64Array(ImmutableList.of(1., 2.))
        .set("f_decimal_array")
        .toNumericArray(
            ImmutableList.of(
                BigDecimal.valueOf(Long.MIN_VALUE), BigDecimal.valueOf(Long.MAX_VALUE)))
        .set("f_boolean_array")
        .toBoolArray(ImmutableList.of(false, true))
        .set("f_string_array")
        .toStringArray(ImmutableList.of("donald_duck", "micky_mouse"))
        .set("f_bytes_array")
        .toBytesArray(
            ImmutableList.of(
                ByteArray.copyFrom("some_bytes".getBytes(UTF_8)),
                ByteArray.copyFrom("some_bytes".getBytes(UTF_8))))
        .set("f_datetime_array")
        .toTimestampArray(
            ImmutableList.of(
                Timestamp.parseTimestamp("2077-10-15T00:00:00Z"),
                Timestamp.parseTimestamp("2077-10-15T00:00:00Z")))
        .set("f_struct_array")
        .toStructArray(
            Type.struct(ImmutableList.of(Type.StructField.of("int64", Type.int64()))),
            ImmutableList.of(INT64_STRUCT, INT64_STRUCT))
        .set("f_int16")
        .to((short) 2)
        .set("f_int32")
        .to(0x7fffffff)
        .set("f_decimal")
        .to(BigDecimal.valueOf(Long.MIN_VALUE))
        .set("f_byte")
        .to(Byte.parseByte("127"))
        .set("f_iterable")
        .toInt64Array(ImmutableList.of(2L, 3L))
        .build();
  }

  private static Mutation createMutationNulls(Mutation.Op operation) {
    Mutation.WriteBuilder builder = chooseBuilder(operation);
    return builder
        .set("f_int64")
        .to((Long) null)
        .set("f_float32")
        .to((Float) null)
        .set("f_float64")
        .to((Double) null)
        .set("f_string")
        .to((String) null)
        .set("f_bytes")
        .to((ByteArray) null)
        .set("f_date_time")
        .to((Timestamp) null)
        .set("f_bool")
        .to((Boolean) null)
        .set("f_struct")
        .to(Type.struct(), null)
        .set("f_struct_int64")
        .to(Type.struct(Type.StructField.of("int64", Type.int64())), null)
        .set("f_array")
        .toInt64Array((List<Long>) null)
        .set("f_struct_array")
        .toStructArray(Type.struct(Type.StructField.of("int64", Type.int64())), null)
        .build();
  }

  private static Mutation.WriteBuilder chooseBuilder(Mutation.Op op) {
    switch (op) {
      case INSERT:
        return Mutation.newInsertBuilder(TABLE);
      case UPDATE:
        return Mutation.newUpdateBuilder(TABLE);
      case REPLACE:
        return Mutation.newReplaceBuilder(TABLE);
      case INSERT_OR_UPDATE:
        return Mutation.newInsertOrUpdateBuilder(TABLE);
      default:
        throw new IllegalArgumentException("Operation '" + op + "' not supported");
    }
  }
}
