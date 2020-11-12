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
import static org.apache.beam.sdk.io.gcp.spanner.StructUtils.beamTypeToSpannerType;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import java.math.BigDecimal;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.junit.Test;

@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class StructUtilsTest {
  private static final Schema EMPTY_SCHEMA = Schema.builder().build();
  private static final Schema INT64_SCHEMA = Schema.builder().addInt64Field("int64").build();

  @Test
  public void testStructToBeamRow() {
    Schema schema = getSchemaTemplate().addDateTimeField("f_date").build();
    Row row = getRowTemplate(schema).withFieldValue("f_date", DateTime.parse("2077-10-24")).build();
    Struct struct =
        getStructTemplate().set("f_date").to(Date.fromYearMonthDay(2077, 10, 24)).build();
    assertEquals(row, StructUtils.structToBeamRow(struct, schema));
  }

  @Test
  public void testStructToBeamRowFailsColumnsDontMatch() {
    Schema schema = Schema.builder().addInt64Field("f_int64").build();
    Struct struct = Struct.newBuilder().set("f_different_field").to(5L).build();
    Exception exception =
        assertThrows(
            IllegalArgumentException.class, () -> StructUtils.structToBeamRow(struct, schema));
    assertEquals("Field not found: f_int64", exception.getMessage());
  }

  @Test
  public void testStructToBeamRowFailsTypesDontMatch() {
    Schema schema = Schema.builder().addInt64Field("f_int64").build();
    Struct struct = Struct.newBuilder().set("f_int64").to("string_value").build();
    Exception exception =
        assertThrows(ClassCastException.class, () -> StructUtils.structToBeamRow(struct, schema));
    assertEquals("java.lang.String cannot be cast to java.lang.Long", exception.getMessage());
  }

  @Test
  public void testBeamRowToStruct() {
    Schema schema =
        getSchemaTemplate().addIterableField("f_iterable", Schema.FieldType.INT64).build();
    Row row = getRowTemplate(schema).withFieldValue("f_iterable", ImmutableList.of(20L)).build();
    Struct struct =
        getStructTemplate().set("f_iterable").toInt64Array(ImmutableList.of(20L)).build();
    assertEquals(struct, StructUtils.beamRowToStruct(row));
  }

  @Test
  public void testBeamRowToStructFailsTypeNotSupported() {
    Schema schema =
        getSchemaTemplate()
            .addMapField("f_map", Schema.FieldType.STRING, Schema.FieldType.STRING)
            .build();
    Row row = getRowTemplate(schema).withFieldValue("f_map", ImmutableMap.of("a", "b")).build();
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> StructUtils.beamRowToStruct(row));
    assertThat(
        exception.getMessage(),
        containsString("Unsupported beam type 'MAP' while translating row to struct."));
  }

  @Test
  public void testBeamTypeToSpannerTypeTranslation() {
    assertEquals(Type.int64(), beamTypeToSpannerType(Schema.FieldType.INT64));
    assertEquals(Type.int64(), beamTypeToSpannerType(Schema.FieldType.INT32));
    assertEquals(Type.int64(), beamTypeToSpannerType(Schema.FieldType.INT16));
    assertEquals(Type.int64(), beamTypeToSpannerType(Schema.FieldType.BYTE));
    assertEquals(Type.bytes(), beamTypeToSpannerType(Schema.FieldType.BYTES));
    assertEquals(Type.string(), beamTypeToSpannerType(Schema.FieldType.STRING));
    assertEquals(Type.float64(), beamTypeToSpannerType(Schema.FieldType.FLOAT));
    assertEquals(Type.float64(), beamTypeToSpannerType(Schema.FieldType.DOUBLE));
    assertEquals(Type.bool(), beamTypeToSpannerType(Schema.FieldType.BOOLEAN));
    assertEquals(Type.numeric(), beamTypeToSpannerType(Schema.FieldType.DECIMAL));
    assertEquals(
        Type.struct(ImmutableList.of(Type.StructField.of("int64", Type.int64()))),
        beamTypeToSpannerType(Schema.FieldType.row(INT64_SCHEMA)));
    assertEquals(
        Type.array(Type.int64()),
        beamTypeToSpannerType(Schema.FieldType.array(Schema.FieldType.INT64)));
  }

  private Schema.Builder getSchemaTemplate() {
    return Schema.builder()
        .addInt64Field("f_int64")
        .addDoubleField("f_float64")
        .addDecimalField("f_decimal")
        .addStringField("f_string")
        .addByteArrayField("f_bytes")
        .addDateTimeField("f_timestamp")
        .addBooleanField("f_bool")
        .addRowField("f_struct", EMPTY_SCHEMA)
        .addRowField("f_struct_int64", INT64_SCHEMA)
        .addArrayField("f_array", Schema.FieldType.INT64)
        .addArrayField("f_struct_array", Schema.FieldType.row(INT64_SCHEMA));
  }

  private Row.FieldValueBuilder getRowTemplate(Schema schema) {
    return Row.withSchema(schema)
        .withFieldValue("f_int64", 1L)
        .withFieldValue("f_float64", 5.5)
        .withFieldValue("f_decimal", BigDecimal.ONE)
        .withFieldValue("f_string", "ducky_doo")
        .withFieldValue("f_bytes", ByteArray.copyFrom("random_bytes".getBytes(UTF_8)).toByteArray())
        .withFieldValue("f_timestamp", DateTime.parse("2077-01-10"))
        .withFieldValue("f_bool", true)
        .withFieldValue("f_struct", Row.withSchema(EMPTY_SCHEMA).build())
        .withFieldValue(
            "f_struct_int64", Row.withSchema(INT64_SCHEMA).withFieldValue("int64", 10L).build())
        .withFieldValue("f_array", ImmutableList.of(55L, 43L))
        .withFieldValue(
            "f_struct_array",
            ImmutableList.of(
                Row.withSchema(INT64_SCHEMA).withFieldValue("int64", 1L).build(),
                Row.withSchema(INT64_SCHEMA).withFieldValue("int64", 2L).build()));
  }

  private Struct.Builder getStructTemplate() {
    return Struct.newBuilder()
        .set("f_int64")
        .to(1L)
        .set("f_float64")
        .to(5.5)
        .set("f_decimal")
        .to(BigDecimal.ONE)
        .set("f_string")
        .to("ducky_doo")
        .set("f_bytes")
        .to(ByteArray.copyFrom("random_bytes".getBytes(UTF_8)))
        .set("f_timestamp")
        .to(
            Timestamp.ofTimeMicroseconds(
                DateTime.parse("2077-01-10").toInstant().getMillis() * 1000L))
        .set("f_bool")
        .to(true)
        .set("f_struct")
        .to(Struct.newBuilder().build())
        .set("f_struct_int64")
        .to(Struct.newBuilder().set("int64").to(10L).build())
        .set("f_array")
        .toInt64Array(ImmutableList.of(55L, 43L))
        .set("f_struct_array")
        .toStructArray(
            Type.struct(Type.StructField.of("int64", Type.int64())),
            ImmutableList.of(
                Struct.newBuilder().set("int64").to(1L).build(),
                Struct.newBuilder().set("int64").to(2L).build()));
  }
}
