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
package org.apache.beam.sdk.extensions.sql;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

/** Unit Tests for ComplexTypes, including nested ROW etc. */
public class BeamComplexTypeTest {
  private static final Schema innerRowSchema =
      Schema.builder().addStringField("string_field").addInt64Field("long_field").build();

  private static final Schema innerRowWithArraySchema =
      Schema.builder()
          .addStringField("string_field")
          .addArrayField("array_field", FieldType.INT64)
          .build();

  private static final Schema nullableInnerRowSchema =
      Schema.builder()
          .addNullableField("inner_row_field", FieldType.row(innerRowSchema))
          .addNullableField("array_field", FieldType.array(FieldType.row(innerRowSchema)))
          .build();

  private static final Schema nullableNestedRowWithArraySchema =
      Schema.builder()
          .addNullableField("field1", FieldType.row(innerRowWithArraySchema))
          .addNullableField("field2", FieldType.array(FieldType.row(innerRowWithArraySchema)))
          .addNullableField("field3", FieldType.row(nullableInnerRowSchema))
          .build();

  private static final Schema nestedRowSchema =
      Schema.builder()
          .addStringField("nonRowfield1")
          .addRowField("RowField", innerRowSchema)
          .addInt64Field("nonRowfield2")
          .addRowField("RowFieldTwo", innerRowSchema)
          .build();

  private static final Schema rowWithArraySchema =
      Schema.builder()
          .addStringField("field1")
          .addInt64Field("field2")
          .addArrayField("field3", FieldType.INT64)
          .build();

  private static final Schema flattenedRowSchema =
      Schema.builder()
          .addStringField("field1")
          .addStringField("field2")
          .addInt64Field("field3")
          .addInt64Field("field4")
          .addStringField("field5")
          .addInt64Field("field6")
          .build();

  private static final ReadOnlyTableProvider readOnlyTableProvider =
      new ReadOnlyTableProvider(
          "test_provider",
          ImmutableMap.of(
              "arrayWithRowTestTable",
              TestBoundedTable.of(FieldType.array(FieldType.row(innerRowSchema)), "col")
                  .addRows(
                      Arrays.asList(Row.withSchema(innerRowSchema).addValues("str", 1L).build())),
              "nestedArrayTestTable",
              TestBoundedTable.of(FieldType.array(FieldType.array(FieldType.INT64)), "col")
                  .addRows(Arrays.asList(Arrays.asList(1L, 2L, 3L), Arrays.asList(4L, 5L))),
              "nestedRowTestTable",
              TestBoundedTable.of(Schema.FieldType.row(nestedRowSchema), "col")
                  .addRows(
                      Row.withSchema(nestedRowSchema)
                          .addValues(
                              "str",
                              Row.withSchema(innerRowSchema).addValues("inner_str_one", 1L).build(),
                              2L,
                              Row.withSchema(innerRowSchema).addValues("inner_str_two", 3L).build())
                          .build()),
              "basicRowTestTable",
              TestBoundedTable.of(Schema.FieldType.row(innerRowSchema), "col")
                  .addRows(Row.withSchema(innerRowSchema).addValues("innerStr", 1L).build()),
              "rowWithArrayTestTable",
              TestBoundedTable.of(Schema.FieldType.row(rowWithArraySchema), "col")
                  .addRows(
                      Row.withSchema(rowWithArraySchema)
                          .addValues("str", 4L, Arrays.asList(5L, 6L))
                          .build())));

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testNestedRow() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(readOnlyTableProvider);
    PCollection<Row> stream =
        BeamSqlRelUtils.toPCollection(
            pipeline, sqlEnv.parseQuery("SELECT nestedRowTestTable.col FROM nestedRowTestTable"));
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(flattenedRowSchema)
                .addValues("str", "inner_str_one", 1L, 2L, "inner_str_two", 3L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testArrayWithRow() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(readOnlyTableProvider);
    PCollection<Row> stream =
        BeamSqlRelUtils.toPCollection(
            pipeline,
            sqlEnv.parseQuery("SELECT arrayWithRowTestTable.col[1] FROM arrayWithRowTestTable"));
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(innerRowSchema).addValues("str", 1L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testNestedArray() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(readOnlyTableProvider);
    PCollection<Row> stream =
        BeamSqlRelUtils.toPCollection(
            pipeline,
            sqlEnv.parseQuery(
                "SELECT nestedArrayTestTable.col[1][3], nestedArrayTestTable.col[2][1] FROM nestedArrayTestTable"));
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("field1").addInt64Field("field2").build())
                .addValues(3L, 4L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testBasicRow() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(readOnlyTableProvider);
    PCollection<Row> stream =
        BeamSqlRelUtils.toPCollection(
            pipeline, sqlEnv.parseQuery("SELECT col FROM basicRowTestTable"));
    PAssert.that(stream)
        .containsInAnyOrder(Row.withSchema(innerRowSchema).addValues("innerStr", 1L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testArrayConstructor() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(readOnlyTableProvider);
    PCollection<Row> stream =
        BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery("SELECT ARRAY[1, 2, 3] f_arr"));
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addArrayField("f_arr", FieldType.INT32).build())
                .addValue(Arrays.asList(1, 2, 3))
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testRowWithArray() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(readOnlyTableProvider);
    PCollection<Row> stream =
        BeamSqlRelUtils.toPCollection(
            pipeline,
            sqlEnv.parseQuery(
                "SELECT rowWithArrayTestTable.col.field3[2] FROM rowWithArrayTestTable"));
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(Schema.builder().addInt64Field("int64").build()).addValue(6L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testFieldAccessToNestedRow() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(readOnlyTableProvider);
    PCollection<Row> stream =
        BeamSqlRelUtils.toPCollection(
            pipeline,
            sqlEnv.parseQuery(
                "SELECT nestedRowTestTable.col.RowField.string_field, nestedRowTestTable.col.RowFieldTwo.long_field FROM nestedRowTestTable"));
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder().addStringField("field1").addInt64Field("field2").build())
                .addValues("inner_str_one", 3L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Ignore("https://issues.apache.org/jira/browse/BEAM-5189")
  @Test
  public void testSelectInnerRowOfNestedRow() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(readOnlyTableProvider);
    PCollection<Row> stream =
        BeamSqlRelUtils.toPCollection(
            pipeline,
            sqlEnv.parseQuery("SELECT nestedRowTestTable.col.RowField FROM nestedRowTestTable"));
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder().addStringField("field1").addInt64Field("field2").build())
                .addValues("inner_str_one", 1L)
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testNestedBytes() {
    byte[] bytes = new byte[] {-70, -83, -54, -2};

    Schema nestedInputSchema = Schema.of(Schema.Field.of("c_bytes", Schema.FieldType.BYTES));
    Schema inputSchema =
        Schema.of(Schema.Field.of("nested", Schema.FieldType.row(nestedInputSchema)));

    Schema outputSchema = Schema.of(Schema.Field.of("f0", Schema.FieldType.BYTES));

    Row nestedRow = Row.withSchema(nestedInputSchema).addValue(bytes).build();
    Row row = Row.withSchema(inputSchema).addValue(nestedRow).build();
    Row expected = Row.withSchema(outputSchema).addValue(bytes).build();

    PCollection<Row> result =
        pipeline
            .apply(Create.of(row).withRowSchema(inputSchema))
            .apply(SqlTransform.query("SELECT t.nested.c_bytes AS f0 FROM PCOLLECTION t"));

    PAssert.that(result).containsInAnyOrder(expected);

    pipeline.run();
  }

  @Test
  public void testNestedArrayOfBytes() {
    byte[] bytes = new byte[] {-70, -83, -54, -2};

    Schema nestedInputSchema =
        Schema.of(Schema.Field.of("c_bytes", Schema.FieldType.array(Schema.FieldType.BYTES)));
    Schema inputSchema =
        Schema.of(Schema.Field.of("nested", Schema.FieldType.row(nestedInputSchema)));

    Schema outputSchema = Schema.of(Schema.Field.of("f0", Schema.FieldType.BYTES));

    Row nestedRow = Row.withSchema(nestedInputSchema).addValue(ImmutableList.of(bytes)).build();
    Row row = Row.withSchema(inputSchema).addValue(nestedRow).build();
    Row expected = Row.withSchema(outputSchema).addValue(bytes).build();

    PCollection<Row> result =
        pipeline
            .apply(Create.of(row).withRowSchema(inputSchema))
            .apply(SqlTransform.query("SELECT t.nested.c_bytes[1] AS f0 FROM PCOLLECTION t"));

    PAssert.that(result).containsInAnyOrder(expected);

    pipeline.run();
  }

  @Test
  public void testRowConstructor() {
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(readOnlyTableProvider);
    PCollection<Row> stream =
        BeamSqlRelUtils.toPCollection(
            pipeline, sqlEnv.parseQuery("SELECT ROW(1, ROW(2, 3), 'str', ROW('str2', 'str3'))"));
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addInt32Field("field1")
                        .addInt32Field("field2")
                        .addInt32Field("field3")
                        .addStringField("field4")
                        .addStringField("field5")
                        .addStringField("field6")
                        .build())
                .addValues(1, 2, 3, "str", "str2", "str3")
                .build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testNullRows() {

    Row nullRow = Row.nullRow(nullableNestedRowWithArraySchema);

    PCollection<Row> outputRow =
        pipeline
            .apply(Create.of(nullRow))
            .setRowSchema(nullableNestedRowWithArraySchema)
            .apply(
                SqlTransform.query(
                    "select PCOLLECTION.field1.string_field as row_string_field, PCOLLECTION.field2[2].string_field as array_string_field from PCOLLECTION"));

    PAssert.that(outputRow)
        .containsInAnyOrder(
            Row.nullRow(
                Schema.builder()
                    .addNullableField("row_string_field", FieldType.STRING)
                    .addNullableField("array_string_field", FieldType.STRING)
                    .build()));

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testNullInnerRow() {

    Row nestedInnerRow = Row.withSchema(innerRowSchema).addValues("str", 1000L).build();

    Row innerRow =
        Row.withSchema(nullableInnerRowSchema)
            .addValues(null, Arrays.asList(nestedInnerRow))
            .build();

    Row row =
        Row.withSchema(nullableNestedRowWithArraySchema).addValues(null, null, innerRow).build();

    PCollection<Row> outputRow =
        pipeline
            .apply(Create.of(row))
            .setRowSchema(nullableNestedRowWithArraySchema)
            .apply(
                SqlTransform.query(
                    "select PCOLLECTION.field3.inner_row_field.string_field as string_field, PCOLLECTION.field3.array_field[1].long_field as long_field from PCOLLECTION"));

    PAssert.that(outputRow)
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addNullableField("string_field", FieldType.STRING)
                        .addInt64Field("long_field")
                        .build())
                .addValues(null, 1000L)
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testNullDatetimeFields() {
    Instant current = new Instant(1561671380000L); // Long value corresponds to 27/06/2019

    Schema dateTimeFieldSchema =
        Schema.builder()
            .addField("dateTimeField", FieldType.DATETIME)
            .addNullableField("nullableDateTimeField", FieldType.DATETIME)
            .addField("timeTypeField", FieldType.logicalType(SqlTypes.TIME))
            .addNullableField("nullableTimeTypeField", FieldType.logicalType(SqlTypes.TIME))
            .addField("dateTypeField", FieldType.logicalType(SqlTypes.DATE))
            .addNullableField("nullableDateTypeField", FieldType.logicalType(SqlTypes.DATE))
            .build();

    Row dateTimeRow =
        Row.withSchema(dateTimeFieldSchema)
            .addValues(current, null, LocalTime.of(1, 0, 0), null, LocalDate.of(2019, 6, 27), null)
            .build();

    PCollection<Row> outputRow =
        pipeline
            .apply(Create.of(dateTimeRow))
            .setRowSchema(dateTimeFieldSchema)
            .apply(
                SqlTransform.query(
                    "select EXTRACT(YEAR from dateTimeField) as yyyy, "
                        + " EXTRACT(YEAR from nullableDateTimeField) as year_with_null, "
                        + " EXTRACT(MONTH from dateTimeField) as mm, "
                        + " EXTRACT(MONTH from nullableDateTimeField) as month_with_null, "
                        + " timeTypeField + interval '1' hour as time_with_hour_added, "
                        + " nullableTimeTypeField + interval '1' hour as hour_added_with_null, "
                        + " timeTypeField - INTERVAL '60' SECOND as time_with_seconds_added, "
                        + " nullableTimeTypeField - INTERVAL '60' SECOND as seconds_added_with_null, "
                        + " EXTRACT(DAY from dateTypeField) as dd, "
                        + " EXTRACT(DAY from nullableDateTypeField) as day_with_null, "
                        + " dateTypeField + interval '1' day as date_with_day_added, "
                        + " nullableDateTypeField + interval '1' day as day_added_with_null "
                        + " from PCOLLECTION"));

    Schema outputRowSchema =
        Schema.builder()
            .addField("yyyy", FieldType.INT64)
            .addNullableField("year_with_null", FieldType.INT64)
            .addField("mm", FieldType.INT64)
            .addNullableField("month_with_null", FieldType.INT64)
            .addField("time_with_hour_added", FieldType.logicalType(SqlTypes.TIME))
            .addNullableField("hour_added_with_null", FieldType.logicalType(SqlTypes.TIME))
            .addField("time_with_seconds_added", FieldType.logicalType(SqlTypes.TIME))
            .addNullableField("seconds_added_with_null", FieldType.logicalType(SqlTypes.TIME))
            .addField("dd", FieldType.INT64)
            .addNullableField("day_with_null", FieldType.INT64)
            .addField("date_with_day_added", FieldType.logicalType(SqlTypes.DATE))
            .addNullableField("day_added_with_null", FieldType.logicalType(SqlTypes.DATE))
            .build();

    PAssert.that(outputRow)
        .containsInAnyOrder(
            Row.withSchema(outputRowSchema)
                .addValues(
                    2019L,
                    null,
                    06L,
                    null,
                    LocalTime.of(2, 0, 0),
                    null,
                    LocalTime.of(0, 59, 0),
                    null,
                    27L,
                    null,
                    LocalDate.of(2019, 6, 28),
                    null)
                .build());

    pipeline.run().waitUntilFinish(Duration.standardMinutes(2));
  }

  @Test
  public void testMapWithRowAsValue() {

    Schema inputSchema =
        Schema.builder()
            .addMapField("mapWithValueAsRow", FieldType.STRING, FieldType.row(rowWithArraySchema))
            .build();

    Map<String, Row> mapWithValueAsRow = new HashMap<>();
    Row complexRow =
        Row.withSchema(rowWithArraySchema)
            .addValues("RED", 5L, Arrays.asList(10L, 20L, 30L))
            .build();
    mapWithValueAsRow.put("key", complexRow);

    Row rowOfMap = Row.withSchema(inputSchema).addValue(mapWithValueAsRow).build();

    PCollection<Row> outputRow =
        pipeline
            .apply(Create.of(rowOfMap))
            .setRowSchema(inputSchema)
            .apply(
                SqlTransform.query(
                    "select  PCOLLECTION.mapWithValueAsRow['key'].field1 as color, PCOLLECTION.mapWithValueAsRow['key'].field3[2]  as num   from PCOLLECTION"));

    Row expectedRow =
        Row.withSchema(Schema.builder().addStringField("color").addInt64Field("num").build())
            .addValues("RED", 20L)
            .build();

    PAssert.that(outputRow).containsInAnyOrder(expectedRow);
    pipeline.run().waitUntilFinish(Duration.standardMinutes(1));
  }

  @Test
  public void testMapWithNullRowFields() {

    Schema nullableInnerSchema =
        Schema.builder()
            .addNullableField("strField", FieldType.STRING)
            .addNullableField("arrField", FieldType.array(FieldType.INT64))
            .build();
    Schema inputSchema =
        Schema.builder()
            .addMapField("mapField", FieldType.STRING, FieldType.row(nullableInnerSchema))
            .addNullableField(
                "nullableMapField",
                FieldType.map(FieldType.STRING, FieldType.row(nullableInnerSchema)))
            .build();

    Row mapValue = Row.withSchema(nullableInnerSchema).addValues("str", null).build();
    Map<String, Row> mapWithValueAsRow = new HashMap<>();
    mapWithValueAsRow.put("key", mapValue);

    Row inputRow = Row.withSchema(inputSchema).addValues(mapWithValueAsRow, null).build();

    PCollection<Row> outputRow =
        pipeline
            .apply(Create.of(inputRow))
            .setRowSchema(inputSchema)
            .apply(
                SqlTransform.query(
                    "select PCOLLECTION.mapField['key'].strField as str, PCOLLECTION.mapField['key'].arrField[1] as arr, PCOLLECTION.nullableMapField['key'].arrField[1] as nullableField  from PCOLLECTION"));

    Row expectedRow =
        Row.withSchema(
                Schema.builder()
                    .addStringField("str")
                    .addNullableField("arr", FieldType.INT64)
                    .addNullableField("nullableField", FieldType.INT64)
                    .build())
            .addValues("str", null, null)
            .build();
    PAssert.that(outputRow).containsInAnyOrder(expectedRow);
    pipeline.run().waitUntilFinish(Duration.standardMinutes(1));
  }
}
