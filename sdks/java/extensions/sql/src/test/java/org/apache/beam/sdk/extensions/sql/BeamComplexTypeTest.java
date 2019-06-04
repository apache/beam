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

import java.util.Arrays;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
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

  private static final Schema nestedRowWithArraySchema =
      Schema.builder()
          .addStringField("field1")
          .addRowField("field2", innerRowWithArraySchema)
          .addInt64Field("field3")
          .addArrayField("field4", FieldType.array(FieldType.STRING))
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
}
