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
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Ad-hoc tests for CAST. */
public class BeamSqlMultipleSchemasTest {

  private static final Schema ROW_SCHEMA =
      Schema.builder().addInt32Field("f_int").addStringField("f_string").build();

  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException exceptions = ExpectedException.none();

  @Test
  public void testSelectFromQualifiedPCollection() {
    PCollection<Row> input = pipeline.apply(create(row(1, "strstr")));

    PCollection<Row> result =
        input.apply(SqlTransform.query("SELECT f_int, f_string FROM beam.PCOLLECTION"));

    PAssert.that(result).containsInAnyOrder(row(1, "strstr"));
    pipeline.run();
  }

  @Test
  public void testSelectFromUnqualifiedPCollection() {
    PCollection<Row> input = pipeline.apply(create(row(1, "strstr")));

    PCollection<Row> result =
        input.apply(SqlTransform.query("SELECT f_int, f_string FROM PCOLLECTION"));

    PAssert.that(result).containsInAnyOrder(row(1, "strstr"));
    pipeline.run();
  }

  @Test
  public void testSelectFromExtraSchema() {
    PCollection<Row> inputMain =
        pipeline.apply("mainInput", create(row(1, "pcollection_1"), row(2, "pcollection_2")));

    PCollection<Row> inputExtra =
        pipeline.apply("extraInput", create(row(1, "_extra_table_1"), row(2, "_extra_table_2")));

    TableProvider extraInputProvider = extraTableProvider("extraTable", inputExtra);

    PCollection<Row> result =
        inputMain.apply(
            SqlTransform.query("SELECT f_int, f_string FROM extraSchema.extraTable")
                .withTableProvider("extraSchema", extraInputProvider));

    PAssert.that(result).containsInAnyOrder(row(1, "_extra_table_1"), row(2, "_extra_table_2"));
    pipeline.run();
  }

  @Test
  public void testOverrideUnqualifiedMainSchema() {
    PCollection<Row> inputMain =
        pipeline.apply("mainInput", create(row(1, "pcollection_1"), row(2, "pcollection_2")));

    PCollection<Row> inputExtra =
        pipeline.apply("extraInput", create(row(1, "_extra_table_1"), row(2, "_extra_table_2")));

    TableProvider extraInputProvider = extraTableProvider("extraTable", inputExtra);

    PCollection<Row> result =
        inputMain.apply(
            SqlTransform.query("SELECT f_int, f_string FROM extraTable")
                .withTableProvider("beam", extraInputProvider));

    PAssert.that(result).containsInAnyOrder(row(1, "_extra_table_1"), row(2, "_extra_table_2"));
    pipeline.run();
  }

  @Test
  public void testOverrideQualifiedMainSchema() {
    PCollection<Row> inputMain =
        pipeline.apply("mainInput", create(row(1, "pcollection_1"), row(2, "pcollection_2")));

    PCollection<Row> inputExtra =
        pipeline.apply("extraInput", create(row(1, "_extra_table_1"), row(2, "_extra_table_2")));

    TableProvider extraInputProvider = extraTableProvider("extraTable", inputExtra);

    PCollection<Row> result =
        inputMain.apply(
            SqlTransform.query("SELECT f_int, f_string FROM beam.extraTable")
                .withTableProvider("beam", extraInputProvider));

    PAssert.that(result).containsInAnyOrder(row(1, "_extra_table_1"), row(2, "_extra_table_2"));
    pipeline.run();
  }

  @Test
  public void testSetDefaultUnqualifiedSchema() {
    PCollection<Row> inputMain =
        pipeline.apply("mainInput", create(row(1, "pcollection_1"), row(2, "pcollection_2")));

    PCollection<Row> inputExtra =
        pipeline.apply("extraInput", create(row(1, "_extra_table_1"), row(2, "_extra_table_2")));

    TableProvider extraInputProvider = extraTableProvider("extraTable", inputExtra);

    PCollection<Row> result =
        inputMain.apply(
            SqlTransform.query("SELECT f_int, f_string FROM extraTable")
                .withDefaultTableProvider("extraSchema", extraInputProvider));

    PAssert.that(result).containsInAnyOrder(row(1, "_extra_table_1"), row(2, "_extra_table_2"));
    pipeline.run();
  }

  @Test
  public void testSetDefaultUnqualifiedSchemaAndJoin() {
    PCollection<Row> inputMain =
        pipeline.apply("mainInput", create(row(1, "pcollection_1"), row(2, "pcollection_2")));

    PCollection<Row> inputExtra =
        pipeline.apply("extraInput", create(row(1, "_extra_table_1"), row(2, "_extra_table_2")));

    TableProvider extraInputProvider = extraTableProvider("extraTable", inputExtra);

    PCollection<Row> result =
        inputMain.apply(
            SqlTransform.query(
                    "SELECT extra.f_int, main.f_string || extra.f_string AS f_string \n"
                        + "FROM extraTable AS extra \n"
                        + "   INNER JOIN \n"
                        + " beam.PCOLLECTION AS main \n"
                        + "   ON main.f_int = extra.f_int")
                .withDefaultTableProvider("extraSchema", extraInputProvider));

    PAssert.that(result)
        .containsInAnyOrder(
            row(1, "pcollection_1_extra_table_1"), row(2, "pcollection_2_extra_table_2"));
    pipeline.run();
  }

  @Test
  public void testSetDefaultQualifiedSchema() {
    PCollection<Row> inputMain =
        pipeline.apply("mainInput", create(row(1, "pcollection_1"), row(2, "pcollection_2")));

    PCollection<Row> inputExtra =
        pipeline.apply("extraInput", create(row(1, "_extra_table_1"), row(2, "_extra_table_2")));

    TableProvider extraInputProvider = extraTableProvider("extraTable", inputExtra);

    PCollection<Row> result =
        inputMain.apply(
            SqlTransform.query("SELECT f_int, f_string FROM extraSchema.extraTable")
                .withDefaultTableProvider("extraSchema", extraInputProvider));

    PAssert.that(result).containsInAnyOrder(row(1, "_extra_table_1"), row(2, "_extra_table_2"));
    pipeline.run();
  }

  @Test
  public void testJoinWithExtraSchema() {
    PCollection<Row> inputMain =
        pipeline.apply("mainInput", create(row(1, "pcollection_1"), row(2, "pcollection_2")));

    PCollection<Row> inputExtra =
        pipeline.apply("extraInput", create(row(1, "_extra_table_1"), row(2, "_extra_table_2")));

    TableProvider extraInputProvider = extraTableProvider("extraTable", inputExtra);

    PCollection<Row> result =
        inputMain.apply(
            SqlTransform.query(
                    "SELECT extra.f_int, main.f_string || extra.f_string AS f_string \n"
                        + "FROM extraSchema.extraTable AS extra \n"
                        + "   INNER JOIN \n"
                        + " PCOLLECTION AS main \n"
                        + "   ON main.f_int = extra.f_int")
                .withTableProvider("extraSchema", extraInputProvider));

    PAssert.that(result)
        .containsInAnyOrder(
            row(1, "pcollection_1_extra_table_1"), row(2, "pcollection_2_extra_table_2"));
    pipeline.run();
  }

  @Test
  public void testJoinQualifiedMainWithExtraSchema() {
    PCollection<Row> inputMain =
        pipeline.apply("mainInput", create(row(1, "pcollection_1"), row(2, "pcollection_2")));

    PCollection<Row> inputExtra =
        pipeline.apply("extraInput", create(row(1, "_extra_table_1"), row(2, "_extra_table_2")));

    TableProvider extraInputProvider = extraTableProvider("extraTable", inputExtra);

    PCollection<Row> result =
        inputMain.apply(
            SqlTransform.query(
                    "SELECT extra.f_int, main.f_string || extra.f_string AS f_string \n"
                        + "FROM extraSchema.extraTable AS extra \n"
                        + "   INNER JOIN \n"
                        + " beam.PCOLLECTION AS main \n"
                        + "   ON main.f_int = extra.f_int")
                .withTableProvider("extraSchema", extraInputProvider));

    PAssert.that(result)
        .containsInAnyOrder(
            row(1, "pcollection_1_extra_table_1"), row(2, "pcollection_2_extra_table_2"));
    pipeline.run();
  }

  private TableProvider extraTableProvider(String tableName, PCollection<Row> rows) {
    return new ReadOnlyTableProvider(
        "testExtraTableProvider", ImmutableMap.of(tableName, new BeamPCollectionTable<>(rows)));
  }

  private Row row(int fIntValue, String fStringValue) {
    return Row.withSchema(ROW_SCHEMA).addValues(fIntValue, fStringValue).build();
  }

  private PTransform<PBegin, PCollection<Row>> create(Row... rows) {
    return Create.of(Arrays.asList(rows)).withRowSchema(ROW_SCHEMA);
  }
}
