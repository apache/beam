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
package org.apache.beam.sdk.extensions.sql.zetasql;

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/*
 * Run Test for Analytic Functions.
 * */
@RunWith(JUnit4.class)
public class AnalyticFunctionsTest extends ZetaSqlTestBase {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    initialize();
  }

  // Tests for Numbering Functions

  @Test
  public void RowNumberTest() {
    String sql = "SELECT x, ROW_NUMBER() over (ORDER BY x ) as agg  FROM analytic_table2";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    Schema overResultSchema = Schema.builder().addInt64Field("x").addInt64Field("agg").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(overResultSchema).addValues(1L, 1L).build(),
            Row.withSchema(overResultSchema).addValues(2L, 2L).build(),
            Row.withSchema(overResultSchema).addValues(2L, 3L).build(),
            Row.withSchema(overResultSchema).addValues(5L, 4L).build(),
            Row.withSchema(overResultSchema).addValues(8L, 5L).build(),
            Row.withSchema(overResultSchema).addValues(10L, 6L).build(),
            Row.withSchema(overResultSchema).addValues(10L, 7L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void DenseRankTest() {
    String sql = "SELECT x, DENSE_RANK() over (ORDER BY x ) as agg  FROM analytic_table2";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    Schema overResultSchema = Schema.builder().addInt64Field("x").addInt64Field("agg").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(overResultSchema).addValues(1L, 1L).build(),
            Row.withSchema(overResultSchema).addValues(2L, 2L).build(),
            Row.withSchema(overResultSchema).addValues(2L, 2L).build(),
            Row.withSchema(overResultSchema).addValues(5L, 3L).build(),
            Row.withSchema(overResultSchema).addValues(8L, 4L).build(),
            Row.withSchema(overResultSchema).addValues(10L, 5L).build(),
            Row.withSchema(overResultSchema).addValues(10L, 5L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void RankTest() {
    String sql = "SELECT x, RANK() over (ORDER BY x ) as agg  FROM analytic_table2";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    Schema overResultSchema = Schema.builder().addInt64Field("x").addInt64Field("agg").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(overResultSchema).addValues(1L, 1L).build(),
            Row.withSchema(overResultSchema).addValues(2L, 2L).build(),
            Row.withSchema(overResultSchema).addValues(2L, 2L).build(),
            Row.withSchema(overResultSchema).addValues(5L, 4L).build(),
            Row.withSchema(overResultSchema).addValues(8L, 5L).build(),
            Row.withSchema(overResultSchema).addValues(10L, 6L).build(),
            Row.withSchema(overResultSchema).addValues(10L, 6L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void PercentRankTest() {
    String sql = "SELECT x, PERCENT_RANK() over (ORDER BY x ) as agg  FROM analytic_table2";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    Schema overResultSchema = Schema.builder().addInt64Field("x").addDoubleField("agg").build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(overResultSchema).addValues(1L, 0.0 / 6.0).build(),
            Row.withSchema(overResultSchema).addValues(2L, 1.0 / 6.0).build(),
            Row.withSchema(overResultSchema).addValues(2L, 1.0 / 6.0).build(),
            Row.withSchema(overResultSchema).addValues(5L, 3.0 / 6.0).build(),
            Row.withSchema(overResultSchema).addValues(8L, 4.0 / 6.0).build(),
            Row.withSchema(overResultSchema).addValues(10L, 5.0 / 6.0).build(),
            Row.withSchema(overResultSchema).addValues(10L, 5.0 / 6.0).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  // Tests for Navigation Functions

  @Test
  public void firstValueTest() {
    String sql =
        "SELECT item, purchases, category, FIRST_VALUE(purchases) over "
            + "("
            + "PARTITION BY category "
            + "ORDER BY purchases "
            + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            + ")"
            + " as total_purchases  FROM analytic_table";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    Schema overResultSchema =
        Schema.builder()
            .addStringField("item")
            .addInt64Field("purchases")
            .addStringField("category")
            .addInt64Field("total_purchases")
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(overResultSchema).addValues("orange", 2L, "fruit", 2L).build(),
            Row.withSchema(overResultSchema).addValues("apple", 8L, "fruit", 2L).build(),
            Row.withSchema(overResultSchema).addValues("leek", 2L, "vegetable", 2L).build(),
            Row.withSchema(overResultSchema).addValues("cabbage", 9L, "vegetable", 2L).build(),
            Row.withSchema(overResultSchema).addValues("lettuce", 10L, "vegetable", 2L).build(),
            Row.withSchema(overResultSchema).addValues("kale", 23L, "vegetable", 2L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void lastValueTest() {
    String sql =
        "SELECT item, purchases, category, LAST_VALUE(purchases) over "
            + "("
            + "PARTITION BY category "
            + "ORDER BY purchases "
            + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            + ")"
            + " as total_purchases  FROM analytic_table";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    Schema overResultSchema =
        Schema.builder()
            .addStringField("item")
            .addInt64Field("purchases")
            .addStringField("category")
            .addInt64Field("total_purchases")
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(overResultSchema).addValues("orange", 2L, "fruit", 2L).build(),
            Row.withSchema(overResultSchema).addValues("apple", 8L, "fruit", 8L).build(),
            Row.withSchema(overResultSchema).addValues("leek", 2L, "vegetable", 2L).build(),
            Row.withSchema(overResultSchema).addValues("cabbage", 9L, "vegetable", 9L).build(),
            Row.withSchema(overResultSchema).addValues("lettuce", 10L, "vegetable", 10L).build(),
            Row.withSchema(overResultSchema).addValues("kale", 23L, "vegetable", 23L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  // Tests for Aggregation Functions

  @Test
  public void sumTest() {
    String sql =
        "SELECT item, purchases, category, sum(purchases) over "
            + "("
            + "PARTITION BY category "
            + "ORDER BY purchases "
            + "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING"
            + ")"
            + " as total_purchases  FROM analytic_table";
    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    Schema overResultSchema =
        Schema.builder()
            .addStringField("item")
            .addInt64Field("purchases")
            .addStringField("category")
            .addInt64Field("total_purchases")
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(overResultSchema).addValues("orange", 2L, "fruit", 10L).build(),
            Row.withSchema(overResultSchema).addValues("apple", 8L, "fruit", 10L).build(),
            Row.withSchema(overResultSchema).addValues("leek", 2L, "vegetable", 11L).build(),
            Row.withSchema(overResultSchema).addValues("cabbage", 9L, "vegetable", 21L).build(),
            Row.withSchema(overResultSchema).addValues("lettuce", 10L, "vegetable", 42L).build(),
            Row.withSchema(overResultSchema).addValues("kale", 23L, "vegetable", 33L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }

  @Test
  public void cumulativeSum() throws Exception {
    String sql =
        "SELECT item, purchases, category, sum(purchases) over "
            + "("
            + "PARTITION BY category "
            + "ORDER BY purchases "
            + "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
            + ")"
            + " as total_purchases  FROM analytic_table";

    ZetaSQLQueryPlanner zetaSQLQueryPlanner = new ZetaSQLQueryPlanner(config);
    BeamRelNode beamRelNode = zetaSQLQueryPlanner.convertToBeamRel(sql);
    PCollection<Row> stream = BeamSqlRelUtils.toPCollection(pipeline, beamRelNode);
    Schema overResultSchema =
        Schema.builder()
            .addStringField("item")
            .addInt64Field("purchases")
            .addStringField("category")
            .addInt64Field("total_purchases")
            .build();
    PAssert.that(stream)
        .containsInAnyOrder(
            Row.withSchema(overResultSchema).addValues("orange", 2L, "fruit", 2L).build(),
            Row.withSchema(overResultSchema).addValues("apple", 8L, "fruit", 10L).build(),
            Row.withSchema(overResultSchema).addValues("leek", 2L, "vegetable", 2L).build(),
            Row.withSchema(overResultSchema).addValues("cabbage", 9L, "vegetable", 11L).build(),
            Row.withSchema(overResultSchema).addValues("lettuce", 10L, "vegetable", 21L).build(),
            Row.withSchema(overResultSchema).addValues("kale", 23L, "vegetable", 44L).build());
    pipeline.run().waitUntilFinish(Duration.standardMinutes(PIPELINE_EXECUTION_WAITTIME_MINUTES));
  }
}
