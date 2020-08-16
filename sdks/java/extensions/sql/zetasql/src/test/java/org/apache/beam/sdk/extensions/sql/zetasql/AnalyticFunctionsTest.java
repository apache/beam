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

import java.util.*;
import org.apache.beam.sdk.extensions.sql.*;
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
