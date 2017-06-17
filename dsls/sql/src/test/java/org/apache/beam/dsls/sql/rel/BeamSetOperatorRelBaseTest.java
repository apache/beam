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

package org.apache.beam.dsls.sql.rel;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.beam.dsls.sql.BeamSqlCli;
import org.apache.beam.dsls.sql.BeamSqlEnv;
import org.apache.beam.dsls.sql.planner.MockedBeamSqlTable;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test for {@code BeamSetOperatorRelBase}.
 */
public class BeamSetOperatorRelBaseTest {
  static BeamSqlEnv sqlEnv = new BeamSqlEnv();

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();
  public static final Date THE_DATE = new Date();
  private static MockedBeamSqlTable orderDetailsTable = MockedBeamSqlTable
      .of(SqlTypeName.BIGINT, "order_id",
          SqlTypeName.INTEGER, "site_id",
          SqlTypeName.DOUBLE, "price",
          SqlTypeName.TIMESTAMP, "order_time",

          1L, 1, 1.0, THE_DATE,
          2L, 2, 2.0, THE_DATE);

  @BeforeClass
  public static void prepare() {
    THE_DATE.setTime(100000);
    sqlEnv.registerTable("ORDER_DETAILS", orderDetailsTable);
  }

  @Test
  public void testSameWindow() throws Exception {
    String sql = "SELECT "
        + " order_id, site_id, count(*) as cnt "
        + "FROM ORDER_DETAILS GROUP BY order_id, site_id"
        + ", TUMBLE(order_time, INTERVAL '1' HOUR) "
        + " UNION SELECT "
        + " order_id, site_id, count(*) as cnt "
        + "FROM ORDER_DETAILS GROUP BY order_id, site_id"
        + ", TUMBLE(order_time, INTERVAL '1' HOUR) ";

    PCollection<BeamSqlRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, sqlEnv);
    List<BeamSqlRow> expRows =
        MockedBeamSqlTable.of(
        SqlTypeName.BIGINT, "order_id",
        SqlTypeName.INTEGER, "site_id",
        SqlTypeName.BIGINT, "cnt",

        1L, 1, 1L,
        2L, 2, 1L
    ).getInputRecords();
    // compare valueInString to ignore the windowStart & windowEnd
    PAssert.that(rows.apply(ParDo.of(new ToString()))).containsInAnyOrder(toString(expRows));
    pipeline.run();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDifferentWindows() throws Exception {
    String sql = "SELECT "
        + " order_id, site_id, count(*) as cnt "
        + "FROM ORDER_DETAILS GROUP BY order_id, site_id"
        + ", TUMBLE(order_time, INTERVAL '1' HOUR) "
        + " UNION SELECT "
        + " order_id, site_id, count(*) as cnt "
        + "FROM ORDER_DETAILS GROUP BY order_id, site_id"
        + ", TUMBLE(order_time, INTERVAL '2' HOUR) ";

    // use a real pipeline rather than the TestPipeline because we are
    // testing exceptions, the pipeline will not actually run.
    Pipeline pipeline1 = Pipeline.create(PipelineOptionsFactory.create());
    BeamSqlCli.compilePipeline(sql, pipeline1, sqlEnv);
    pipeline.run();
  }

  static class ToString extends DoFn<BeamSqlRow, String> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
      ctx.output(ctx.element().valueInString());
    }
  }

  static List<String> toString (List<BeamSqlRow> rows) {
    List<String> strs = new ArrayList<>();
    for (BeamSqlRow row : rows) {
      strs.add(row.valueInString());
    }

    return strs;
  }
}
