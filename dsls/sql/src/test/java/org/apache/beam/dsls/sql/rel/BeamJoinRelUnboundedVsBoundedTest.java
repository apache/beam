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

import java.sql.Types;
import java.util.Date;
import org.apache.beam.dsls.sql.BeamSqlCli;
import org.apache.beam.dsls.sql.BeamSqlEnv;
import org.apache.beam.dsls.sql.TestUtils;
import org.apache.beam.dsls.sql.planner.MockedBeamSqlTable;
import org.apache.beam.dsls.sql.planner.MockedUnboundedTable;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.dsls.sql.transform.BeamSqlOutputToConsoleFn;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.Duration;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Unbounded + Unbounded Test for {@code BeamJoinRel}.
 */
public class BeamJoinRelUnboundedVsBoundedTest {
  @Rule
  public final TestPipeline pipeline = TestPipeline.create();
  private static final BeamSqlEnv beamSqlEnv = new BeamSqlEnv();
  public static final Date FIRST_DATE = new Date(1);
  public static final Date SECOND_DATE = new Date(1 + 3600 * 1000);
  public static final Date THIRD_DATE = new Date(1 + 3600 * 1000 + 3600 * 1000 + 1);
  private static final Duration WINDOW_SIZE = Duration.standardHours(1);

  @BeforeClass
  public static void prepare() {
    beamSqlEnv.registerTable("ORDER_DETAILS", MockedUnboundedTable
        .of(
            Types.INTEGER, "order_id",
            Types.INTEGER, "site_id",
            Types.INTEGER, "price",
            Types.TIMESTAMP, "order_time"
        )
        .timestampColumnIndex(3)
        .addRows(
            Duration.ZERO,
            1, 1, 1, FIRST_DATE,
            1, 2, 2, FIRST_DATE
        )
        .addRows(
            WINDOW_SIZE.plus(Duration.standardSeconds(1)),
            2, 2, 3, SECOND_DATE,
            2, 3, 3, SECOND_DATE,
            // this late data is omitted
            1, 2, 3, FIRST_DATE
        )
        .addRows(
            WINDOW_SIZE.plus(WINDOW_SIZE).plus(Duration.standardSeconds(1)),
            3, 3, 3, THIRD_DATE,
            // this late data is omitted
            2, 2, 3, SECOND_DATE
        )
    );

    beamSqlEnv.registerTable("ORDER_DETAILS1", MockedBeamSqlTable
        .of(SqlTypeName.INTEGER, "order_id",
            SqlTypeName.VARCHAR, "buyer",

            1, "james",
            2, "bond"
        ));
  }

  @Test
  public void testInnerJoin_unboundedTableOnTheLeftSide() throws Exception {
    String sql = "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
        + " JOIN "
        + " ORDER_DETAILS1 o2 "
        + " on "
        + " o1.order_id=o2.order_id"
        ;

    PCollection<BeamSqlRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, beamSqlEnv);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                Types.INTEGER, "order_id",
                Types.INTEGER, "sum_site_id",
                Types.VARCHAR, "buyer"
            ).values(
                1, 3, "james",
                2, 5, "bond"
            ).getStringRows()
        );
    pipeline.run();
  }

  @Test
  public void testInnerJoin_boundedTableOnTheLeftSide() throws Exception {
    String sql = "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
        + " ORDER_DETAILS1 o2 "
        + " JOIN "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
        + " on "
        + " o1.order_id=o2.order_id"
        ;

    PCollection<BeamSqlRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, beamSqlEnv);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                Types.INTEGER, "order_id",
                Types.INTEGER, "sum_site_id",
                Types.VARCHAR, "buyer"
            ).values(
                1, 3, "james",
                2, 5, "bond"
            ).getStringRows()
        );
    pipeline.run();
  }

  @Test
  public void testLeftOuterJoin() throws Exception {
    String sql = "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
        + " LEFT OUTER JOIN "
        + " ORDER_DETAILS1 o2 "
        + " on "
        + " o1.order_id=o2.order_id"
        ;

    PCollection<BeamSqlRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, beamSqlEnv);
    rows.apply(ParDo.of(new BeamSqlOutputToConsoleFn("helloworld")));
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                Types.INTEGER, "order_id",
                Types.INTEGER, "sum_site_id",
                Types.VARCHAR, "buyer"
            ).values(
                1, 3, "james",
                2, 5, "bond",
                3, 3, null
            ).getStringRows()
        );
    pipeline.run();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testLeftOuterJoinError() throws Exception {
    String sql = "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
        + " ORDER_DETAILS1 o2 "
        + " LEFT OUTER JOIN "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
        + " on "
        + " o1.order_id=o2.order_id"
        ;
    pipeline.enableAbandonedNodeEnforcement(false);
    BeamSqlCli.compilePipeline(sql, pipeline, beamSqlEnv);
    pipeline.run();
  }

  @Test
  public void testRightOuterJoin() throws Exception {
    String sql = "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
        + " ORDER_DETAILS1 o2 "
        + " RIGHT OUTER JOIN "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
        + " on "
        + " o1.order_id=o2.order_id"
        ;
    PCollection<BeamSqlRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, beamSqlEnv);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                Types.INTEGER, "order_id",
                Types.INTEGER, "sum_site_id",
                Types.VARCHAR, "buyer"
            ).values(
                1, 3, "james",
                2, 5, "bond",
                3, 3, null
            ).getStringRows()
        );
    pipeline.run();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testRightOuterJoinError() throws Exception {
    String sql = "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
        + " RIGHT OUTER JOIN "
        + " ORDER_DETAILS1 o2 "
        + " on "
        + " o1.order_id=o2.order_id"
        ;

    pipeline.enableAbandonedNodeEnforcement(false);
    BeamSqlCli.compilePipeline(sql, pipeline, beamSqlEnv);
    pipeline.run();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testFullOuterJoinError() throws Exception {
    String sql = "SELECT o1.order_id, o1.sum_site_id, o2.buyer FROM "
        + " ORDER_DETAILS1 o2 "
        + " FULL OUTER JOIN "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
        + " on "
        + " o1.order_id=o2.order_id"
        ;
    pipeline.enableAbandonedNodeEnforcement(false);
    BeamSqlCli.compilePipeline(sql, pipeline, beamSqlEnv);
    pipeline.run();
  }
}
