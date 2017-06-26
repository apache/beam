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
import org.apache.beam.dsls.sql.planner.MockedUnboundedTable;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Unbounded + Unbounded Test for {@code BeamJoinRel}.
 */
public class BeamJoinRelUnboundedVsUnboundedTest {
  @Rule
  public final TestPipeline pipeline = TestPipeline.create();
  private static final BeamSqlEnv beamSqlEnv = new BeamSqlEnv();
  public static final Date FIRST_DATE = new Date(1);
  public static final Date SECOND_DATE = new Date(1 + 3600 * 1000);

  private static final Duration WINDOW_SIZE = Duration.standardHours(1);

  @BeforeClass
  public static void prepare() {
    beamSqlEnv.registerTable("ORDER_DETAILS", MockedUnboundedTable
        .of(Types.INTEGER, "order_id",
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
            WINDOW_SIZE.plus(Duration.standardMinutes(1)),
            2, 2, 3, SECOND_DATE,
            2, 3, 3, SECOND_DATE,
            // this late record is omitted(First window)
            1, 3, 3, FIRST_DATE
        )
        .addRows(
            // this late record is omitted(Second window)
            WINDOW_SIZE.plus(WINDOW_SIZE).plus(Duration.standardMinutes(1)),
            2, 3, 3, SECOND_DATE
        )
    );
  }

  @Test
  public void testInnerJoin() throws Exception {
    String sql = "SELECT * FROM "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
        + " JOIN "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o2 "
        + " on "
        + " o1.order_id=o2.order_id"
        ;

    PCollection<BeamSqlRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, beamSqlEnv);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                Types.INTEGER, "order_id",
                Types.INTEGER, "sum_site_id",
                Types.INTEGER, "order_id0",
                Types.INTEGER, "sum_site_id0").values(
                1, 3, 1, 3,
                2, 5, 2, 5
            ).getStrRows()
        );
    pipeline.run();
  }

  @Test
  public void testLeftOuterJoin() throws Exception {
    String sql = "SELECT * FROM "
        + "(select site_id as order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY site_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
        + " LEFT OUTER JOIN "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o2 "
        + " on "
        + " o1.order_id=o2.order_id"
        ;

    // 1, 1 | 1, 3
    // 2, 2 | NULL, NULL
    // ---- | -----
    // 2, 2 | 2, 5
    // 3, 3 | NULL, NULL

    PCollection<BeamSqlRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, beamSqlEnv);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                Types.INTEGER, "order_id",
                Types.INTEGER, "sum_site_id",
                Types.INTEGER, "order_id0",
                Types.INTEGER, "sum_site_id0"
            ).values(
                1, 1, 1, 3,
                2, 2, null, null,
                2, 2, 2, 5,
                3, 3, null, null
            ).getStrRows()
        );
    pipeline.run();
  }

  @Test
  public void testRightOuterJoin() throws Exception {
    String sql = "SELECT * FROM "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
        + " RIGHT OUTER JOIN "
        + "(select site_id as order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY site_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o2 "
        + " on "
        + " o1.order_id=o2.order_id"
        ;

    PCollection<BeamSqlRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, beamSqlEnv);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                Types.INTEGER, "order_id",
                Types.INTEGER, "sum_site_id",
                Types.INTEGER, "order_id0",
                Types.INTEGER, "sum_site_id0"
            ).values(
                1, 3, 1, 1,
                null, null, 2, 2,
                2, 5, 2, 2,
                null, null, 3, 3
            ).getStrRows()
        );
    pipeline.run();
  }

  @Test
  public void testFullOuterJoin() throws Exception {
    String sql = "SELECT * FROM "
        + "(select site_id as order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY site_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o1 "
        + " LEFT OUTER JOIN "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o2 "
        + " on "
        + " o1.order_id=o2.order_id"
        ;

    // 1, 1 | 1, 3
    // 2, 2 | NULL, NULL
    // ---- | -----
    // 2, 2 | 2, 5
    // 3, 3 | NULL, NULL

    PCollection<BeamSqlRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, beamSqlEnv);
    PAssert.that(rows.apply(ParDo.of(new TestUtils.BeamSqlRow2StringDoFn())))
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                Types.INTEGER, "order_id",
                Types.INTEGER, "sum_site_id",
                Types.INTEGER, "order_id0",
                Types.INTEGER, "sum_site_id0"
            ).values(
                1, 1, 1, 3,
                2, 2, null, null,
                2, 2, 2, 5,
                3, 3, null, null
            ).getStrRows()
        );
    pipeline.run();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testWindowsMismatch() throws Exception {
    String sql = "SELECT * FROM "
        + "(select site_id as order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY site_id, TUMBLE(order_time, INTERVAL '2' HOUR)) o1 "
        + " LEFT OUTER JOIN "
        + "(select order_id, sum(site_id) as sum_site_id FROM ORDER_DETAILS "
        + "          GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)) o2 "
        + " on "
        + " o1.order_id=o2.order_id"
        ;
    pipeline.enableAbandonedNodeEnforcement(false);
    BeamSqlCli.compilePipeline(sql, pipeline, beamSqlEnv);
    pipeline.run();
  }
}
