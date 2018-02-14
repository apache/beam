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

package org.apache.beam.sdk.extensions.sql.impl.rel;

import org.apache.beam.sdk.extensions.sql.SqlTypeCoders;
import org.apache.beam.sdk.extensions.sql.TestUtils;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.mock.MockedBoundedTable;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Bounded + Bounded Test for {@code BeamJoinRel}.
 */
public class BeamJoinRelBoundedVsBoundedTest extends BaseRelTest {
  @Rule
  public final TestPipeline pipeline = TestPipeline.create();
  private static final BeamSqlEnv BEAM_SQL_ENV = new BeamSqlEnv();

  public static final MockedBoundedTable ORDER_DETAILS1 =
      MockedBoundedTable.of(
          SqlTypeCoders.INTEGER, "order_id",
          SqlTypeCoders.INTEGER, "site_id",
          SqlTypeCoders.INTEGER, "price"
      ).addRows(
          1, 2, 3,
          2, 3, 3,
          3, 4, 5
      );

  public static final MockedBoundedTable ORDER_DETAILS2 =
      MockedBoundedTable.of(
          SqlTypeCoders.INTEGER, "order_id",
          SqlTypeCoders.INTEGER, "site_id",
          SqlTypeCoders.INTEGER, "price"
      ).addRows(
          1, 2, 3,
          2, 3, 3,
          3, 4, 5
      );

  @BeforeClass
  public static void prepare() {
    BEAM_SQL_ENV.registerTable("ORDER_DETAILS1", ORDER_DETAILS1);
    BEAM_SQL_ENV.registerTable("ORDER_DETAILS2", ORDER_DETAILS2);
  }

  @Test
  public void testInnerJoin() throws Exception {
    String sql =
        "SELECT *  "
        + "FROM ORDER_DETAILS1 o1"
        + " JOIN ORDER_DETAILS2 o2"
        + " on "
        + " o1.order_id=o2.site_id AND o2.price=o1.site_id"
        ;

    PCollection<Row> rows = compilePipeline(sql, pipeline, BEAM_SQL_ENV);
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            SqlTypeCoders.INTEGER, "order_id",
            SqlTypeCoders.INTEGER, "site_id",
            SqlTypeCoders.INTEGER, "price",
            SqlTypeCoders.INTEGER, "order_id0",
            SqlTypeCoders.INTEGER, "site_id0",
            SqlTypeCoders.INTEGER, "price0"
        ).addRows(
            2, 3, 3, 1, 2, 3
        ).getRows());
    pipeline.run();
  }

  @Test
  public void testLeftOuterJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " LEFT OUTER JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id"
        ;

    PCollection<Row> rows = compilePipeline(sql, pipeline, BEAM_SQL_ENV);
    pipeline.enableAbandonedNodeEnforcement(false);
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            SqlTypeCoders.INTEGER, "order_id",
            SqlTypeCoders.INTEGER, "site_id",
            SqlTypeCoders.INTEGER, "price",
            SqlTypeCoders.INTEGER, "order_id0",
            SqlTypeCoders.INTEGER, "site_id0",
            SqlTypeCoders.INTEGER, "price0"
        ).addRows(
            1, 2, 3, null, null, null,
            2, 3, 3, 1, 2, 3,
            3, 4, 5, null, null, null
        ).getRows());
    pipeline.run();
  }

  @Test
  public void testRightOuterJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " RIGHT OUTER JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id"
        ;

    PCollection<Row> rows = compilePipeline(sql, pipeline, BEAM_SQL_ENV);
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            SqlTypeCoders.INTEGER, "order_id",
            SqlTypeCoders.INTEGER, "site_id",
            SqlTypeCoders.INTEGER, "price",
            SqlTypeCoders.INTEGER, "order_id0",
            SqlTypeCoders.INTEGER, "site_id0",
            SqlTypeCoders.INTEGER, "price0"
        ).addRows(
            2, 3, 3, 1, 2, 3,
            null, null, null, 2, 3, 3,
            null, null, null, 3, 4, 5
        ).getRows());
    pipeline.run();
  }

  @Test
  public void testFullOuterJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " FULL OUTER JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id"
        ;

    PCollection<Row> rows = compilePipeline(sql, pipeline, BEAM_SQL_ENV);
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
          SqlTypeCoders.INTEGER, "order_id",
          SqlTypeCoders.INTEGER, "site_id",
          SqlTypeCoders.INTEGER, "price",
          SqlTypeCoders.INTEGER, "order_id0",
          SqlTypeCoders.INTEGER, "site_id0",
          SqlTypeCoders.INTEGER, "price0"
        ).addRows(
          2, 3, 3, 1, 2, 3,
          1, 2, 3, null, null, null,
          3, 4, 5, null, null, null,
          null, null, null, 2, 3, 3,
          null, null, null, 3, 4, 5
        ).getRows());
    pipeline.run();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testException_nonEqualJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1"
            + " JOIN ORDER_DETAILS2 o2"
            + " on "
            + " o1.order_id>o2.site_id"
        ;

    pipeline.enableAbandonedNodeEnforcement(false);
    compilePipeline(sql, pipeline, BEAM_SQL_ENV);
    pipeline.run();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testException_crossJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS1 o1, ORDER_DETAILS2 o2";

    pipeline.enableAbandonedNodeEnforcement(false);
    compilePipeline(sql, pipeline, BEAM_SQL_ENV);
    pipeline.run();
  }
}
