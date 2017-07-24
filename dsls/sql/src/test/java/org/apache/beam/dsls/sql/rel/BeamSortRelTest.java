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
import org.apache.beam.dsls.sql.mock.MockedBoundedTable;
import org.apache.beam.sdk.sd.BeamRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test for {@code BeamSortRel}.
 */
public class BeamSortRelTest {
  static BeamSqlEnv sqlEnv = new BeamSqlEnv();

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  @Before
  public void prepare() {
    sqlEnv.registerTable("ORDER_DETAILS",
        MockedBoundedTable.of(
            Types.BIGINT, "order_id",
            Types.INTEGER, "site_id",
            Types.DOUBLE, "price",
            Types.TIMESTAMP, "order_time"
        ).addRows(
            1L, 2, 1.0, new Date(),
            1L, 1, 2.0, new Date(),
            2L, 4, 3.0, new Date(),
            2L, 1, 4.0, new Date(),
            5L, 5, 5.0, new Date(),
            6L, 6, 6.0, new Date(),
            7L, 7, 7.0, new Date(),
            8L, 8888, 8.0, new Date(),
            8L, 999, 9.0, new Date(),
            10L, 100, 10.0, new Date()
        )
    );
    sqlEnv.registerTable("SUB_ORDER_RAM",
        MockedBoundedTable.of(
            Types.BIGINT, "order_id",
            Types.INTEGER, "site_id",
            Types.DOUBLE, "price"
        )
    );
  }

  @Test
  public void testOrderBy_basic() throws Exception {
    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + "ORDER BY order_id asc, site_id desc limit 4";

    PCollection<BeamRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, sqlEnv);
    PAssert.that(rows).containsInAnyOrder(TestUtils.RowsBuilder.of(
        Types.BIGINT, "order_id",
        Types.INTEGER, "site_id",
        Types.DOUBLE, "price"
    ).addRows(
        1L, 2, 1.0,
        1L, 1, 2.0,
        2L, 4, 3.0,
        2L, 1, 4.0
    ).getRows());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testOrderBy_nullsFirst() throws Exception {
    sqlEnv.registerTable("ORDER_DETAILS",
        MockedBoundedTable.of(
            Types.BIGINT, "order_id",
            Types.INTEGER, "site_id",
            Types.DOUBLE, "price"
        ).addRows(
            1L, 2, 1.0,
            1L, null, 2.0,
            2L, 1, 3.0,
            2L, null, 4.0,
            5L, 5, 5.0
        )
    );
    sqlEnv.registerTable("SUB_ORDER_RAM", MockedBoundedTable
        .of(Types.BIGINT, "order_id",
            Types.INTEGER, "site_id",
            Types.DOUBLE, "price"));

    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + "ORDER BY order_id asc, site_id desc NULLS FIRST limit 4";

    PCollection<BeamRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, sqlEnv);
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.BIGINT, "order_id",
            Types.INTEGER, "site_id",
            Types.DOUBLE, "price"
        ).addRows(
            1L, null, 2.0,
            1L, 2, 1.0,
            2L, null, 4.0,
            2L, 1, 3.0
        ).getRows()
    );
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testOrderBy_nullsLast() throws Exception {
    sqlEnv.registerTable("ORDER_DETAILS", MockedBoundedTable
        .of(Types.BIGINT, "order_id",
            Types.INTEGER, "site_id",
            Types.DOUBLE, "price"
        ).addRows(
            1L, 2, 1.0,
            1L, null, 2.0,
            2L, 1, 3.0,
            2L, null, 4.0,
            5L, 5, 5.0));
    sqlEnv.registerTable("SUB_ORDER_RAM", MockedBoundedTable
        .of(Types.BIGINT, "order_id",
            Types.INTEGER, "site_id",
            Types.DOUBLE, "price"));

    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + "ORDER BY order_id asc, site_id desc NULLS LAST limit 4";

    PCollection<BeamRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, sqlEnv);
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.BIGINT, "order_id",
            Types.INTEGER, "site_id",
            Types.DOUBLE, "price"
        ).addRows(
            1L, 2, 1.0,
            1L, null, 2.0,
            2L, 1, 3.0,
            2L, null, 4.0
        ).getRows()
    );
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testOrderBy_with_offset() throws Exception {
    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + "ORDER BY order_id asc, site_id desc limit 4 offset 4";

    PCollection<BeamRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, sqlEnv);
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.BIGINT, "order_id",
            Types.INTEGER, "site_id",
            Types.DOUBLE, "price"
        ).addRows(
            5L, 5, 5.0,
            6L, 6, 6.0,
            7L, 7, 7.0,
            8L, 8888, 8.0
        ).getRows()
    );
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testOrderBy_bigFetch() throws Exception {
    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + "ORDER BY order_id asc, site_id desc limit 11";

    PCollection<BeamRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, sqlEnv);
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            Types.BIGINT, "order_id",
            Types.INTEGER, "site_id",
            Types.DOUBLE, "price"
        ).addRows(
            1L, 2, 1.0,
            1L, 1, 2.0,
            2L, 4, 3.0,
            2L, 1, 4.0,
            5L, 5, 5.0,
            6L, 6, 6.0,
            7L, 7, 7.0,
            8L, 8888, 8.0,
            8L, 999, 9.0,
            10L, 100, 10.0
        ).getRows()
    );
    pipeline.run().waitUntilFinish();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testOrderBy_exception() throws Exception {
    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id)  SELECT "
        + " order_id, COUNT(*) "
        + "FROM ORDER_DETAILS "
        + "GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)"
        + "ORDER BY order_id asc limit 11";

    TestPipeline pipeline = TestPipeline.create();
    BeamSqlCli.compilePipeline(sql, pipeline, sqlEnv);
  }
}
