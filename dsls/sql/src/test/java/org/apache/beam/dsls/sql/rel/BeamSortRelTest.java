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

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import org.apache.beam.dsls.sql.BeamSqlCli;
import org.apache.beam.dsls.sql.BeamSqlEnv;
import org.apache.beam.dsls.sql.exception.BeamSqlUnsupportedException;
import org.apache.beam.dsls.sql.planner.MockedBeamSqlTable;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test for {@code BeamSortRel}.
 */
public class BeamSortRelTest {
  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  private static MockedBeamSqlTable subOrderRamTable = MockedBeamSqlTable.of(
      SqlTypeName.BIGINT, "order_id",
      SqlTypeName.INTEGER, "site_id",
      SqlTypeName.DOUBLE, "price");

  private static MockedBeamSqlTable orderDetailTable = MockedBeamSqlTable
      .of(SqlTypeName.BIGINT, "order_id",
          SqlTypeName.INTEGER, "site_id",
          SqlTypeName.DOUBLE, "price",
          SqlTypeName.TIMESTAMP, "order_time",

          1L, 2, 1.0, new Date(),
          1L, 1, 2.0, new Date(),
          2L, 4, 3.0, new Date(),
          2L, 1, 4.0, new Date(),
          5L, 5, 5.0, new Date(),
          6L, 6, 6.0, new Date(),
          7L, 7, 7.0, new Date(),
          8L, 8888, 8.0, new Date(),
          8L, 999, 9.0, new Date(),
          10L, 100, 10.0, new Date());

  @Test
  public void testOrderBy_basic() throws Exception {
    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + "ORDER BY order_id asc, site_id desc limit 4";

    System.out.println(sql);
    BeamSqlCli.compilePipeline(sql, pipeline);
    pipeline.run().waitUntilFinish();

    assertEquals(
        MockedBeamSqlTable.of(
            SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price",
            1L, 2, 1.0,
            1L, 1, 2.0,
            2L, 4, 3.0,
            2L, 1, 4.0
        ).getInputRecords(), MockedBeamSqlTable.CONTENT);
  }

  @Test
  public void testOrderBy_nullsFirst() throws Exception {
    BeamSqlEnv.registerTable("ORDER_DETAILS", MockedBeamSqlTable
        .of(SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price",

            1L, 2, 1.0,
            1L, null, 2.0,
            2L, 1, 3.0,
            2L, null, 4.0,
            5L, 5, 5.0));
    BeamSqlEnv.registerTable("SUB_ORDER_RAM", MockedBeamSqlTable
        .of(SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price"));

    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + "ORDER BY order_id asc, site_id desc NULLS FIRST limit 4";

    BeamSqlCli.compilePipeline(sql, pipeline);
    pipeline.run().waitUntilFinish();

    assertEquals(
        MockedBeamSqlTable.of(
            SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price",

            1L, null, 2.0,
            1L, 2, 1.0,
            2L, null, 4.0,
            2L, 1, 3.0
        ).getInputRecords(), MockedBeamSqlTable.CONTENT);
  }

  @Test
  public void testOrderBy_nullsLast() throws Exception {
    BeamSqlEnv.registerTable("ORDER_DETAILS", MockedBeamSqlTable
        .of(SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price",

            1L, 2, 1.0,
            1L, null, 2.0,
            2L, 1, 3.0,
            2L, null, 4.0,
            5L, 5, 5.0));
    BeamSqlEnv.registerTable("SUB_ORDER_RAM", MockedBeamSqlTable
        .of(SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price"));

    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + "ORDER BY order_id asc, site_id desc NULLS LAST limit 4";

    BeamSqlCli.compilePipeline(sql, pipeline);
    pipeline.run().waitUntilFinish();

    assertEquals(
        MockedBeamSqlTable.of(
            SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price",

            1L, 2, 1.0,
            1L, null, 2.0,
            2L, 1, 3.0,
            2L, null, 4.0
        ).getInputRecords(), MockedBeamSqlTable.CONTENT);
  }

  @Test
  public void testOrderBy_with_offset() throws Exception {
    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + "ORDER BY order_id asc, site_id desc limit 4 offset 4";

    BeamSqlCli.compilePipeline(sql, pipeline);
    pipeline.run().waitUntilFinish();

    assertEquals(
        MockedBeamSqlTable.of(
            SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price",

            5L, 5, 5.0,
            6L, 6, 6.0,
            7L, 7, 7.0,
            8L, 8888, 8.0
        ).getInputRecords(), MockedBeamSqlTable.CONTENT);
  }

  @Test
  public void testOrderBy_bigFetch() throws Exception {
    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + "ORDER BY order_id asc, site_id desc limit 11";

    BeamSqlCli.compilePipeline(sql, pipeline);
    pipeline.run().waitUntilFinish();

    assertEquals(
        MockedBeamSqlTable.of(
            SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price",

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
        ).getInputRecords(), MockedBeamSqlTable.CONTENT);
  }

  @Test(expected = BeamSqlUnsupportedException.class)
  public void testOrderBy_exception() throws Exception {
    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id)  SELECT "
        + " order_id, COUNT(*) "
        + "FROM ORDER_DETAILS "
        + "GROUP BY order_id, TUMBLE(order_time, INTERVAL '1' HOUR)"
        + "ORDER BY order_id asc limit 11";

    TestPipeline pipeline = TestPipeline.create();
    BeamSqlCli.compilePipeline(sql, pipeline);
  }

  @Before
  public void prepare() {
    BeamSqlEnv.registerTable("ORDER_DETAILS", orderDetailTable);
    BeamSqlEnv.registerTable("SUB_ORDER_RAM", subOrderRamTable);
    MockedBeamSqlTable.CONTENT.clear();
  }

  private void assertEquals(Collection<BeamSqlRow> rows1, Collection<BeamSqlRow> rows2) {
    Assert.assertEquals(rows1.size(), rows2.size());

    Iterator<BeamSqlRow> it1 = rows1.iterator();
    Iterator<BeamSqlRow> it2 = rows2.iterator();
    while (it1.hasNext()) {
      Assert.assertEquals(it1.next(), it2.next());
    }
  }
}
