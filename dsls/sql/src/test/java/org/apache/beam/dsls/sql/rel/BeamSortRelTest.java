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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.beam.dsls.sql.planner.BeamSqlRunner;
import org.apache.beam.dsls.sql.planner.MockedBeamSQLTable;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@code BeamSortRel}.
 */
public class BeamSortRelTest {
  public static BeamSqlRunner runner = new BeamSqlRunner();
  private static MockedBeamSQLTable subOrderRamTable = MockedBeamSQLTable.of(
      SqlTypeName.BIGINT, "order_id",
      SqlTypeName.INTEGER, "site_id",
      SqlTypeName.DOUBLE, "price");

  private static MockedBeamSQLTable orderDetailTable = MockedBeamSQLTable
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
    prepare();
    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + "ORDER BY order_id asc, site_id desc limit 4";

    runner.submitQuery(sql);

    assertEquals(
        MockedBeamSQLTable.of(
            SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price",

            1L, 2, 1.0,
            1L, 1, 2.0,
            2L, 4, 3.0,
            2L, 1, 4.0
        ).getInputRecords(), MockedBeamSQLTable.CONTENT);
  }

  @Test
  public void testOrderBy_nullsFirst() throws Exception {
    runner.addTable("ORDER_DETAILS", MockedBeamSQLTable
        .of(SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price",

            1L, 2, 1.0,
            1L, null, 2.0,
            2L, 1, 3.0,
            2L, null, 4.0,
            5L, 5, 5.0));
    runner.addTable("SUB_ORDER_RAM", MockedBeamSQLTable
        .of(SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price"));

    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + "ORDER BY order_id asc, site_id desc NULLS FIRST limit 4";

    runner.submitQuery(sql);

    assertEquals(
        MockedBeamSQLTable.of(
            SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price",

            1L, null, 2.0,
            1L, 2, 1.0,
            2L, null, 4.0,
            2L, 1, 3.0
        ).getInputRecords(), MockedBeamSQLTable.CONTENT);
  }

  @Test
  public void testOrderBy_nullsLast() throws Exception {
    runner.addTable("ORDER_DETAILS", MockedBeamSQLTable
        .of(SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price",

            1L, 2, 1.0,
            1L, null, 2.0,
            2L, 1, 3.0,
            2L, null, 4.0,
            5L, 5, 5.0));
    runner.addTable("SUB_ORDER_RAM", MockedBeamSQLTable
        .of(SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price"));

    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + "ORDER BY order_id asc, site_id desc NULLS LAST limit 4";

    runner.submitQuery(sql);

    assertEquals(
        MockedBeamSQLTable.of(
            SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price",

            1L, 2, 1.0,
            1L, null, 2.0,
            2L, 1, 3.0,
            2L, null, 4.0
        ).getInputRecords(), MockedBeamSQLTable.CONTENT);
  }

  @Test
  public void testOrderBy_with_offset() throws Exception {
    prepare();
    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + "ORDER BY order_id asc, site_id desc limit 4 offset 4";

    runner.submitQuery(sql);

    assertEquals(
        MockedBeamSQLTable.of(
            SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price",

            5L, 5, 5.0,
            6L, 6, 6.0,
            7L, 7, 7.0,
            8L, 8888, 8.0
        ).getInputRecords(), MockedBeamSQLTable.CONTENT);
  }

  @Test
  public void testOrderBy_bigFetch() throws Exception {
    prepare();
    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + "ORDER BY order_id asc, site_id desc limit 11";

    runner.submitQuery(sql);

    assertEquals(
        MockedBeamSQLTable.of(
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
        ).getInputRecords(), MockedBeamSQLTable.CONTENT);
  }

  public static void prepare() {
    runner.addTable("ORDER_DETAILS", orderDetailTable);
    runner.addTable("SUB_ORDER_RAM", subOrderRamTable);
  }

  private void assertEquals(List<BeamSQLRow> rows1, List<BeamSQLRow> rows2) {
    Assert.assertEquals(rows1.size(), rows2.size());
    for (int i = 0; i < rows1.size(); i++) {
      Assert.assertEquals(rows1.get(i), rows2.get(i));
    }
  }

  // TODO add windowed ORDER BY test case.
  // TODO add test for streaming table

  @Test
  public void testOrderBy_window() throws Exception {
    runner.addTable("ORDER_DETAILS", MockedBeamSQLTable.of(
        SqlTypeName.BIGINT, "order_id",
        SqlTypeName.INTEGER, "site_id",
        SqlTypeName.DOUBLE, "price",
        SqlTypeName.TIMESTAMP, "order_time",

        // 1 -> window1 -> size:4
        1L, 2, 1.0, df("20170512 10:01:00"),
        1L, 1, 2.0, df("20170512 10:02:00"),
        1L, 4, 3.0, df("20170512 10:05:00"),
        1L, 1, 4.0, df("20170512 10:07:00"),

        // 1 -> window2 -> size:1
        1L, 5, 5.0, df("20170512 10:11:00"),

        // 1 -> window3 -> size:1
        1L, 6, 6.0, df("20170512 10:21:00"),

        // 1 -> window4 -> size:4
        1L, 7, 7.0, df("20170512 10:32:00"),
        1L, 8888, 8.0, df("20170512 10:33:00"),
        1L, 999, 9.0, df("20170512 10:34:00"),
        1L, 100, 10.0, df("20170512 10:35:00"),

        // 2 -> window1 -> size:2
        2L, 2, 1.0, df("20170512 10:01:00"),
        2L, 1, 2.0, df("20170512 10:02:00"),

        // 2 -> window2 -> size:3
        2L, 4, 3.0, df("20170512 10:11:00"),
        2L, 1, 4.0, df("20170512 10:12:00"),
        2L, 5, 5.0, df("20170512 10:13:00"),

        // 2 -> window3 -> size:5
        2L, 6, 6.0, df("20170512 10:21:00"),
        2L, 7, 7.0, df("20170512 10:22:00"),
        2L, 8888, 8.0, df("20170512 10:23:00"),
        2L, 999, 9.0, df("20170512 10:24:00"),
        2L, 100, 10.0, df("20170512 10:25:00")
    ));
    runner.addTable("SUB_ORDER_RAM", subOrderRamTable);

    String sql =
        "INSERT INTO SUB_ORDER_RAM (order_id, price) "
            + "SELECT order_id" + ", COUNT(*) AS `SIZE`" + "FROM ORDER_DETAILS "
            + " GROUP BY order_id, TUMBLE(order_time, INTERVAL '10' MINUTE) "
            + " ORDER BY order_id LIMIT 2";

    runner.submitQuery(sql);

    System.out.println();
    System.out.println();
    for (BeamSQLRow row : MockedBeamSQLTable.CONTENT) {
      System.out.println(row.valueInString());
    }
  }

  public static Date df(String input) {
    try {
      return new SimpleDateFormat("YYYYMMDD HH:mm:ss").parse(input);
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return null;
  }
}
