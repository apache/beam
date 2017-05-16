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

import static org.apache.beam.dsls.sql.BeamSQLTestUtils.assertEqualsIgnoreOrder;

import java.util.Date;

import org.apache.beam.dsls.sql.planner.BeamSqlRunner;
import org.apache.beam.dsls.sql.planner.MockedBeamSQLTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for {@code BeamUnionRel}.
 */
public class BeamUnionRelTest {
  public static BeamSqlRunner runner = new BeamSqlRunner();
  private MockedBeamSQLTable orderDetailsTable = MockedBeamSQLTable
      .of(SqlTypeName.BIGINT, "order_id",
          SqlTypeName.INTEGER, "site_id",
          SqlTypeName.DOUBLE, "price",
          SqlTypeName.TIMESTAMP, "order_time",

          1L, 1, 1.0, new Date(),
          2L, 2, 2.0, new Date());
  private MockedBeamSQLTable subOrderRamTable = MockedBeamSQLTable.of(
      SqlTypeName.BIGINT, "order_id",
      SqlTypeName.INTEGER, "site_id",
      SqlTypeName.DOUBLE, "price");
  private MockedBeamSQLTable subOrderRam1Table = MockedBeamSQLTable.of(
      SqlTypeName.BIGINT, "order_id",
      SqlTypeName.INTEGER, "site_id",
      SqlTypeName.INTEGER, "cnt");

  @Before
  public void setUp() {
    runner.addTable("ORDER_DETAILS", orderDetailsTable);
    runner.addTable("SUB_ORDER_RAM", subOrderRamTable);
    runner.addTable("SUB_ORDER_RAM1", subOrderRam1Table);
    MockedBeamSQLTable.CONTENT.clear();
  }

  @Test
  public void testBasic() throws Exception {
    String sql = "";
    sql += "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price) ";
    sql += "SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + " UNION SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS ";

    runner.submitQuery(sql);

    assertEqualsIgnoreOrder(
        MockedBeamSQLTable.of(
            SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price",
            1L, 1, 1.0,
            1L, 1, 1.0,
            2L, 2, 2.0,
            2L, 2, 2.0
        ).getInputRecords(), MockedBeamSQLTable.CONTENT);
  }

  @Test
  public void testUnionSameWindow() throws Exception {
    String sql = "";
    sql += "INSERT INTO SUB_ORDER_RAM1(order_id, site_id, cnt) ";
    sql += "SELECT "
        + " order_id, site_id, count(*) as cnt "
        + "FROM ORDER_DETAILS GROUP BY order_id, site_id"
        + ", TUMBLE(order_time, INTERVAL '1' HOUR) "
        + " UNION SELECT "
        + " order_id, site_id, count(*) as cnt "
        + "FROM ORDER_DETAILS GROUP BY order_id, site_id"
        + ", TUMBLE(order_time, INTERVAL '1' HOUR) ";

    runner.submitQuery(sql);

    assertEqualsIgnoreOrder(
        MockedBeamSQLTable.of(
            SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.INTEGER, "cnt",
            1L, 1, 1,
            1L, 1, 1,
            2L, 2, 1,
            2L, 2, 1
        ).getInputRecords(), MockedBeamSQLTable.CONTENT);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnionDifferentWindows() throws Exception {
    String sql = "";
    sql += "INSERT INTO SUB_ORDER_RAM1(order_id, site_id, cnt) ";
    sql += "SELECT "
        + " order_id, site_id, count(*) as cnt "
        + "FROM ORDER_DETAILS GROUP BY order_id, site_id"
        + ", TUMBLE(order_time, INTERVAL '1' HOUR) "
        + " UNION SELECT "
        + " order_id, site_id, count(*) as cnt "
        + "FROM ORDER_DETAILS GROUP BY order_id, site_id"
        + ", TUMBLE(order_time, INTERVAL '2' HOUR) ";

    runner.submitQuery(sql);
  }
}
