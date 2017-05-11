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

import java.util.List;

import org.apache.beam.dsls.sql.planner.BeamSqlRunner;
import org.apache.beam.dsls.sql.planner.MockedBeamSQLTable;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for {@code BeamSortRel}.
 */
public class BeamSortRelTest {
  public static BeamSqlRunner runner = new BeamSqlRunner();

  @Test
  public void testOrderBy_basic() throws Exception {
    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + "ORDER BY order_id asc, site_id desc limit 4";

    runner.submitQuery(sql);

    assertEquals(
        getTable(
            1L, 2, 1.0,
            1L, 1, 2.0,
            2L, 4, 3.0,
            2L, 1, 4.0
        ).getInputRecords(), MockedBeamSQLTable.CONTENT);
  }

  @Test
  public void testOrderBy_with_offset() throws Exception {
    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + "ORDER BY order_id asc, site_id desc limit 4 offset 4";

    runner.submitQuery(sql);

    assertEquals(
        getTable(
            5L, 5, 5.0,
            6L, 6, 6.0,
            7L, 7, 7.0,
            8L, 8888, 8.0
        ).getInputRecords(), MockedBeamSQLTable.CONTENT);
  }

  @Test
  public void testOrderBy_bigFetch() throws Exception {
    String sql = "INSERT INTO SUB_ORDER_RAM(order_id, site_id, price)  SELECT "
        + " order_id, site_id, price "
        + "FROM ORDER_DETAILS "
        + "ORDER BY order_id asc, site_id desc limit 11";

    runner.submitQuery(sql);

    assertEquals(
        getTable(
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

  @BeforeClass
  public static void prepare() {
    runner.addTable("ORDER_DETAILS", getTable(
        1L, 2, 1.0,
        1L, 1, 2.0,
        2L, 4, 3.0,
        2L, 1, 4.0,
        5L, 5, 5.0,
        6L, 6, 6.0,
        7L, 7, 7.0,
        8L, 8888, 8.0,
        8L, 999, 9.0,
        10L, 100, 10.0));
    runner.addTable("SUB_ORDER_RAM", getTable());
  }

  private void assertEquals(List<BeamSQLRow> rows1, List<BeamSQLRow> rows2) {
    Assert.assertEquals(rows1.size(), rows2.size());
    for (int i = 0; i < rows1.size(); i++) {
      Assert.assertEquals(rows1.get(i), rows2.get(i));
    }
  }

  private static MockedBeamSQLTable getTable(Object... args) {
    final RelProtoDataType protoRowType = new RelProtoDataType() {
      @Override
      public RelDataType apply(RelDataTypeFactory a0) {
        return a0.builder()
            .add("order_id", SqlTypeName.BIGINT)
            .add("site_id", SqlTypeName.INTEGER)
            .add("price", SqlTypeName.DOUBLE)
            .build();
      }
    };

    return new MockedBeamSQLTable(protoRowType).withInputRecords(args);
  }

}
