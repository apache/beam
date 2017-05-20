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

import org.apache.beam.dsls.sql.BeamSqlCli;
import org.apache.beam.dsls.sql.BeamSqlEnv;
import org.apache.beam.dsls.sql.planner.MockedBeamSQLTable;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test for {@code BeamIntersectRel}.
 */
public class BeamIntersectRelTest {
  @Rule
  public final TestPipeline pipeline = TestPipeline.create();
  private static MockedBeamSQLTable orderDetailsTable1 = MockedBeamSQLTable
      .of(SqlTypeName.BIGINT, "order_id",
          SqlTypeName.INTEGER, "site_id",
          SqlTypeName.DOUBLE, "price",
          1L, 1, 1.0,
          1L, 1, 1.0,
          2L, 2, 2.0,
          4L, 4, 4.0
      );

  private static MockedBeamSQLTable orderDetailsTable2 = MockedBeamSQLTable
      .of(SqlTypeName.BIGINT, "order_id",
          SqlTypeName.INTEGER, "site_id",
          SqlTypeName.DOUBLE, "price",
          1L, 1, 1.0,
          2L, 2, 2.0,
          3L, 3, 3.0
      );

  @BeforeClass
  public static void setUp() {
    BeamSqlEnv.registerTable("ORDER_DETAILS1", orderDetailsTable1);
    BeamSqlEnv.registerTable("ORDER_DETAILS2", orderDetailsTable2);
  }

  @Test
  public void testIntersect() throws Exception {
    String sql = "";
    sql += "SELECT order_id, site_id, price "
        + "FROM ORDER_DETAILS1 "
        + " INTERSECT "
        + "SELECT order_id, site_id, price "
        + "FROM ORDER_DETAILS2 ";

    PCollection<BeamSQLRow> rows = BeamSqlCli.compilePipeline(sql, pipeline);
    PAssert.that(rows).containsInAnyOrder(
        MockedBeamSQLTable.of(
        SqlTypeName.BIGINT, "order_id",
        SqlTypeName.INTEGER, "site_id",
        SqlTypeName.DOUBLE, "price",

        1L, 1, 1.0,
        2L, 2, 2.0
    ).getInputRecords());

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testIntersectAll() throws Exception {
    String sql = "";
    sql += "SELECT order_id, site_id, price "
        + "FROM ORDER_DETAILS1 "
        + " INTERSECT ALL "
        + "SELECT order_id, site_id, price "
        + "FROM ORDER_DETAILS2 ";

    PCollection<BeamSQLRow> rows = BeamSqlCli.compilePipeline(sql, pipeline);
    PAssert.that(rows).satisfies(new CheckSize(3));

    PAssert.that(rows).containsInAnyOrder(
        MockedBeamSQLTable.of(
            SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price",
            1L, 1, 1.0,
            1L, 1, 1.0,
            2L, 2, 2.0
        ).getInputRecords());

    pipeline.run();
  }
}
