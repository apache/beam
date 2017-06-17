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
import org.apache.beam.dsls.sql.planner.MockedBeamSqlTable;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/**
 * Test for {@code BeamMinusRel}.
 */
public class BeamMinusRelTest {
  static BeamSqlEnv sqlEnv = new BeamSqlEnv();

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();
  private MockedBeamSqlTable orderDetailsTable1 = MockedBeamSqlTable
      .of(SqlTypeName.BIGINT, "order_id",
          SqlTypeName.INTEGER, "site_id",
          SqlTypeName.DOUBLE, "price",
          1L, 1, 1.0,
          1L, 1, 1.0,
          2L, 2, 2.0,
          4L, 4, 4.0,
          4L, 4, 4.0
      );

  private MockedBeamSqlTable orderDetailsTable2 = MockedBeamSqlTable
      .of(SqlTypeName.BIGINT, "order_id",
          SqlTypeName.INTEGER, "site_id",
          SqlTypeName.DOUBLE, "price",
          1L, 1, 1.0,
          2L, 2, 2.0,
          3L, 3, 3.0
      );

  @Before
  public void setUp() {
    sqlEnv.registerTable("ORDER_DETAILS1", orderDetailsTable1);
    sqlEnv.registerTable("ORDER_DETAILS2", orderDetailsTable2);
    MockedBeamSqlTable.CONTENT.clear();
  }

  @Test
  public void testExcept() throws Exception {
    String sql = "";
    sql += "SELECT order_id, site_id, price "
        + "FROM ORDER_DETAILS1 "
        + " EXCEPT "
        + "SELECT order_id, site_id, price "
        + "FROM ORDER_DETAILS2 ";

    PCollection<BeamSqlRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, sqlEnv);
    PAssert.that(rows).containsInAnyOrder(
        MockedBeamSqlTable.of(
        SqlTypeName.BIGINT, "order_id",
        SqlTypeName.INTEGER, "site_id",
        SqlTypeName.DOUBLE, "price",
            4L, 4, 4.0
    ).getInputRecords());

    pipeline.run();
  }

  @Test
  public void testExceptAll() throws Exception {
    String sql = "";
    sql += "SELECT order_id, site_id, price "
        + "FROM ORDER_DETAILS1 "
        + " EXCEPT ALL "
        + "SELECT order_id, site_id, price "
        + "FROM ORDER_DETAILS2 ";

    PCollection<BeamSqlRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, sqlEnv);
    PAssert.that(rows).satisfies(new CheckSize(2));

    PAssert.that(rows).containsInAnyOrder(
        MockedBeamSqlTable.of(
            SqlTypeName.BIGINT, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.DOUBLE, "price",
            4L, 4, 4.0,
            4L, 4, 4.0
        ).getInputRecords());

    pipeline.run();
  }
}
