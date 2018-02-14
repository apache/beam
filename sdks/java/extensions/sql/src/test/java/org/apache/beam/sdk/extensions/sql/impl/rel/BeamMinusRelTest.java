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
 * Test for {@code BeamMinusRel}.
 */
public class BeamMinusRelTest extends BaseRelTest {
  static BeamSqlEnv sqlEnv = new BeamSqlEnv();

  @Rule
  public final TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void prepare() {
    sqlEnv.registerTable("ORDER_DETAILS1",
        MockedBoundedTable.of(
            SqlTypeCoders.BIGINT, "order_id",
            SqlTypeCoders.INTEGER, "site_id",
            SqlTypeCoders.DOUBLE, "price"
        ).addRows(
            1L, 1, 1.0,
            1L, 1, 1.0,
            2L, 2, 2.0,
            4L, 4, 4.0,
            4L, 4, 4.0
        )
    );

    sqlEnv.registerTable("ORDER_DETAILS2",
        MockedBoundedTable.of(
            SqlTypeCoders.BIGINT, "order_id",
            SqlTypeCoders.INTEGER, "site_id",
            SqlTypeCoders.DOUBLE, "price"
        ).addRows(
            1L, 1, 1.0,
            2L, 2, 2.0,
            3L, 3, 3.0
        )
    );
  }

  @Test
  public void testExcept() throws Exception {
    String sql = "";
    sql += "SELECT order_id, site_id, price "
        + "FROM ORDER_DETAILS1 "
        + " EXCEPT "
        + "SELECT order_id, site_id, price "
        + "FROM ORDER_DETAILS2 ";

    PCollection<Row> rows = compilePipeline(sql, pipeline, sqlEnv);
    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            SqlTypeCoders.BIGINT, "order_id",
            SqlTypeCoders.INTEGER, "site_id",
            SqlTypeCoders.DOUBLE, "price"
        ).addRows(
            4L, 4, 4.0
        ).getRows());

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

    PCollection<Row> rows = compilePipeline(sql, pipeline, sqlEnv);
    PAssert.that(rows).satisfies(new CheckSize(2));

    PAssert.that(rows).containsInAnyOrder(
        TestUtils.RowsBuilder.of(
            SqlTypeCoders.BIGINT, "order_id",
            SqlTypeCoders.INTEGER, "site_id",
            SqlTypeCoders.DOUBLE, "price"
        ).addRows(
            4L, 4, 4.0,
            4L, 4, 4.0
        ).getRows());

    pipeline.run();
  }
}
