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
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/**
 * Bounded + Bounded Test for {@code BeamJoinRel}.
 */
public class BeamJoinRelBoundedVsBoundedTest {
  @Rule
  public final TestPipeline pipeline = TestPipeline.create();
  private static final BeamSqlEnv beamSqlEnv = new BeamSqlEnv();

  @Test
  public void testInnerJoin() throws Exception {
    String sql =
        "SELECT *  "
        + "FROM ORDER_DETAILS o1"
        + " JOIN ORDER_DETAILS o2"
        + " on "
        + " o1.order_id=o2.site_id AND o2.price=o1.site_id"
        ;

    PCollection<BeamSqlRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, beamSqlEnv);
    PAssert.that(rows).containsInAnyOrder(MockedBeamSqlTable.of(
        SqlTypeName.INTEGER, "order_id",
        SqlTypeName.INTEGER, "site_id",
        SqlTypeName.INTEGER, "price",
        SqlTypeName.INTEGER, "order_id0",
        SqlTypeName.INTEGER, "site_id0",
        SqlTypeName.INTEGER, "price0",

        2, 3, 3, 1, 2, 3
        ).getInputRecords());
    pipeline.run();
  }

  @Test
  public void testLeftOuterJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS o1"
            + " LEFT OUTER JOIN ORDER_DETAILS0 o2"
            + " on "
            + " o1.order_id=o2.site_id0 AND o2.price0=o1.site_id"
        ;

    PCollection<BeamSqlRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, beamSqlEnv);
    pipeline.enableAbandonedNodeEnforcement(false);
    PAssert.that(rows).containsInAnyOrder(MockedBeamSqlTable.of(
        SqlTypeName.INTEGER, "order_id",
        SqlTypeName.INTEGER, "site_id",
        SqlTypeName.INTEGER, "price",
        SqlTypeName.INTEGER, "order_id0",
        SqlTypeName.INTEGER, "site_id0",
        SqlTypeName.INTEGER, "price0",

        1, 2, 3, null, null, null,
        2, 3, 3, 1, 2, 3,
        3, 4, 5, null, null, null
    ).getInputRecords());
    pipeline.run();
  }

  @Test
  public void testRightOuterJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS o1"
            + " RIGHT OUTER JOIN ORDER_DETAILS o2"
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id"
        ;

    PCollection<BeamSqlRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, beamSqlEnv);
    PAssert.that(rows).containsInAnyOrder(MockedBeamSqlTable.of(
        SqlTypeName.INTEGER, "order_id",
        SqlTypeName.INTEGER, "site_id",
        SqlTypeName.INTEGER, "price",
        SqlTypeName.INTEGER, "order_id0",
        SqlTypeName.INTEGER, "site_id0",
        SqlTypeName.INTEGER, "price0",

        2, 3, 3, 1, 2, 3,
        null, null, null, 2, 3, 3,
        null, null, null, 3, 4, 5
    ).getInputRecords());
    pipeline.run();
  }

  @Test
  public void testFullOuterJoin() throws Exception {
    String sql =
        "SELECT *  "
            + "FROM ORDER_DETAILS o1"
            + " FULL OUTER JOIN ORDER_DETAILS o2"
            + " on "
            + " o1.order_id=o2.site_id AND o2.price=o1.site_id"
        ;

    PCollection<BeamSqlRow> rows = BeamSqlCli.compilePipeline(sql, pipeline, beamSqlEnv);
    PAssert.that(rows).containsInAnyOrder(MockedBeamSqlTable.of(
        SqlTypeName.INTEGER, "order_id",
        SqlTypeName.INTEGER, "site_id",
        SqlTypeName.INTEGER, "price",
        SqlTypeName.INTEGER, "order_id0",
        SqlTypeName.INTEGER, "site_id0",
        SqlTypeName.INTEGER, "price0",

        2, 3, 3, 1, 2, 3,
        1, 2, 3, null, null, null,
        3, 4, 5, null, null, null,
        null, null, null, 2, 3, 3,
        null, null, null, 3, 4, 5
    ).getInputRecords());
    pipeline.run();
  }

  @BeforeClass
  public static void prepare() {
    beamSqlEnv.registerTable("ORDER_DETAILS",
        MockedBeamSqlTable
        .of(SqlTypeName.INTEGER, "order_id",
            SqlTypeName.INTEGER, "site_id",
            SqlTypeName.INTEGER, "price",

            1, 2, 3, 2, 3, 3, 3, 4, 5));

    beamSqlEnv.registerTable("ORDER_DETAILS0",
        MockedBeamSqlTable
            .of(SqlTypeName.INTEGER, "order_id0",
                SqlTypeName.INTEGER, "site_id0",
                SqlTypeName.INTEGER, "price0",

                1, 2, 3, 2, 3, 3, 3, 4, 5));

  }
}
