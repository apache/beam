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

import java.math.BigDecimal;
import org.apache.beam.sdk.extensions.sql.TestUtils;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/** Test for {@code BeamIntersectRel}. */
public class BeamIntersectRelTest extends BaseRelTest {

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void prepare() {
    registerTable(
        "ORDER_DETAILS1",
        TestBoundedTable.of(
                Schema.FieldType.INT64, "order_id",
                Schema.FieldType.INT32, "site_id",
                Schema.FieldType.DECIMAL, "price")
            .addRows(
                1L,
                1,
                new BigDecimal(1.0),
                1L,
                1,
                new BigDecimal(1.0),
                2L,
                2,
                new BigDecimal(2.0),
                4L,
                4,
                new BigDecimal(4.0)));

    registerTable(
        "ORDER_DETAILS2",
        TestBoundedTable.of(
                Schema.FieldType.INT64, "order_id",
                Schema.FieldType.INT32, "site_id",
                Schema.FieldType.DECIMAL, "price")
            .addRows(
                1L,
                1,
                new BigDecimal(1.0),
                2L,
                2,
                new BigDecimal(2.0),
                3L,
                3,
                new BigDecimal(3.0)));
  }

  @Test
  public void testIntersect() {
    String sql = "";
    sql +=
        "SELECT order_id, site_id, price "
            + "FROM ORDER_DETAILS1 "
            + " INTERSECT "
            + "SELECT order_id, site_id, price "
            + "FROM ORDER_DETAILS2 ";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.FieldType.INT64, "order_id",
                    Schema.FieldType.INT32, "site_id",
                    Schema.FieldType.DECIMAL, "price")
                .addRows(1L, 1, new BigDecimal(1.0), 2L, 2, new BigDecimal(2.0))
                .getRows());

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testIntersectAll() {
    String sql = "";
    sql +=
        "SELECT order_id, site_id, price "
            + "FROM ORDER_DETAILS1 "
            + " INTERSECT ALL "
            + "SELECT order_id, site_id, price "
            + "FROM ORDER_DETAILS2 ";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows).satisfies(new CheckSize(2));

    PAssert.that(rows)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
                    Schema.FieldType.INT64, "order_id",
                    Schema.FieldType.INT32, "site_id",
                    Schema.FieldType.DECIMAL, "price")
                .addRows(1L, 1, new BigDecimal(1.0), 2L, 2, new BigDecimal(2.0))
                .getRows());

    pipeline.run();
  }

  @Test
  public void testNodeStatsEstimation() {
    String sql =
        "SELECT order_id, site_id, price "
            + " FROM ORDER_DETAILS1 "
            + " INTERSECT "
            + " SELECT order_id, site_id, price "
            + " FROM ORDER_DETAILS2 ";

    RelNode root = env.parseQuery(sql);

    while (!(root instanceof BeamIntersectRel)) {
      root = root.getInput(0);
    }

    NodeStats estimate = BeamSqlRelUtils.getNodeStats(root, root.getCluster().getMetadataQuery());

    Assert.assertFalse(estimate.isUnknown());
    Assert.assertEquals(0d, estimate.getRate(), 0.01);

    Assert.assertEquals(3. / 2., estimate.getRowCount(), 0.01);
    Assert.assertEquals(3. / 2., estimate.getWindow(), 0.01);
  }
}
