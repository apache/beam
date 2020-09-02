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

/** Test for {@code BeamUnionRel}. */
public class BeamUnionRelTest extends BaseRelTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void prepare() {
    registerTable(
        "ORDER_DETAILS",
        TestBoundedTable.of(
                Schema.FieldType.INT64, "order_id",
                Schema.FieldType.INT32, "site_id",
                Schema.FieldType.DECIMAL, "price")
            .addRows(1L, 1, new BigDecimal(1.0), 2L, 2, new BigDecimal(2.0)));
  }

  @Test
  public void testUnion() throws Exception {
    String sql =
        "SELECT "
            + " order_id, site_id, price "
            + "FROM ORDER_DETAILS "
            + " UNION SELECT "
            + " order_id, site_id, price "
            + "FROM ORDER_DETAILS ";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
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
  public void testUnionAll() throws Exception {
    String sql =
        "SELECT "
            + " order_id, site_id, price "
            + "FROM ORDER_DETAILS"
            + " UNION ALL "
            + " SELECT order_id, site_id, price "
            + "FROM ORDER_DETAILS";

    PCollection<Row> rows = compilePipeline(sql, pipeline);
    PAssert.that(rows)
        .containsInAnyOrder(
            TestUtils.RowsBuilder.of(
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
                    2L,
                    2,
                    new BigDecimal(2.0))
                .getRows());
    pipeline.run();
  }

  @Test
  public void testNodeStatsEstimation() {
    String sql =
        "SELECT "
            + " order_id, site_id, price "
            + "FROM ORDER_DETAILS "
            + " UNION SELECT "
            + " order_id, site_id, price "
            + "FROM ORDER_DETAILS ";

    RelNode root = env.parseQuery(sql);

    while (!(root instanceof BeamUnionRel)) {
      root = root.getInput(0);
    }

    NodeStats estimate = BeamSqlRelUtils.getNodeStats(root, root.getCluster().getMetadataQuery());

    Assert.assertFalse(estimate.isUnknown());
    Assert.assertEquals(0d, estimate.getRate(), 0.01);

    Assert.assertEquals(2., estimate.getRowCount(), 0.01);
    Assert.assertEquals(2., estimate.getWindow(), 0.01);
  }

  @Test
  public void testNodeStatsEstimationUnionAll() {
    String sql =
        "SELECT "
            + " order_id, site_id, price "
            + "FROM ORDER_DETAILS "
            + " UNION ALL SELECT "
            + " order_id, site_id, price "
            + "FROM ORDER_DETAILS ";

    RelNode root = env.parseQuery(sql);

    while (!(root instanceof BeamUnionRel)) {
      root = root.getInput(0);
    }

    NodeStats estimate = BeamSqlRelUtils.getNodeStats(root, root.getCluster().getMetadataQuery());

    Assert.assertFalse(estimate.isUnknown());
    Assert.assertEquals(0d, estimate.getRate(), 0.01);

    Assert.assertEquals(4., estimate.getRowCount(), 0.01);
    Assert.assertEquals(4., estimate.getWindow(), 0.01);
  }
}
