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
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestUnboundedTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

/** Test for {@code BeamIOSourceRel}. */
public class BeamIOSourceRelTest extends BaseRelTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  private static final DateTime FIRST_DATE = new DateTime(1);
  private static final DateTime SECOND_DATE = new DateTime(1 + 3600 * 1000);

  private static final Duration WINDOW_SIZE = Duration.standardHours(1);

  @BeforeClass
  public static void prepare() {
    registerTable(
        "ORDER_DETAILS_BOUNDED",
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
                new BigDecimal(4.0),
                4L,
                4,
                new BigDecimal(4.0)));

    registerTable(
        "ORDER_DETAILS_UNBOUNDED",
        TestUnboundedTable.of(
                Schema.FieldType.INT32, "order_id",
                Schema.FieldType.INT32, "site_id",
                Schema.FieldType.INT32, "price",
                Schema.FieldType.DATETIME, "order_time")
            .timestampColumnIndex(3)
            .addRows(Duration.ZERO, 1, 1, 1, FIRST_DATE, 1, 2, 6, FIRST_DATE)
            .addRows(
                WINDOW_SIZE.plus(Duration.standardMinutes(1)),
                2,
                2,
                7,
                SECOND_DATE,
                2,
                3,
                8,
                SECOND_DATE,
                // this late record is omitted(First window)
                1,
                3,
                3,
                FIRST_DATE)
            .addRows(
                // this late record is omitted(Second window)
                WINDOW_SIZE.plus(WINDOW_SIZE).plus(Duration.standardMinutes(1)),
                2,
                3,
                3,
                SECOND_DATE)
            .setStatistics(BeamTableStatistics.createUnboundedTableStatistics(2d)));
  }

  @Test
  public void boundedRowCount() {
    String sql = "SELECT * FROM ORDER_DETAILS_BOUNDED";

    RelNode root = env.parseQuery(sql);

    while (!(root instanceof BeamIOSourceRel)) {
      root = root.getInput(0);
    }

    Assert.assertEquals(5d, root.estimateRowCount(RelMetadataQuery.instance()), 0.001);
  }

  @Test
  public void unboundedRowCount() {
    String sql = "SELECT * FROM ORDER_DETAILS_UNBOUNDED";

    RelNode root = env.parseQuery(sql);

    while (!(root instanceof BeamIOSourceRel)) {
      root = root.getInput(0);
    }

    Assert.assertEquals(2d, root.estimateRowCount(RelMetadataQuery.instance()), 0.001);
  }

  @Test
  public void testBoundedNodeStats() {
    String sql = "SELECT * FROM ORDER_DETAILS_BOUNDED";

    RelNode root = env.parseQuery(sql);

    while (!(root instanceof BeamIOSourceRel)) {
      root = root.getInput(0);
    }

    NodeStats estimate = BeamSqlRelUtils.getNodeStats(root, root.getCluster().getMetadataQuery());

    Assert.assertEquals(5d, estimate.getRowCount(), 0.01);
    Assert.assertEquals(0d, estimate.getRate(), 0.01);
    Assert.assertEquals(5d, estimate.getWindow(), 0.01);
  }

  @Test
  public void testUnboundedNodeStats() {
    String sql = "SELECT * FROM ORDER_DETAILS_UNBOUNDED";

    RelNode root = env.parseQuery(sql);

    while (!(root instanceof BeamIOSourceRel)) {
      root = root.getInput(0);
    }

    NodeStats estimate = BeamSqlRelUtils.getNodeStats(root, root.getCluster().getMetadataQuery());

    Assert.assertEquals(0d, estimate.getRowCount(), 0.01);
    Assert.assertEquals(2d, estimate.getRate(), 0.01);
    Assert.assertEquals(BeamIOSourceRel.CONSTANT_WINDOW_SIZE, estimate.getWindow(), 0.01);
  }
}
