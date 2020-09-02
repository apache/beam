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
package org.apache.beam.sdk.extensions.sql.impl.planner;

import org.apache.beam.sdk.extensions.sql.impl.rel.BaseRelTest;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.volcano.RelSubset;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.SingleRel;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** This tests the NodeStats Metadata handler and the estimations. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class NodeStatsTest extends BaseRelTest {
  static class UnknownRel extends SingleRel {
    protected UnknownRel(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
      super(cluster, traits, input);
    }
  }

  public static final TestBoundedTable ORDER_DETAILS1 =
      TestBoundedTable.of(
              Schema.FieldType.INT32, "order_id",
              Schema.FieldType.INT32, "site_id",
              Schema.FieldType.INT32, "price")
          .addRows(1, 2, 3, 2, 3, 3, 3, 4, 5);

  public static final TestBoundedTable ORDER_DETAILS2 =
      TestBoundedTable.of(
              Schema.FieldType.INT32, "order_id",
              Schema.FieldType.INT32, "site_id",
              Schema.FieldType.INT32, "price")
          .addRows(1, 2, 3, 2, 3, 3, 3, 4, 5);

  @BeforeClass
  public static void prepare() {
    registerTable("ORDER_DETAILS1", ORDER_DETAILS1);
    registerTable("ORDER_DETAILS2", ORDER_DETAILS2);
  }

  @Test
  public void testUnknownRel() {
    String sql = " select * from ORDER_DETAILS1 ";
    RelNode root = env.parseQuery(sql);
    RelNode unknown = new UnknownRel(root.getCluster(), null, null);
    NodeStats nodeStats =
        unknown
            .metadata(NodeStatsMetadata.class, unknown.getCluster().getMetadataQuery())
            .getNodeStats();
    Assert.assertTrue(nodeStats.isUnknown());
  }

  @Test
  public void testKnownRel() {
    String sql = " select * from ORDER_DETAILS1 ";
    RelNode root = env.parseQuery(sql);
    NodeStats nodeStats =
        root.metadata(NodeStatsMetadata.class, root.getCluster().getMetadataQuery()).getNodeStats();
    Assert.assertFalse(nodeStats.isUnknown());
  }

  @Test
  public void testSubsetHavingBest() {
    String sql = " select * from ORDER_DETAILS1 ";
    RelNode root = env.parseQuery(sql);
    root = root.getCluster().getPlanner().getRoot();

    // tests if we are actually testing what we want.
    Assert.assertTrue(root instanceof RelSubset);

    NodeStats estimates = BeamSqlRelUtils.getNodeStats(root, root.getCluster().getMetadataQuery());
    Assert.assertFalse(estimates.isUnknown());
  }
}
