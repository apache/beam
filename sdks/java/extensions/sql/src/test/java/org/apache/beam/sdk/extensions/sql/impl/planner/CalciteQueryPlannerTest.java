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
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the behavior of {@code CalciteQueryPlanner}. Note that this is not for the JDBC path. It
 * will be the behavior of SQLTransform path.
 */
public class CalciteQueryPlannerTest extends BaseRelTest {
  @Before
  public void prepare() {
    registerTable(
        "medium_table",
        TestBoundedTable.of(
                Schema.FieldType.INT32, "unbounded_key",
                Schema.FieldType.INT32, "large_key",
                Schema.FieldType.INT32, "id")
            .addRows(1, 1, 1, 1, 1, 2, 1, 1, 3, 1, 1, 4, 1, 1, 5));
  }

  @Test
  public void testclusterCostHandlerUsesBeamCost() {
    String sql = "select * from medium_table";
    BeamRelNode root = env.parseQuery(sql);
    Assert.assertTrue(
        root.getCluster().getPlanner().getCost(root, root.getCluster().getMetadataQuery())
            instanceof BeamCostModel);
  }

  @Test
  public void testNonCumulativeCostMetadataHandler() {
    String sql = "select * from medium_table";
    BeamRelNode root = env.parseQuery(sql);
    Assert.assertTrue(
        root.getCluster().getMetadataQuery().getNonCumulativeCost(root) instanceof BeamCostModel);
    Assert.assertFalse(
        root.getCluster().getMetadataQuery().getNonCumulativeCost(root).isInfinite());
  }

  @Test
  public void testCumulativeCostMetaDataHandler() {
    // This handler is not our handler. It tests if the cumulative handler of Calcite works as
    // expected.
    String sql = "select * from medium_table";
    BeamRelNode root = env.parseQuery(sql);
    Assert.assertTrue(
        root.getCluster().getMetadataQuery().getCumulativeCost(root) instanceof BeamCostModel);
    Assert.assertFalse(root.getCluster().getMetadataQuery().getCumulativeCost(root).isInfinite());
  }
}
