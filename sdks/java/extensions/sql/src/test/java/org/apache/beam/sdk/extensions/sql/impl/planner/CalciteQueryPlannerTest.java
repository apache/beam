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

import org.apache.beam.sdk.extensions.sql.impl.QueryPlanner.QueryParameters;
import org.apache.beam.sdk.extensions.sql.impl.rel.BaseRelTest;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
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

  @Test
  public void testParseAndConvertHelpers() throws Exception {
    String sql = "select * from medium_table";
    RelNode logicalPlan = env.parseLogicalPlan(sql);
    Assert.assertNotNull(logicalPlan);

    BeamRelNode physicalPlan = env.convertToBeamRel(logicalPlan);
    Assert.assertNotNull(physicalPlan);
    Assert.assertTrue(
        physicalPlan
                .getCluster()
                .getPlanner()
                .getCost(physicalPlan, physicalPlan.getCluster().getMetadataQuery())
            instanceof BeamCostModel);
  }

  @Test
  public void testParseAndConvertHelpersWithParameters() throws Exception {
    String sql = "select * from medium_table where id = ?";
    RelNode logicalPlan = env.parseLogicalPlan(sql);
    Assert.assertNotNull(logicalPlan);

    QueryParameters params = QueryParameters.ofPositional(ImmutableList.of(3));
    BeamRelNode physicalPlan = env.convertToBeamRel(logicalPlan, params);
    Assert.assertNotNull(physicalPlan);

    String explained = BeamSqlRelUtils.explainLazily(physicalPlan).toString();
    Assert.assertTrue(explained.contains("3"));
  }
}
