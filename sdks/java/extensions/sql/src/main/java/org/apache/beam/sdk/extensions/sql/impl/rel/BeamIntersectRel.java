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

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.Intersect;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.SetOp;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * {@code BeamRelNode} to replace a {@code Intersect} node.
 *
 * <p>This is used to combine two SELECT statements, but returns rows only from the first SELECT
 * statement that are identical to a row in the second SELECT statement.
 */
public class BeamIntersectRel extends Intersect implements BeamRelNode {
  public BeamIntersectRel(
      RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
    super(cluster, traits, inputs, all);
  }

  @Override
  public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    return new BeamIntersectRel(getCluster(), traitSet, inputs, all);
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new BeamSetOperatorRelBase(this, BeamSetOperatorRelBase.OpType.INTERSECT, all);
  }

  @Override
  public NodeStats estimateNodeStats(RelMetadataQuery mq) {
    // This takes the minimum of the inputs for all the estimate factors.
    double minimumRows = Double.POSITIVE_INFINITY;
    double minimumWindowSize = Double.POSITIVE_INFINITY;
    double minimumRate = Double.POSITIVE_INFINITY;

    for (RelNode input : inputs) {
      NodeStats inputEstimates = BeamSqlRelUtils.getNodeStats(input, mq);
      minimumRows = Math.min(minimumRows, inputEstimates.getRowCount());
      minimumRate = Math.min(minimumRate, inputEstimates.getRate());
      minimumWindowSize = Math.min(minimumWindowSize, inputEstimates.getWindow());
    }

    return NodeStats.create(minimumRows, minimumRate, minimumWindowSize).multiply(0.5);
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {

    NodeStats inputsStatSummation =
        inputs.stream()
            .map(input -> BeamSqlRelUtils.getNodeStats(input, mq))
            .reduce(NodeStats.create(0, 0, 0), NodeStats::plus);

    return BeamCostModel.FACTORY.makeCost(
        inputsStatSummation.getRowCount(), inputsStatSummation.getRate());
  }
}
