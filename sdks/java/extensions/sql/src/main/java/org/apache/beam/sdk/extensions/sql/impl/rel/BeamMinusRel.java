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
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.Minus;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.SetOp;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.metadata.RelMetadataQuery;

/**
 * {@code BeamRelNode} to replace a {@code Minus} node.
 *
 * <p>Corresponds to the SQL {@code EXCEPT} operator.
 */
public class BeamMinusRel extends Minus implements BeamRelNode {

  public BeamMinusRel(
      RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
    super(cluster, traits, inputs, all);
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    NodeStats inputsEstimatesSummation =
        inputs.stream()
            .map(input -> BeamSqlRelUtils.getNodeStats(input, mq))
            .reduce(NodeStats.create(0, 0, 0), NodeStats::plus);

    return BeamCostModel.FACTORY.makeCost(
        inputsEstimatesSummation.getRowCount(), inputsEstimatesSummation.getRate());
  }

  @Override
  public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
    return new BeamMinusRel(getCluster(), traitSet, inputs, all);
  }

  @Override
  public PTransform<PCollectionList<Row>, PCollection<Row>> buildPTransform() {
    return new BeamSetOperatorRelBase(this, BeamSetOperatorRelBase.OpType.MINUS, all);
  }

  @Override
  public NodeStats estimateNodeStats(RelMetadataQuery mq) {
    NodeStats firstInputEstimates = BeamSqlRelUtils.getNodeStats(inputs.get(0), mq);
    // The first input minus half of the others. (We are assuming half of them have intersection)
    for (int i = 1; i < inputs.size(); i++) {
      NodeStats inputEstimate = BeamSqlRelUtils.getNodeStats(inputs.get(i), mq);
      firstInputEstimates = firstInputEstimates.minus(inputEstimate.multiply(0.5));
    }
    return firstInputEstimates;
  }
}
