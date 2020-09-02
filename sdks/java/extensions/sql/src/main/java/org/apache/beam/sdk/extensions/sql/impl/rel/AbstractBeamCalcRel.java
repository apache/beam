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

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamCostModel;
import org.apache.beam.sdk.extensions.sql.impl.planner.NodeStats;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.Calc;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexLocalRef;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexProgram;

/** BeamRelNode to replace {@code Project} and {@code Filter} node. */
@Internal
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public abstract class AbstractBeamCalcRel extends Calc implements BeamRelNode {

  public AbstractBeamCalcRel(
      RelOptCluster cluster, RelTraitSet traits, RelNode input, RexProgram program) {
    super(cluster, traits, input, program);
  }

  public boolean isInputSortRelAndLimitOnly() {
    return (input instanceof BeamSortRel) && ((BeamSortRel) input).isLimitOnly();
  }

  public int getLimitCountOfSortRel() {
    if (input instanceof BeamSortRel) {
      return ((BeamSortRel) input).getCount();
    }

    throw new RuntimeException("Could not get the limit count from a non BeamSortRel input.");
  }

  @Override
  public NodeStats estimateNodeStats(RelMetadataQuery mq) {
    NodeStats inputStat = BeamSqlRelUtils.getNodeStats(input, mq);
    double selectivity = estimateFilterSelectivity(getInput(), program, mq);

    return inputStat.multiply(selectivity);
  }

  private static double estimateFilterSelectivity(
      RelNode child, RexProgram program, RelMetadataQuery mq) {
    // Similar to calcite, if the calc node is representing filter operation we estimate the filter
    // selectivity based on the number of equality conditions, number of inequality conditions, ....
    RexLocalRef programCondition = program.getCondition();
    RexNode condition;
    if (programCondition == null) {
      condition = null;
    } else {
      condition = program.expandLocalRef(programCondition);
    }
    // Currently this gets the selectivity based on Calcite's Selectivity Handler (RelMdSelectivity)
    return mq.getSelectivity(child, condition);
  }

  @Override
  public BeamCostModel beamComputeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    NodeStats inputStat = BeamSqlRelUtils.getNodeStats(this.input, mq);
    return BeamCostModel.FACTORY.makeCost(inputStat.getRowCount(), inputStat.getRate());
  }
}
