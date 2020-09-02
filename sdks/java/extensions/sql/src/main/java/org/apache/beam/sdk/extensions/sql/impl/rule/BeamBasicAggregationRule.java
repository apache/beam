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
package org.apache.beam.sdk.extensions.sql.impl.rule;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamAggregationRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.volcano.RelSubset;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.Aggregate;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.Calc;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.Filter;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.Project;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.RelFactories;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.tools.RelBuilderFactory;

/**
 * Aggregation rule that doesn't include projection.
 *
 * <p>Doesn't support windowing, as we extract window information from projection node.
 *
 * <p>{@link BeamAggregationRule} supports projection and windowing.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class BeamBasicAggregationRule extends RelOptRule {
  public static final BeamBasicAggregationRule INSTANCE =
      new BeamBasicAggregationRule(Aggregate.class, RelFactories.LOGICAL_BUILDER);

  public BeamBasicAggregationRule(
      Class<? extends Aggregate> aggregateClass, RelBuilderFactory relBuilderFactory) {
    super(operand(aggregateClass, operand(RelNode.class, any())), relBuilderFactory, null);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate aggregate = call.rel(0);
    RelNode relNode = call.rel(1);

    if (relNode instanceof Project || relNode instanceof Calc || relNode instanceof Filter) {
      if (isWindowed(relNode) || hasWindowedParents(relNode)) {
        // This case is expected to get handled by the 'BeamAggregationRule'
        return;
      }
    }

    RelNode newTableScan = relNode.copy(relNode.getTraitSet(), relNode.getInputs());

    call.transformTo(
        new BeamAggregationRel(
            aggregate.getCluster(),
            aggregate.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
            convert(
                newTableScan, newTableScan.getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggregate.getAggCallList(),
            null,
            -1));
  }

  private static boolean isWindowed(RelNode node) {
    List<RexNode> projects = null;

    if (node instanceof Project) {
      projects = new ArrayList<>(((Project) node).getProjects());
    } else if (node instanceof Calc) {
      projects =
          ((Calc) node)
              .getProgram().getProjectList().stream()
                  .map(
                      rexLocalRef ->
                          ((Calc) node).getProgram().getExprList().get(rexLocalRef.getIndex()))
                  .collect(Collectors.toList());
    }

    if (projects != null) {
      for (RexNode projNode : projects) {
        if (!(projNode instanceof RexCall)) {
          continue;
        }

        SqlKind sqlKind = ((RexCall) projNode).op.kind;
        if (sqlKind == SqlKind.SESSION || sqlKind == SqlKind.HOP || sqlKind == SqlKind.TUMBLE) {
          return true;
        }
      }
    }

    return false;
  }

  private static boolean hasWindowedParents(RelNode node) {
    List<RelNode> parents = new ArrayList<>();

    for (RelNode inputNode : node.getInputs()) {
      if (inputNode instanceof RelSubset) {
        parents.addAll(((RelSubset) inputNode).getParentRels());
        parents.addAll(((RelSubset) inputNode).getRelList());
      }
    }

    for (RelNode parent : parents) {
      if (isWindowed(parent)) {
        return true;
      }
    }

    return false;
  }
}
