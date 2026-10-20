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

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamCrossJoinRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamJoinRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.Join;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.RelFactories;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalJoin;

/**
 * Rule to convert a {@code LogicalJoin} with a cross / non-equi condition into a {@code
 * BeamCrossJoinRel}, but only when BOTH inputs are bounded. Equi-joins keep using the existing
 * CoGBK / side-input rules; those rules are gated with {@code isJoinLegal} so they no longer match
 * the cross / non-equi case. Unbounded cross joins are intentionally not matched (rejected).
 */
public class BeamCrossJoinRule extends RelOptRule {
  public static final BeamCrossJoinRule INSTANCE = new BeamCrossJoinRule();

  private BeamCrossJoinRule() {
    super(
        operand(LogicalJoin.class, operand(RelNode.class, any()), operand(RelNode.class, any())),
        RelFactories.LOGICAL_BUILDER,
        "BeamCrossJoinRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Join join = (Join) call.rel(0);
    // Only PURE cross joins (literal-true condition). Non-equi / theta joins (a real, non-equi
    // condition) are left to the CoGBK / side-input rules so they keep surfacing their original
    // "not supported" diagnostics rather than being silently rerouted here.
    if (!join.getCondition().isAlwaysTrue()) {
      return false;
    }
    if (BeamJoinRel.containsSeekableInput(call.rel(0))) {
      return false;
    }
    // Only bounded x bounded is supported.
    return BeamJoinRel.getBoundednessOfRelNode(call.rel(1)) == PCollection.IsBounded.BOUNDED
        && BeamJoinRel.getBoundednessOfRelNode(call.rel(2)) == PCollection.IsBounded.BOUNDED;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = (Join) call.rel(0);
    BeamCrossJoinRel rel =
        new BeamCrossJoinRel(
            join.getCluster(),
            join.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
            convert(
                join.getLeft(),
                join.getLeft().getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
            convert(
                join.getRight(),
                join.getRight().getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
            join.getCondition(),
            join.getVariablesSet(),
            join.getJoinType());
    call.transformTo(rel);
  }
}
