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

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamCoGBKJoinRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamJoinRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;

/**
 * Rule to convert {@code LogicalJoin} node to {@code BeamCoGBKJoinRel} node.
 *
 * <p>This rule is matched when both the inputs to {@code LogicalJoin} node have the same
 * Boundedness i.e. either when both the inputs are {@code PCollection.IsBounded.BOUNDED} or when
 * both the inputs are {@code PCollection.IsBounded.UNBOUNDED}
 *
 * <p>As {@code BeamSideInputLookupJoinRel} also matches this condition when both the inputs are
 * {@code PCollection.IsBounded.BOUNDED}, to avoid conflicts, this rule is not matched when any of
 * the inputs to {@code LogicalJoin} node are Seekable.
 */
public class BeamCoGBKJoinRule extends RelOptRule {
  public static final BeamCoGBKJoinRule INSTANCE = new BeamCoGBKJoinRule();

  private BeamCoGBKJoinRule() {
    super(
        operand(LogicalJoin.class, operand(RelNode.class, any()), operand(RelNode.class, any())),
        RelFactories.LOGICAL_BUILDER,
        "BeamCoGBKJoinRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    // The Rule does not match when any of the inputs are Seekable
    if (BeamJoinRel.containsSeekableInput(call.rel(0))) {
      return false;
    }
    PCollection.IsBounded boundednessOfLeftRel = BeamJoinRel.getBoundednessOfRelNode(call.rel(1));
    PCollection.IsBounded boundednessOfRightRel = BeamJoinRel.getBoundednessOfRelNode(call.rel(2));
    return (boundednessOfLeftRel == boundednessOfRightRel);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = (Join) call.rel(0);

    BeamCoGBKJoinRel rel =
        new BeamCoGBKJoinRel(
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
