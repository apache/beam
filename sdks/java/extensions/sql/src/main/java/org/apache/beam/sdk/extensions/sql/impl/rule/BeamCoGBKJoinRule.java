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
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamCalcRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamCoGBKJoinRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamJoinRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.Join;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.JoinRelType;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.core.RelFactories;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexBuilder;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexFieldAccess;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexProgramBuilder;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.fun.SqlStdOperatorTable;

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
    // Only a PURE cross join over two bounded inputs is handed off to BeamCrossJoinRule; everything
    // else (equi-joins and non-equi/theta joins, which still surface their original diagnostics)
    // stays on this rule.
    if (((Join) call.rel(0)).getCondition().isAlwaysTrue()
        && BeamJoinRel.getBoundednessOfRelNode(call.rel(1)) == PCollection.IsBounded.BOUNDED
        && BeamJoinRel.getBoundednessOfRelNode(call.rel(2)) == PCollection.IsBounded.BOUNDED) {
      return false;
    }
    PCollection.IsBounded boundednessOfLeftRel = BeamJoinRel.getBoundednessOfRelNode(call.rel(1));
    PCollection.IsBounded boundednessOfRightRel = BeamJoinRel.getBoundednessOfRelNode(call.rel(2));
    return (boundednessOfLeftRel == boundednessOfRightRel);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = (Join) call.rel(0);

    RexNode condition = join.getCondition();
    List<RexNode> conjuncts = new ArrayList<>();
    RelOptUtil.decomposeConjunction(condition, conjuncts);

    List<RexNode> equiConditions = new ArrayList<>();
    List<RexNode> residualConditions = new ArrayList<>();
    int leftFieldCount = join.getLeft().getRowType().getFieldCount();

    for (RexNode conjunct : conjuncts) {
      if (isEquiJoinConjunct(conjunct, leftFieldCount)) {
        equiConditions.add(conjunct);
      } else {
        residualConditions.add(conjunct);
      }
    }

    if (equiConditions.isEmpty()
        || (!residualConditions.isEmpty() && join.getJoinType() != JoinRelType.INNER)) {
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
              condition,
              join.getVariablesSet(),
              join.getJoinType());
      call.transformTo(rel);
      return;
    }

    RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    RexNode finalEquiCondition;
    if (equiConditions.size() == 1) {
      finalEquiCondition = equiConditions.get(0);
    } else {
      finalEquiCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, equiConditions);
    }

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
            finalEquiCondition,
            join.getVariablesSet(),
            join.getJoinType());

    if (residualConditions.isEmpty()) {
      call.transformTo(rel);
    } else {
      RexNode finalResidualCondition;
      if (residualConditions.size() == 1) {
        finalResidualCondition = residualConditions.get(0);
      } else {
        finalResidualCondition = rexBuilder.makeCall(SqlStdOperatorTable.AND, residualConditions);
      }
      RexProgramBuilder programBuilder = new RexProgramBuilder(rel.getRowType(), rexBuilder);
      programBuilder.addIdentity();
      programBuilder.addCondition(finalResidualCondition);
      BeamCalcRel calcRel =
          new BeamCalcRel(
              join.getCluster(),
              join.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
              rel,
              programBuilder.getProgram());
      call.transformTo(calcRel);
    }
  }

  private static int extractInputIndex(RexNode node) {
    if (node instanceof RexInputRef) {
      return ((RexInputRef) node).getIndex();
    }
    if (node instanceof RexFieldAccess) {
      return extractInputIndex(((RexFieldAccess) node).getReferenceExpr());
    }
    return -1;
  }

  private boolean isEquiJoinConjunct(RexNode conjunct, int leftFieldCount) {
    if (!(conjunct instanceof RexCall)) {
      return false;
    }
    RexCall call = (RexCall) conjunct;
    if (!call.getOperator().getName().equals("=")) {
      return false;
    }
    if (call.getOperands().size() != 2) {
      return false;
    }
    RexNode op0 = call.getOperands().get(0);
    RexNode op1 = call.getOperands().get(1);

    int idx0 = extractInputIndex(op0);
    int idx1 = extractInputIndex(op1);
    if (idx0 < 0 || idx1 < 0) {
      return false;
    }
    boolean op0Left = idx0 < leftFieldCount;
    boolean op1Left = idx1 < leftFieldCount;
    return op0Left != op1Left;
  }
}
