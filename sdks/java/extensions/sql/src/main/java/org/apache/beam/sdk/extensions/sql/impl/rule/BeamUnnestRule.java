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

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamUnnestRel;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.volcano.RelSubset;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.SingleRel;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Correlate;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.JoinRelType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Uncollect;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.logical.LogicalProject;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexFieldAccess;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * A {@code ConverterRule} to replace {@link Correlate} {@link Uncollect} with {@link
 * BeamUnnestRule}.
 */
public class BeamUnnestRule extends RelOptRule {
  public static final BeamUnnestRule INSTANCE = new BeamUnnestRule();

  // TODO: more general Correlate
  private BeamUnnestRule() {
    super(
        operand(
            LogicalCorrelate.class, operand(RelNode.class, any()), operand(SingleRel.class, any())),
        "BeamUnnestRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalCorrelate correlate = call.rel(0);
    RelNode outer = call.rel(1);
    RelNode uncollect = call.rel(2);

    if (correlate.getCorrelationId().getId() != 0) {
      // Only one level of correlation nesting is supported
      return;
    }
    if (correlate.getRequiredColumns().cardinality() != 1) {
      // can only unnest a single column
      return;
    }
    if (correlate.getJoinType() != JoinRelType.INNER) {
      return;
    }

    if (!(uncollect instanceof Uncollect)) {
      // Drop projection
      uncollect = ((SingleRel) uncollect).getInput();
      if (uncollect instanceof RelSubset) {
        uncollect = ((RelSubset) uncollect).getOriginal();
      }
      if (!(uncollect instanceof Uncollect)) {
        return;
      }
    }

    RelNode project = ((Uncollect) uncollect).getInput();
    if (project instanceof RelSubset) {
      project = ((RelSubset) project).getOriginal();
    }
    if (!(project instanceof LogicalProject)) {
      return;
    }

    if (((LogicalProject) project).getProjects().size() != 1) {
      // can only unnest a single column
      return;
    }

    RexNode exp = ((LogicalProject) project).getProjects().get(0);
    if (!(exp instanceof RexFieldAccess)) {
      return;
    }
    RexFieldAccess fieldAccess = (RexFieldAccess) exp;
    // Innermost field index comes first (e.g. struct.field1.field2 => [2, 1])
    ImmutableList.Builder<Integer> fieldAccessIndices = ImmutableList.builder();
    while (true) {
      fieldAccessIndices.add(fieldAccess.getField().getIndex());
      if (!(fieldAccess.getReferenceExpr() instanceof RexFieldAccess)) {
        break;
      }
      fieldAccess = (RexFieldAccess) fieldAccess.getReferenceExpr();
    }

    call.transformTo(
        new BeamUnnestRel(
            correlate.getCluster(),
            correlate.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
            outer,
            call.rel(2).getRowType(),
            fieldAccessIndices.build()));
  }
}
