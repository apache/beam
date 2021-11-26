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

import org.apache.beam.sdk.extensions.sql.impl.rel.CalcRelSplitter;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.core.Calc;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.core.RelFactories;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RelOptRule} that converts a {@link LogicalCalc} into a chain of {@link
 * org.apache.beam.sdk.extensions.sql.impl.rel.AbstractBeamCalcRel} nodes via {@link
 * CalcRelSplitter}.
 */
public abstract class BeamCalcSplittingRule extends RelOptRule {
  private static final Logger LOG = LoggerFactory.getLogger(BeamCalcSplittingRule.class);

  protected BeamCalcSplittingRule(String description) {
    super(operand(LogicalCalc.class, any()), RelFactories.LOGICAL_BUILDER, description);
  }

  @Override
  public boolean matches(RelOptRuleCall x) {
    CalcRelSplitter.RelType[] relTypes = getRelTypes();
    for (RelNode relNode : x.getRelList()) {
      if (relNode instanceof LogicalCalc) {
        LogicalCalc logicalCalc = (LogicalCalc) relNode;
        for (RexNode rexNode : logicalCalc.getProgram().getExprList()) {
          if (!relTypes[0].canImplement(rexNode, false)
              && !relTypes[1].canImplement(rexNode, false)) {
            LOG.error("Cannot implement expression {} with rule {}.", rexNode, this.description);
            return false;
          }
        }
      }
    }
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
    final Calc calc = (Calc) relOptRuleCall.rel(0);
    final CalcRelSplitter transform =
        new CalcRelSplitter(calc, relOptRuleCall.builder(), getRelTypes());
    RelNode newRel = transform.execute();
    relOptRuleCall.transformTo(newRel);
  }

  protected abstract CalcRelSplitter.RelType[] getRelTypes();
}
