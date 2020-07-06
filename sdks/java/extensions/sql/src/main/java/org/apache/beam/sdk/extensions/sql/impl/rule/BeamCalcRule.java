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

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamCalcRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.Convention;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.convert.ConverterRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Calc;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexOver;

/** A {@code ConverterRule} to replace {@link Calc} with {@link BeamCalcRel}. */
public class BeamCalcRule extends ConverterRule {
  public static final BeamCalcRule INSTANCE = new BeamCalcRule();

  private BeamCalcRule() {
    super(LogicalCalc.class, Convention.NONE, BeamLogicalConvention.INSTANCE, "BeamCalcRule");
  }

  @Override
  public boolean matches(RelOptRuleCall x) {
    /**
     * The Analytic Functions (a.k.a. window functions) match with both Calc and Window rules. So,
     * it is necessary to skip the Calc rule in order to execute the more suitable conversion
     * (BeamWindowRule).
     */
    boolean hasRexOver = false;
    List<RelNode> resList = x.getRelList();
    for (RelNode relNode : resList) {
      if (relNode instanceof LogicalCalc) {
        LogicalCalc logicalCalc = (LogicalCalc) relNode;
        for (RexNode rexNode : logicalCalc.getProgram().getExprList()) {
          if (rexNode instanceof RexOver) {
            hasRexOver = true;
            break;
          }
        }
      }
    }
    return !hasRexOver;
  }

  @Override
  public RelNode convert(RelNode rel) {
    final Calc calc = (Calc) rel;
    final RelNode input = calc.getInput();

    return new BeamCalcRel(
        calc.getCluster(),
        calc.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
        RelOptRule.convert(input, input.getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        calc.getProgram());
  }
}
