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
package org.apache.beam.sdk.extensions.sql.zetasql;

import org.apache.beam.sdk.extensions.sql.impl.CalcRelSplitter;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamCalcRel;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Calc;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.RelFactories;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RelBuilder;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RelBuilderFactory;

/**
 * {@link RelOptRule} that converts a {@link LogicalCalc} to a chain of {@link BeamZetaSqlCalcRel}
 * and/or {@link BeamCalcRel} via {@link CalcRelSplitter}.
 *
 * <p>Only Java UDFs are implemented using {@link BeamCalcRel}. All other expressions are
 * implemented using {@link BeamZetaSqlCalcRel}.
 */
public class BeamCalcSplittingRule extends RelOptRule {
  public static final BeamCalcSplittingRule INSTANCE =
      new BeamCalcSplittingRule(RelFactories.LOGICAL_BUILDER);

  private BeamCalcSplittingRule(RelBuilderFactory relBuilderFactory) {
    super(operand(LogicalCalc.class, any()), relBuilderFactory, "BeamCalcSplittingRule");
  }

  @Override
  public boolean matches(RelOptRuleCall x) {
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
    final Calc calc = (Calc) relOptRuleCall.rel(0);
    final BeamCalcSplitter transform = new BeamCalcSplitter(calc, relOptRuleCall.builder());
    RelNode newRel = transform.execute();
    relOptRuleCall.transformTo(newRel);
  }

  static class BeamCalcSplitter extends CalcRelSplitter {
    // Order matters here. Putting BeamZetaSqlRelType first means it will be used for all
    // expressions except Java UDFs.
    private static final RelType[] REL_TYPES = {
      new BeamZetaSqlRelType("BeamZetaSqlRelType"), new BeamCalcRelType("BeamCalcRelType")
    };

    BeamCalcSplitter(Calc calc, RelBuilder relBuilder) {
      super(calc, relBuilder, REL_TYPES);
    }
  }
}
