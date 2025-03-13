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

import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.rules.CoreRules;

/**
 * Planner rule to merge a {@link BeamZetaSqlCalcRel} with a {@link BeamZetaSqlCalcRel}. Subset of
 * {@link CalcMergeRule}.
 */
public class BeamZetaSqlCalcMergeRule extends RelOptRule {
  public static final BeamZetaSqlCalcMergeRule INSTANCE = new BeamZetaSqlCalcMergeRule();

  public BeamZetaSqlCalcMergeRule() {
    super(
        operand(
            BeamZetaSqlCalcRel.class,
            operand(BeamZetaSqlCalcRel.class, any()),
            new RelOptRuleOperand[0]));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    CoreRules.CALC_MERGE.onMatch(call);
  }
}
