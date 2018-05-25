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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalCorrelate;

/** A {@code ConverterRule} to replace {@link Union} with {@link BeamUnnestRule}. */
public class BeamUnnestRule extends RelOptRule {
  public static final BeamUnnestRule INSTANCE = new BeamUnnestRule();

  // TODO: more general Correlate
  private BeamUnnestRule() {
    super(
        operand(
            LogicalCorrelate.class, operand(RelNode.class, any()), operand(Uncollect.class, any())),
        "BeamUnnestRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalCorrelate correlate = call.rel(0);
    RelNode outer = call.rel(1);
    RelNode uncollect = call.rel(2);

    call.transformTo(
        new BeamUnnestRel(
            correlate.getCluster(),
            correlate.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
            outer,
            convert(uncollect, uncollect.getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
            correlate.getCorrelationId(),
            correlate.getRequiredColumns(),
            correlate.getJoinType()));
  }
}
