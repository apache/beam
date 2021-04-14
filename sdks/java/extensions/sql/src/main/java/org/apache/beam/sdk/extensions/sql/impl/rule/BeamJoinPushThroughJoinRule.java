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

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamJoinRel;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.Join;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.RelFactories;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.tools.RelBuilderFactory;

/**
 * This is exactly similar to {@link
 * org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.rules.JoinPushThroughJoinRule}. It
 * only checks if the condition of the new bottom join is supported.
 */
public class BeamJoinPushThroughJoinRule extends JoinPushThroughJoinRule {
  /** Instance of the rule that works on logical joins only, and pushes to the right. */
  public static final RelOptRule RIGHT =
      new BeamJoinPushThroughJoinRule(
          "BeamJoinPushThroughJoinRule:right",
          true,
          LogicalJoin.class,
          RelFactories.LOGICAL_BUILDER);

  /** Instance of the rule that works on logical joins only, and pushes to the left. */
  public static final RelOptRule LEFT =
      new BeamJoinPushThroughJoinRule(
          "BeamJoinPushThroughJoinRule:left",
          false,
          LogicalJoin.class,
          RelFactories.LOGICAL_BUILDER);

  /** Creates a JoinPushThroughJoinRule. */
  private BeamJoinPushThroughJoinRule(
      String description,
      boolean right,
      Class<? extends Join> clazz,
      RelBuilderFactory relBuilderFactory) {
    super(description, right, clazz, relBuilderFactory);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    super.onMatch(
        new JoinRelOptRuleCall(
            call,
            rel -> {
              Join topJoin = (Join) rel.getInput(0);
              Join bottomJoin = (Join) ((Join) rel.getInput(0)).getLeft();
              return BeamJoinRel.isJoinLegal(topJoin) && BeamJoinRel.isJoinLegal(bottomJoin);
            }));
  }
}
