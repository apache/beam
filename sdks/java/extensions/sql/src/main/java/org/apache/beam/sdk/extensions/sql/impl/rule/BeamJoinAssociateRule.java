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
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.Join;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.RelFactories;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.rules.JoinAssociateRule;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.tools.RelBuilderFactory;

/**
 * This is very similar to {@link
 * org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.rules.JoinAssociateRule}. It only
 * checks if the resulting condition is supported before transforming.
 */
public class BeamJoinAssociateRule extends JoinAssociateRule {

  public static final org.apache.beam.sdk.extensions.sql.impl.rule.BeamJoinAssociateRule INSTANCE =
      new org.apache.beam.sdk.extensions.sql.impl.rule.BeamJoinAssociateRule(
          RelFactories.LOGICAL_BUILDER);

  private BeamJoinAssociateRule(RelBuilderFactory relBuilderFactory) {
    super(relBuilderFactory);
  }

  @Override
  public void onMatch(final RelOptRuleCall call) {
    super.onMatch(
        new JoinRelOptRuleCall(
            call,
            rel -> {
              Join topJoin = (Join) rel;
              Join bottomJoin = (Join) ((Join) rel).getRight();
              return BeamJoinRel.isJoinLegal(topJoin) && BeamJoinRel.isJoinLegal(bottomJoin);
            }));
  }
}
