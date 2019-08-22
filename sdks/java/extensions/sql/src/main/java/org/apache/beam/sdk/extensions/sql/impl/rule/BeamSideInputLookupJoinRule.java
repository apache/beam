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
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSideInputLookupJoinRel;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalJoin;

/**
 * Rule to convert {@code LogicalJoin} node to {@code BeamSideInputLookupJoinRel} node.
 *
 * <p>This rule is matched when any of the inputs to {@code LogicalJoin} node are Seekable
 */
public class BeamSideInputLookupJoinRule extends ConverterRule {
  public static final BeamSideInputLookupJoinRule INSTANCE = new BeamSideInputLookupJoinRule();

  public BeamSideInputLookupJoinRule() {
    super(
        LogicalJoin.class,
        Convention.NONE,
        BeamLogicalConvention.INSTANCE,
        "BeamSideInputLookupJoinRule");
  }

  // The Rule is Matched when any of the inputs are Seekable
  @Override
  public boolean matches(RelOptRuleCall call) {
    RelNode joinRel = call.rel(0);
    boolean matches = BeamJoinRel.containsSeekableInput(joinRel);
    return (matches);
  }

  @Override
  public RelNode convert(RelNode rel) {
    Join join = (Join) rel;

    return (new BeamSideInputLookupJoinRel(
        join.getCluster(),
        join.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
        convert(
            join.getLeft(), join.getLeft().getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        convert(
            join.getRight(), join.getRight().getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        join.getCondition(),
        join.getVariablesSet(),
        join.getJoinType()));
  }
}
