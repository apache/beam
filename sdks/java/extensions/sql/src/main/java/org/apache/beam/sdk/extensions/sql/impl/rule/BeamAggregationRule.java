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
import java.util.Optional;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamAggregationRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * Rule to detect the window/trigger settings.
 *
 */
public class BeamAggregationRule extends RelOptRule {
  private final BeamSqlEnv sqlEnv;

  public static BeamAggregationRule forSqlEnv(BeamSqlEnv sqlEnv) {
    return new BeamAggregationRule(Aggregate.class, Project.class, RelFactories.LOGICAL_BUILDER,
        sqlEnv);
  }

  public BeamAggregationRule(
      Class<? extends Aggregate> aggregateClass,
      Class<? extends Project> projectClass,
      RelBuilderFactory relBuilderFactory,
      BeamSqlEnv sqlEnv) {
    super(
        operand(aggregateClass,
            operand(projectClass, any())),
        relBuilderFactory, null);
    this.sqlEnv = sqlEnv;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);
    updateWindow(call, aggregate, project);
  }

  private void updateWindow(RelOptRuleCall call, Aggregate aggregate,
                            Project project) {
    ImmutableBitSet groupByFields = aggregate.getGroupSet();
    List<RexNode> projectMapping = project.getProjects();

    Optional<AggregateWindowField> windowField = Optional.empty();

    for (int groupFieldIndex : groupByFields.asList()) {
      RexNode projNode = projectMapping.get(groupFieldIndex);
      if (!(projNode instanceof RexCall)) {
        continue;
      }

      windowField = AggregateWindowFactory.getWindowFieldAt((RexCall) projNode, groupFieldIndex);
    }

    BeamAggregationRel newAggregator = new BeamAggregationRel(aggregate.getCluster(),
        aggregate.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
        convert(aggregate.getInput(),
            aggregate.getInput().getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        aggregate.indicator,
        aggregate.getGroupSet(),
        aggregate.getGroupSets(),
        aggregate.getAggCallList(),
        windowField,
        sqlEnv);
    call.transformTo(newAggregator);
  }

}
