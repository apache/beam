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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.volcano.RelSubset;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.SingleRel;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.Aggregate;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.Filter;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.Project;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.RelFactories;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.tools.RelBuilderFactory;

/**
 * This rule is essentially a wrapper around Calcite's {@code AggregateProjectMergeRule}. In the
 * case when an underlying IO supports project push-down it is more efficient to not merge {@code
 * Project} with an {@code Aggregate}, leaving it for the {@code BeamIOPUshDownRule}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class BeamAggregateProjectMergeRule extends AggregateProjectMergeRule {
  public static final AggregateProjectMergeRule INSTANCE =
      new BeamAggregateProjectMergeRule(
          Aggregate.class, Project.class, RelFactories.LOGICAL_BUILDER);

  public BeamAggregateProjectMergeRule(
      Class<? extends Aggregate> aggregateClass,
      Class<? extends Project> projectClass,
      RelBuilderFactory relBuilderFactory) {
    super(aggregateClass, projectClass, relBuilderFactory);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(1);
    BeamIOSourceRel io = getUnderlyingIO(new HashSet<>(), project);

    // Only perform AggregateProjectMergeRule when IO is not present or project push-down is not
    // supported.
    if (io == null || !io.getBeamSqlTable().supportsProjects().isSupported()) {
      super.onMatch(call);
    }
  }

  /**
   * Following scenarios are possible:<br>
   * 1) Aggregate <- Project <- IO.<br>
   * 2) Aggregate <- Project <- Chain of Project/Filter <- IO.<br>
   * 3) Aggregate <- Project <- Something else.<br>
   * 4) Aggregate <- Project <- Chain of Project/Filter <- Something else.
   *
   * @param parent project that matched this rule.
   * @return {@code BeamIOSourceRel} when it is present or null when some other {@code RelNode} is
   *     present.
   */
  private BeamIOSourceRel getUnderlyingIO(Set<RelNode> visitedNodes, SingleRel parent) {
    // No need to look at the same node more than once.
    if (visitedNodes.contains(parent)) {
      return null;
    }
    visitedNodes.add(parent);
    List<RelNode> nodes = ((RelSubset) parent.getInput()).getRelList();

    for (RelNode node : nodes) {
      if (node instanceof Filter || node instanceof Project) {
        // Search node inputs for an IO.
        BeamIOSourceRel child = getUnderlyingIO(visitedNodes, (SingleRel) node);
        if (child != null) {
          return child;
        }
      } else if (node instanceof BeamIOSourceRel) {
        return (BeamIOSourceRel) node;
      }
    }

    return null;
  }
}
