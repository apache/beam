package org.apache.beam.sdk.extensions.sql.impl.rule;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.volcano.RelSubset;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.SingleRel;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Aggregate;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Filter;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Project;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.RelFactories;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RelBuilderFactory;

/**
 * This rule is essentially a wrapper around Calcite's {@code AggregateProjectMergeRule}. In the
 * case when an underlying IO supports project push-down it is more efficient to not merge
 * {@code Project} with an {@code Aggregate}, leaving it for the {@code BeamIOPUshDownRule}.
 */
public class BeamAggregateProjectMergeRule extends AggregateProjectMergeRule {
  public static final AggregateProjectMergeRule INSTANCE = new BeamAggregateProjectMergeRule(Aggregate.class, Project.class, RelFactories.LOGICAL_BUILDER);
  private Set<RelNode> visitedNodes = new HashSet<>();

  public BeamAggregateProjectMergeRule(
      Class<? extends Aggregate> aggregateClass,
      Class<? extends Project> projectClass,
      RelBuilderFactory relBuilderFactory) {
    super(aggregateClass, projectClass, relBuilderFactory);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(1);
    BeamIOSourceRel io = getUnderlyingIO(project);

    // Only perform AggregateProjectMergeRule when IO is not present or project push-down is not supported.
    if (io == null || !io.getBeamSqlTable().supportsProjects().isSupported()) {
      super.onMatch(call);
    }
  }

  /**
   * Following scenarios are possible:
   * 1) Aggregate <- Project <- IO.
   * 2) Aggregate <- Project <- Chain of Project/Filter <- IO.
   * 3) Aggregate <- Project <- Something else.
   * 4) Aggregate <- Project <- Chain of Project/Filter <- Something else.
   * @param parent project that matched this rule.
   * @return {@code BeamIOSourceRel} when it is present or null when some other {@code RelNode} is present.
   */
  private BeamIOSourceRel getUnderlyingIO(SingleRel parent) {
    // No need to look at the same node more than once.
    if (visitedNodes.contains(parent)) {
      return null;
    }
    visitedNodes.add(parent);
    List<RelNode> nodes = ((RelSubset) parent.getInput()).getRelList();

    for (RelNode node : nodes) {
      if (node instanceof Filter || node instanceof Project) {
        return getUnderlyingIO((SingleRel) node);
      } else if (node instanceof BeamIOSourceRel) {
        return (BeamIOSourceRel) node;
      }
    }

    return null;
  }
}
