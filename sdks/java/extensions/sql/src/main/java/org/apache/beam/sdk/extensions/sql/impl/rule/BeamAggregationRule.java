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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamAggregationRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.Aggregate;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.Project;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.RelFactories;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.tools.RelBuilderFactory;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/** Rule to detect the window/trigger settings. */
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class BeamAggregationRule extends RelOptRule {
  public static final BeamAggregationRule INSTANCE =
      new BeamAggregationRule(Aggregate.class, Project.class, RelFactories.LOGICAL_BUILDER);

  public BeamAggregationRule(
      Class<? extends Aggregate> aggregateClass,
      Class<? extends Project> projectClass,
      RelBuilderFactory relBuilderFactory) {
    super(operand(aggregateClass, operand(projectClass, any())), relBuilderFactory, null);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);
    RelNode x = updateWindow(call, aggregate, project);
    if (x == null) {
      // Non-windowed case should be handled by the BeamBasicAggregationRule
      return;
    }
    call.transformTo(x);
  }

  private static RelNode updateWindow(RelOptRuleCall call, Aggregate aggregate, Project project) {
    ImmutableBitSet groupByFields = aggregate.getGroupSet();
    ArrayList<RexNode> projects = new ArrayList(project.getProjects());

    WindowFn windowFn = null;
    int windowFieldIndex = -1;

    for (int groupFieldIndex : groupByFields.asList()) {
      RexNode projNode = projects.get(groupFieldIndex);
      if (!(projNode instanceof RexCall)) {
        continue;
      }

      RexCall rexCall = (RexCall) projNode;
      WindowFn fn = createWindowFn(rexCall.getOperands(), rexCall.op.kind);
      if (fn != null) {
        windowFn = fn;
        windowFieldIndex = groupFieldIndex;
        projects.set(groupFieldIndex, rexCall.getOperands().get(0));
      }
    }

    if (windowFn == null) {
      return null;
    }

    final Project newProject =
        project.copy(project.getTraitSet(), project.getInput(), projects, project.getRowType());

    return new BeamAggregationRel(
        aggregate.getCluster(),
        aggregate.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
        convert(newProject, newProject.getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        aggregate.getGroupSet(),
        aggregate.getGroupSets(),
        aggregate.getAggCallList(),
        windowFn,
        windowFieldIndex);
  }

  /**
   * Returns a {@link WindowFn} based on the SQL windowing function defined by {#code operatorKind}.
   * Supported {@link SqlKind}s:
   *
   * <ul>
   *   <li>{@link SqlKind#TUMBLE}, mapped to {@link FixedWindows};
   *   <li>{@link SqlKind#HOP}, mapped to {@link SlidingWindows};
   *   <li>{@link SqlKind#SESSION}, mapped to {@link Sessions};
   * </ul>
   *
   * <p>For example:
   *
   * <pre>{@code
   * SELECT event_timestamp, COUNT(*)
   * FROM PCOLLECTION
   * GROUP BY TUMBLE(event_timestamp, INTERVAL '1' HOUR)
   * }</pre>
   *
   * <p>SQL window functions support optional window_offset parameter which indicates a how window
   * definition is offset from the event time. Offset is zero if not specified.
   *
   * <p>Beam model does not support offset for session windows, so this method will throw {@link
   * UnsupportedOperationException} if offset is specified in SQL query for {@link SqlKind#SESSION}.
   */
  private static @Nullable WindowFn createWindowFn(List<RexNode> parameters, SqlKind operatorKind) {
    switch (operatorKind) {
      case TUMBLE:

        // Fixed-size, non-intersecting time-based windows, for example:
        //   every hour aggregate elements from the previous hour;
        //
        // SQL Syntax:
        //   TUMBLE(monotonic_field, window_size [, window_offset])
        //
        // Example:
        //   TUMBLE(event_timestamp_field, INTERVAL '1' HOUR)

        FixedWindows fixedWindows = FixedWindows.of(durationParameter(parameters, 1));
        if (parameters.size() == 3) {
          fixedWindows = fixedWindows.withOffset(durationParameter(parameters, 2));
        }

        return fixedWindows;
      case HOP:

        // Sliding, fixed-size, intersecting time-based windows, for example:
        //   every minute aggregate elements from the previous hour;
        //
        // SQL Syntax:
        //   HOP(monotonic_field, emit_frequency, window_size [, window_offset])

        SlidingWindows slidingWindows =
            SlidingWindows.of(durationParameter(parameters, 2))
                .every(durationParameter(parameters, 1));

        if (parameters.size() == 4) {
          slidingWindows = slidingWindows.withOffset(durationParameter(parameters, 3));
        }

        return slidingWindows;
      case SESSION:

        // Session windows, for example:
        //   aggregate events after a gap of 1 minute of no events;
        //
        // SQL Syntax:
        //   SESSION(monotonic_field, session_gap)
        //
        // Example:
        //   SESSION(event_timestamp_field, INTERVAL '1' MINUTE)

        Sessions sessions = Sessions.withGapDuration(durationParameter(parameters, 1));
        if (parameters.size() == 3) {
          throw new UnsupportedOperationException(
              "Specifying alignment (offset) is not supported for session windows");
        }

        return sessions;
      default:
        return null;
    }
  }

  private static Duration durationParameter(List<RexNode> parameters, int parameterIndex) {
    return Duration.millis(longValue(parameters.get(parameterIndex)));
  }

  private static long longValue(RexNode operand) {
    if (operand instanceof RexLiteral) {
      // @TODO: this can be simplified after CALCITE-2837
      return ((Number) RexLiteral.value(operand)).longValue();
    } else {
      throw new IllegalArgumentException(String.format("[%s] is not valid.", operand));
    }
  }
}
