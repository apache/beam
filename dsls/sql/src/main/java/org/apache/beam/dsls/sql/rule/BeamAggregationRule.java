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
package org.apache.beam.dsls.sql.rule;

import com.google.common.collect.ImmutableList;
import java.util.GregorianCalendar;
import java.util.List;
import org.apache.beam.dsls.sql.exception.InvalidFieldException;
import org.apache.beam.dsls.sql.rel.BeamAggregationRel;
import org.apache.beam.dsls.sql.rel.BeamLogicalConvention;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.joda.time.Duration;

/**
 * Rule to detect the window/trigger settings.
 *
 */
public class BeamAggregationRule extends RelOptRule {
  public static final BeamAggregationRule INSTANCE =
      new BeamAggregationRule(Aggregate.class, Project.class, RelFactories.LOGICAL_BUILDER);

  public BeamAggregationRule(
      Class<? extends Aggregate> aggregateClass,
      Class<? extends Project> projectClass,
      RelBuilderFactory relBuilderFactory) {
    super(
        operand(aggregateClass,
            operand(projectClass, any())),
        relBuilderFactory, null);
  }

  public BeamAggregationRule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);
    updateWindowTrigger(call, aggregate, project);
  }

  private void updateWindowTrigger(RelOptRuleCall call, Aggregate aggregate,
      Project project) {
    ImmutableBitSet groupByFields = aggregate.getGroupSet();
    List<RexNode> projectMapping = project.getProjects();

    WindowFn windowFn = new GlobalWindows();
    Trigger triggerFn = Repeatedly.forever(AfterWatermark.pastEndOfWindow());
    int windowFieldIdx = -1;
    Duration allowedLatence = Duration.ZERO;

    for (int groupField : groupByFields.asList()) {
      RexNode projNode = projectMapping.get(groupField);
      if (projNode instanceof RexCall) {
        SqlOperator op = ((RexCall) projNode).op;
        ImmutableList<RexNode> parameters = ((RexCall) projNode).operands;
        String functionName = op.getName();
        switch (functionName) {
        case "TUMBLE":
          windowFieldIdx = groupField;
          windowFn = FixedWindows
              .of(Duration.millis(getWindowParameterAsMillis(parameters.get(1))));
          if (parameters.size() == 3) {
            GregorianCalendar delayTime = (GregorianCalendar) ((RexLiteral) parameters.get(2))
                .getValue();
            triggerFn = createTriggerWithDelay(delayTime);
            allowedLatence = (Duration.millis(delayTime.getTimeInMillis()));
          }
          break;
        case "HOP":
          windowFieldIdx = groupField;
          windowFn = SlidingWindows
              .of(Duration.millis(getWindowParameterAsMillis(parameters.get(1))))
              .every(Duration.millis(getWindowParameterAsMillis(parameters.get(2))));
          if (parameters.size() == 4) {
            GregorianCalendar delayTime = (GregorianCalendar) ((RexLiteral) parameters.get(3))
                .getValue();
            triggerFn = createTriggerWithDelay(delayTime);
            allowedLatence = (Duration.millis(delayTime.getTimeInMillis()));
          }
          break;
        case "SESSION":
          windowFieldIdx = groupField;
          windowFn = Sessions
              .withGapDuration(Duration.millis(getWindowParameterAsMillis(parameters.get(1))));
          if (parameters.size() == 3) {
            GregorianCalendar delayTime = (GregorianCalendar) ((RexLiteral) parameters.get(2))
                .getValue();
            triggerFn = createTriggerWithDelay(delayTime);
            allowedLatence = (Duration.millis(delayTime.getTimeInMillis()));
          }
          break;
        default:
          break;
        }
      }
    }

    BeamAggregationRel newAggregator = new BeamAggregationRel(aggregate.getCluster(),
        aggregate.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
        convert(aggregate.getInput(),
            aggregate.getInput().getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        aggregate.indicator,
        aggregate.getGroupSet(),
        aggregate.getGroupSets(),
        aggregate.getAggCallList(),
        windowFn,
        triggerFn,
        windowFieldIdx,
        allowedLatence);
    call.transformTo(newAggregator);
  }

  private Trigger createTriggerWithDelay(GregorianCalendar delayTime) {
    return Repeatedly.forever(AfterWatermark.pastEndOfWindow().withLateFirings(AfterProcessingTime
        .pastFirstElementInPane().plusDelayOf(Duration.millis(delayTime.getTimeInMillis()))));
  }

  private long getWindowParameterAsMillis(RexNode parameterNode) {
    if (parameterNode instanceof RexLiteral) {
      return RexLiteral.intValue(parameterNode);
    } else {
      throw new InvalidFieldException(String.format("[%s] is not valid.", parameterNode));
    }
  }

}
