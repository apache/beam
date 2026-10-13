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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.convert.ConverterRule;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.rules.CoreRules;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.rules.TransformationRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for the wrapper that gives a planner rule's output a provenance label. */
@RunWith(JUnit4.class)
public class StageNameRuleTest {

  /**
   * {@link RelOptRule}'s constructor walks its operand tree and re-points every operand at the rule
   * being constructed. Handing it the delegate's own operands would therefore reassign them away
   * from the delegate -- which is a singleton also registered with Calcite's internal planners.
   */
  @Test
  public void wrappingLeavesTheDelegateOperandsPointingAtTheDelegate() {
    RelOptRule delegate = CoreRules.FILTER_INTO_JOIN;
    RelOptRuleOperand delegateOperand = delegate.getOperand();

    RelOptRule wrapper = StageNameRule.wrap(delegate);

    assertThat(delegateOperand.getRule(), is(sameInstance(delegate)));
    assertThat(delegate.getOperand(), is(sameInstance(delegateOperand)));
    assertThat(wrapper.getOperand(), is(not(sameInstance(delegateOperand))));
    assertThat(wrapper.getOperand().getRule(), is(sameInstance(wrapper)));
  }

  @Test
  public void copiedOperandTreeHasTheSameShape() {
    RelOptRule wrapper = StageNameRule.wrap(CoreRules.FILTER_INTO_JOIN);
    assertSameShape(CoreRules.FILTER_INTO_JOIN.getOperand(), wrapper.getOperand());
  }

  private static void assertSameShape(RelOptRuleOperand source, RelOptRuleOperand copy) {
    assertThat(copy.getMatchedClass(), is(source.getMatchedClass()));
    assertThat(copy.childPolicy, is(source.childPolicy));
    assertThat(copy.getChildOperands().size(), is(source.getChildOperands().size()));
    for (int i = 0; i < source.getChildOperands().size(); i++) {
      assertSameShape(source.getChildOperands().get(i), copy.getChildOperands().get(i));
    }
  }

  /**
   * The planner reads a converter rule's in and out traits to register trait conversions, which a
   * wrapper cannot stand in for. Those rules propagate hints themselves instead.
   */
  @Test
  public void converterRulesAreLeftAlone() {
    RelOptRule converter = BeamCalcRule.INSTANCE;
    assertThat(converter, is(instanceOf(ConverterRule.class)));
    assertThat(StageNameRule.wrap(converter), is(sameInstance(converter)));
  }

  /** The planner branches on these markers, so a wrapper has to present the same ones. */
  @Test
  public void ruleMarkersSurviveWrapping() {
    assertThat(CoreRules.FILTER_INTO_JOIN, is(instanceOf(TransformationRule.class)));
    assertThat(
        StageNameRule.wrap(CoreRules.FILTER_INTO_JOIN), is(instanceOf(TransformationRule.class)));

    assertThat(CoreRules.PROJECT_REMOVE, is(instanceOf(SubstitutionRule.class)));
    assertThat(
        StageNameRule.wrap(CoreRules.PROJECT_REMOVE), is(instanceOf(SubstitutionRule.class)));
  }

  /**
   * A wrapper keeps its delegate's description, and the planner rejects two rules with the same
   * description. Beam's rule set overlaps Calcite's defaults, which {@code JdbcDriver} finds
   * already registered, so the wrapper has to displace the original rather than sit beside it.
   */
  @Test
  public void addToDisplacesAnAlreadyRegisteredRule() {
    VolcanoPlanner planner = new VolcanoPlanner();
    RelOptRule rule = CoreRules.FILTER_INTO_JOIN;
    planner.addRule(rule);

    StageNameRule.addTo(planner, rule);

    assertThat(planner.getRules(), not(hasItem(sameInstance(rule))));
    assertThat(
        planner.getRules().stream()
            .anyMatch(r -> r instanceof StageNameRule && r.toString().equals(rule.toString())),
        is(true));
  }

  @Test
  public void wrapperKeepsTheDelegateDescription() {
    RelOptRule rule = CoreRules.FILTER_INTO_JOIN;
    assertThat(StageNameRule.wrap(rule).toString(), is(rule.toString()));
  }
}
