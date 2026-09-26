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
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.impl.rel.StageName;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.Convention;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptPlanner;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelOptRuleOperandChildren;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.plan.RelTrait;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.convert.ConverterRule;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.rules.TransformationRule;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.RuleSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.RuleSets;

/**
 * Wraps a planner rule so that the nodes it produces inherit a {@link StageName} composed from the
 * nodes it matched.
 *
 * <p>None of the rules that fuse relational nodes propagate hints, and they are Calcite's, not
 * Beam's, to change. Beam does own the rule set, so it can hand each rule a {@link
 * StageNameRuleCall} that labels whatever the rule produces.
 *
 * <p>The planner sees the wrapper: a rule looked up by identity in {@code
 * RelOptPlanner#getRules()}, or excluded by instance, has to be looked up as the wrapper rather
 * than as the original. Rule <em>bodies</em> are unaffected, because {@code
 * StageNameRuleCall#getRule} hands back the delegate.
 */
@Internal
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class StageNameRule extends RelOptRule {

  private final RelOptRule delegate;

  /** The rule this one stands in for. */
  protected RelOptRule delegate() {
    return delegate;
  }

  /**
   * Returns {@code rule} wrapped so its output is labelled, or {@code rule} itself where wrapping
   * is not applicable.
   *
   * <p>The planner branches on {@link ConverterRule}, {@link SubstitutionRule} and {@link
   * TransformationRule}, so the wrapper has to present the same markers. Converter rules cannot be
   * wrapped at all -- the planner reads their in/out traits to register trait conversions -- so
   * they carry hints through explicitly instead.
   *
   * <p>Wrapping happens here, at the boundary where rules are handed to a planner, rather than in
   * {@code BeamRuleSets}. A wrapper is not an instance of the rule class it wraps, and callers
   * select rules out of the published rule set by type.
   */
  public static RelOptRule wrap(RelOptRule rule) {
    if (rule instanceof ConverterRule) {
      return rule;
    }
    if (rule instanceof SubstitutionRule) {
      return new Substitution(rule);
    }
    if (rule instanceof TransformationRule) {
      return new Transformation(rule);
    }
    return new StageNameRule(rule);
  }

  /** {@code ruleSets} with every rule wrapped, for handing to a planner. */
  public static Collection<RuleSet> wrapAll(Collection<RuleSet> ruleSets) {
    List<RuleSet> wrapped = new ArrayList<>();
    for (RuleSet ruleSet : ruleSets) {
      List<RelOptRule> rules = new ArrayList<>();
      for (RelOptRule rule : ruleSet) {
        rules.add(wrap(rule));
      }
      wrapped.add(RuleSets.ofList(rules));
    }
    return wrapped;
  }

  /**
   * Adds {@code rule} to {@code planner} wrapped, dropping the unwrapped original.
   *
   * <p>A wrapper keeps its delegate's description and the planner requires descriptions to be
   * unique. Beam's rule set overlaps Calcite's defaults, which are registered before Beam gets a
   * chance, so a wrapper has to displace its original rather than sit alongside it -- otherwise
   * both fire and the unlabelled result may win.
   */
  public static void addTo(RelOptPlanner planner, RelOptRule rule) {
    RelOptRule wrapped = wrap(rule);
    if (wrapped instanceof StageNameRule) {
      planner.removeRule(rule);
    }
    planner.addRule(wrapped);
  }

  private StageNameRule(RelOptRule delegate) {
    super(
        CopiedOperand.copyOf(delegate.getOperand()),
        delegate.relBuilderFactory,
        delegate.toString());
    this.delegate = delegate;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return delegate.matches(call);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    delegate.onMatch(new StageNameRuleCall(call, delegate));
  }

  @Override
  public Convention getOutConvention() {
    return delegate.getOutConvention();
  }

  @Override
  public RelTrait getOutTrait() {
    return delegate.getOutTrait();
  }

  private static class Transformation extends StageNameRule implements TransformationRule {
    Transformation(RelOptRule delegate) {
      super(delegate);
    }
  }

  private static class Substitution extends StageNameRule implements SubstitutionRule {
    Substitution(RelOptRule delegate) {
      super(delegate);
    }

    @Override
    public boolean autoPruneOld() {
      return ((SubstitutionRule) delegate()).autoPruneOld();
    }
  }

  /**
   * A structural clone of another rule's operand tree.
   *
   * <p>Constructing a {@link RelOptRule} re-points its operands back at itself, so the wrapper must
   * not share the delegate's operands: the delegate singletons are also registered directly with
   * Calcite's own internal planners.
   */
  private static class CopiedOperand extends RelOptRuleOperand {
    private CopiedOperand(
        Class<? extends RelNode> clazz,
        RelTrait trait,
        Predicate<RelNode> predicate,
        RelOptRuleOperandChildren children) {
      super(clazz, trait, predicate, children);
    }

    static RelOptRuleOperand copyOf(RelOptRuleOperand source) {
      List<RelOptRuleOperand> children = new ArrayList<>();
      for (RelOptRuleOperand child : source.getChildOperands()) {
        children.add(copyOf(child));
      }
      return new CopiedOperand(
          source.getMatchedClass(),
          source.trait,
          // Class and trait are re-checked by the copy's own matches() before this runs, so the
          // delegate's predicate is only consulted for the part it alone knows.
          source::matches,
          new RelOptRuleOperandChildren(source.childPolicy, children));
    }
  }
}
