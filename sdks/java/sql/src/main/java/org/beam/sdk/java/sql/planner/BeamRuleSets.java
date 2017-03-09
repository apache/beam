package org.beam.sdk.java.sql.planner;

import java.util.Iterator;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RuleSet;
import org.beam.sdk.java.sql.rel.BeamRelNode;
import org.beam.sdk.java.sql.rule.BeamFilterRule;
import org.beam.sdk.java.sql.rule.BeamIOSinkRule;
import org.beam.sdk.java.sql.rule.BeamIOSourceRule;
import org.beam.sdk.java.sql.rule.BeamProjectRule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * {@link RuleSet} which translate a standard Calcite {@link RelNode} tree, to represent with {@link BeamRelNode}
 * 
 */
public class BeamRuleSets {
  private static final ImmutableSet<RelOptRule> calciteToBeamConversionRules = ImmutableSet
      .<RelOptRule>builder()
      .add(
          BeamIOSourceRule.INSTANCE, BeamProjectRule.INSTANCE, BeamFilterRule.INSTANCE,
          BeamIOSinkRule.INSTANCE
      ).build();

  public static RuleSet[] getRuleSets() {
    return new RuleSet[] {new BeamRuleSet(ImmutableSet.<RelOptRule>builder()
            .addAll(calciteToBeamConversionRules).build()) };
  }

  private static class BeamRuleSet implements RuleSet {
    final ImmutableSet<RelOptRule> rules;

    public BeamRuleSet(ImmutableSet<RelOptRule> rules) {
      this.rules = rules;
    }

    public BeamRuleSet(ImmutableList<RelOptRule> rules) {
      this.rules = ImmutableSet.<RelOptRule>builder().addAll(rules).build();
    }

    @Override
    public Iterator<RelOptRule> iterator() {
      return rules.iterator();
    }
  }

}
