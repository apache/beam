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
package org.apache.beam.dsls.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Iterator;

import org.apache.beam.dsls.sql.rel.BeamRelNode;
import org.apache.beam.dsls.sql.rule.BeamFilterRule;
import org.apache.beam.dsls.sql.rule.BeamIOSinkRule;
import org.apache.beam.dsls.sql.rule.BeamIOSourceRule;
import org.apache.beam.dsls.sql.rule.BeamProjectRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RuleSet;

/**
 * {@link RuleSet} used in {@link BeamQueryPlanner}. It translates a standard
 * Calcite {@link RelNode} tree, to represent with {@link BeamRelNode}
 *
 */
public class BeamRuleSets {
  private static final ImmutableSet<RelOptRule> calciteToBeamConversionRules = ImmutableSet
      .<RelOptRule>builder().add(BeamIOSourceRule.INSTANCE, BeamProjectRule.INSTANCE,
          BeamFilterRule.INSTANCE, BeamIOSinkRule.INSTANCE)
      .build();

  public static RuleSet[] getRuleSets() {
    return new RuleSet[] { new BeamRuleSet(
        ImmutableSet.<RelOptRule>builder().addAll(calciteToBeamConversionRules).build()) };
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
