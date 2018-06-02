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
package org.apache.beam.sdk.extensions.sql.impl.planner;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamAggregationRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamCalcRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamEnumerableConverterRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamIntersectRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamJoinRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamMinusRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamSortRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamUncollectRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamUnionRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamUnnestRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamValuesRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.CalcRemoveRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

/**
 * {@link RuleSet} used in {@code BeamQueryPlanner}. It translates a standard Calcite {@link
 * RelNode} tree, to represent with {@link BeamRelNode}
 */
@Internal
public class BeamRuleSets {

  private static final List<RelOptRule> LOGICAL_OPTIMIZATIONS =
      ImmutableList.of(
          // Rules so we only have to implement Calc
          FilterCalcMergeRule.INSTANCE,
          ProjectCalcMergeRule.INSTANCE,
          FilterToCalcRule.INSTANCE,
          ProjectToCalcRule.INSTANCE,
          CalcRemoveRule.INSTANCE,
          CalcMergeRule.INSTANCE);

  private static final List<RelOptRule> BEAM_CONVERTERS =
      ImmutableList.of(
          BeamCalcRule.INSTANCE,
          BeamAggregationRule.INSTANCE,
          BeamSortRule.INSTANCE,
          BeamValuesRule.INSTANCE,
          BeamIntersectRule.INSTANCE,
          BeamMinusRule.INSTANCE,
          BeamUnionRule.INSTANCE,
          BeamUncollectRule.INSTANCE,
          BeamUnnestRule.INSTANCE,
          BeamJoinRule.INSTANCE);

  private static final List<RelOptRule> BEAM_TO_ENUMERABLE =
      ImmutableList.of(BeamEnumerableConverterRule.INSTANCE);

  public static RuleSet[] getRuleSets() {
    return new RuleSet[] {
      RuleSets.ofList(
          ImmutableList.<RelOptRule>builder()
              .addAll(LOGICAL_OPTIMIZATIONS)
              .addAll(BEAM_CONVERTERS)
              .addAll(BEAM_TO_ENUMERABLE)
              .build())
    };
  }
}
