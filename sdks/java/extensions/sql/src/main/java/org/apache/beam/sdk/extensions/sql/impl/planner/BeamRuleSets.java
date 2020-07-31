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

import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamAggregateProjectMergeRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamAggregationRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamBasicAggregationRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamCalcRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamCoGBKJoinRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamEnumerableConverterRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamIOPushDownRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamIntersectRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamJoinAssociateRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamJoinPushThroughJoinRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamMinusRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamSideInputJoinRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamSideInputLookupJoinRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamSortRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamTableFunctionScanRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamUncollectRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamUnionRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamUnnestRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamValuesRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamWindowRule;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.AggregateUnionAggregateRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.ProjectSortTransposeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.UnionEliminatorRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RuleSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RuleSets;

/**
 * {@link RuleSet} used in {@code BeamQueryPlanner}. It translates a standard Calcite {@link
 * RelNode} tree, to represent with {@link BeamRelNode}
 */
@Internal
public class BeamRuleSets {
  private static final List<RelOptRule> LOGICAL_OPTIMIZATIONS =
      ImmutableList.of(
          // Rules for window functions
          ProjectToWindowRule.PROJECT,
          // Rules so we only have to implement Calc
          FilterCalcMergeRule.INSTANCE,
          ProjectCalcMergeRule.INSTANCE,
          FilterToCalcRule.INSTANCE,
          ProjectToCalcRule.INSTANCE,
          BeamIOPushDownRule.INSTANCE,
          // disabled due to https://issues.apache.org/jira/browse/BEAM-6810
          // CalcRemoveRule.INSTANCE,
          CalcMergeRule.INSTANCE,

          // push a filter into a join
          FilterJoinRule.FILTER_ON_JOIN,
          // push filter into the children of a join
          FilterJoinRule.JOIN,
          // push filter through an aggregation
          FilterAggregateTransposeRule.INSTANCE,
          // push filter through set operation
          FilterSetOpTransposeRule.INSTANCE,
          // push project through set operation
          ProjectSetOpTransposeRule.INSTANCE,

          // aggregation and projection rules
          BeamAggregateProjectMergeRule.INSTANCE,
          // push a projection past a filter or vice versa
          ProjectFilterTransposeRule.INSTANCE,
          FilterProjectTransposeRule.INSTANCE,
          // push a projection to the children of a join
          // merge projections
          ProjectMergeRule.INSTANCE,
          // ProjectRemoveRule.INSTANCE,
          // reorder sort and projection
          SortProjectTransposeRule.INSTANCE,
          ProjectSortTransposeRule.INSTANCE,

          // join rules
          JoinPushExpressionsRule.INSTANCE,
          JoinCommuteRule.INSTANCE,
          BeamJoinAssociateRule.INSTANCE,
          BeamJoinPushThroughJoinRule.RIGHT,
          BeamJoinPushThroughJoinRule.LEFT,

          // remove union with only a single child
          UnionEliminatorRule.INSTANCE,
          // convert non-all union into all-union + distinct
          UnionToDistinctRule.INSTANCE,

          // remove aggregation if it does not aggregate and input is already distinct
          AggregateRemoveRule.INSTANCE,
          // push aggregate through join
          AggregateJoinTransposeRule.EXTENDED,
          // aggregate union rule
          AggregateUnionAggregateRule.INSTANCE,

          // reduce aggregate functions like AVG, STDDEV_POP etc.
          // AggregateReduceFunctionsRule.INSTANCE,

          // remove unnecessary sort rule
          // https://issues.apache.org/jira/browse/BEAM-5073
          // SortRemoveRule.INSTANCE,
          BeamTableFunctionScanRule.INSTANCE,
          // prune empty results rules
          PruneEmptyRules.AGGREGATE_INSTANCE,
          PruneEmptyRules.FILTER_INSTANCE,
          PruneEmptyRules.JOIN_LEFT_INSTANCE,
          PruneEmptyRules.JOIN_RIGHT_INSTANCE,
          PruneEmptyRules.PROJECT_INSTANCE,
          PruneEmptyRules.SORT_INSTANCE,
          PruneEmptyRules.UNION_INSTANCE);

  private static final List<RelOptRule> BEAM_CONVERTERS =
      ImmutableList.of(
          BeamWindowRule.INSTANCE,
          BeamCalcRule.INSTANCE,
          BeamAggregationRule.INSTANCE,
          BeamBasicAggregationRule.INSTANCE,
          BeamSortRule.INSTANCE,
          BeamValuesRule.INSTANCE,
          BeamIntersectRule.INSTANCE,
          BeamMinusRule.INSTANCE,
          BeamUnionRule.INSTANCE,
          BeamUncollectRule.INSTANCE,
          BeamUnnestRule.INSTANCE,
          BeamSideInputJoinRule.INSTANCE,
          BeamCoGBKJoinRule.INSTANCE,
          BeamSideInputLookupJoinRule.INSTANCE);

  private static final List<RelOptRule> BEAM_TO_ENUMERABLE =
      ImmutableList.of(BeamEnumerableConverterRule.INSTANCE);

  public static Collection<RuleSet> getRuleSets() {

    return ImmutableList.of(
        RuleSets.ofList(
            ImmutableList.<RelOptRule>builder()
                .addAll(BEAM_CONVERTERS)
                .addAll(BEAM_TO_ENUMERABLE)
                .addAll(LOGICAL_OPTIMIZATIONS)
                .build()));
  }
}
