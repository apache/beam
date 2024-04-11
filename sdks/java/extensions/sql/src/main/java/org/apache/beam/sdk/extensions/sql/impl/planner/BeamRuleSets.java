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
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamAggregationRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamBasicAggregationRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamCalcMergeRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamCalcRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamCoGBKJoinRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamEnumerableConverterRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamIOPushDownRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamIntersectRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamJoinAssociateRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamJoinPushThroughJoinRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamMatchRule;
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
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.rules.CoreRules;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.tools.RuleSet;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.tools.RuleSets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/**
 * {@link RuleSet} used in {@code BeamQueryPlanner}. It translates a standard Calcite {@link
 * RelNode} tree, to represent with {@link BeamRelNode}
 */
@Internal
public class BeamRuleSets {
  private static final List<RelOptRule> LOGICAL_OPTIMIZATIONS =
      ImmutableList.of(
          // Rules for window functions
          CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW,
          // Rules so we only have to implement Calc
          CoreRules.FILTER_TO_CALC,
          CoreRules.PROJECT_TO_CALC,
          BeamIOPushDownRule.INSTANCE,
          // disabled due to https://issues.apache.org/jira/browse/BEAM-6810
          // CoreRules.CALC_REMOVE,

          // Rules to merge matching Calcs together.
          BeamCalcMergeRule.INSTANCE,

          // push a filter into a join
          CoreRules.FILTER_INTO_JOIN,
          // push filter into the children of a join
          CoreRules.JOIN_CONDITION_PUSH,
          // push filter through an aggregation
          CoreRules.FILTER_AGGREGATE_TRANSPOSE,
          // push filter through set operation
          CoreRules.FILTER_SET_OP_TRANSPOSE,
          // push project through set operation
          CoreRules.PROJECT_SET_OP_TRANSPOSE,

          // aggregation and projection rules
          // BeamAggregateProjectMergeRule.INSTANCE,
          // push a projection past a filter or vice versa
          CoreRules.PROJECT_FILTER_TRANSPOSE,
          CoreRules.FILTER_PROJECT_TRANSPOSE,
          // push a projection to the children of a join
          // merge projections
          CoreRules.PROJECT_MERGE,
          // CoreRules.PROJECT_REMOVE,
          // reorder sort and projection
          CoreRules.SORT_PROJECT_TRANSPOSE,

          // join rules
          CoreRules.JOIN_PUSH_EXPRESSIONS,
          CoreRules.JOIN_COMMUTE,
          BeamJoinAssociateRule.INSTANCE,
          BeamJoinPushThroughJoinRule.RIGHT,
          BeamJoinPushThroughJoinRule.LEFT,

          // remove union with only a single child
          CoreRules.UNION_REMOVE,
          // convert non-all union into all-union + distinct
          CoreRules.UNION_TO_DISTINCT,

          // remove aggregation if it does not aggregate and input is already distinct
          CoreRules.AGGREGATE_REMOVE,
          // push aggregate through join
          CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED,
          // aggregate union rule
          CoreRules.AGGREGATE_UNION_AGGREGATE,

          // reduce aggregate functions like AVG, STDDEV_POP etc.
          // CoreRules.AGGREGATE_REDUCE_FUNCTIONS,

          // remove unnecessary sort rule
          // https://github.com/apache/beam/issues/19006
          // CoreRules.SORT_REMOVE,,
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
          BeamSideInputLookupJoinRule.INSTANCE,
          BeamMatchRule.INSTANCE);

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
