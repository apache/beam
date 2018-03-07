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

import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamAggregationRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamFilterRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamIOSinkRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamIOSourceRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamIntersectRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamJoinRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamMinusRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamProjectRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamSortRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamUnionRule;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamValuesRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

/**
 * {@link RuleSet} used in {@link BeamQueryPlanner}. It translates a standard Calcite {@link
 * RelNode} tree, to represent with {@link BeamRelNode}
 */
public class BeamRuleSets {

  public static RuleSet[] getRuleSets(BeamSqlEnv sqlEnv) {
    return new RuleSet[] {
      RuleSets.ofList(
          BeamIOSourceRule.forSqlEnv(sqlEnv),
          BeamProjectRule.INSTANCE,
          BeamFilterRule.INSTANCE,
          BeamIOSinkRule.forSqlEnv(sqlEnv),
          BeamAggregationRule.INSTANCE,
          BeamSortRule.INSTANCE,
          BeamValuesRule.INSTANCE,
          BeamIntersectRule.INSTANCE,
          BeamMinusRule.INSTANCE,
          BeamUnionRule.INSTANCE,
          BeamJoinRule.forSqlEnv(sqlEnv))
    };
  }
}
