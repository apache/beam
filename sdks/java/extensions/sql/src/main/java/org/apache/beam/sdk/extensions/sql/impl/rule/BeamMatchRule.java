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

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamMatchRel;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.Convention;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.convert.ConverterRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Match;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.logical.LogicalMatch;

/** {@code ConverterRule} to replace {@code Match} with {@code BeamMatchRel}. */
public class BeamMatchRule extends ConverterRule {
  public static final BeamMatchRule INSTANCE = new BeamMatchRule();

  private BeamMatchRule() {
    super(LogicalMatch.class, Convention.NONE, BeamLogicalConvention.INSTANCE, "BeamMatchRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    Match match = (Match) rel;
    final RelNode input = match.getInput();
    return new BeamMatchRel(
        match.getCluster(),
        match.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
        convert(input, input.getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        match.getRowType(),
        match.getPattern(),
        match.isStrictStart(),
        match.isStrictEnd(),
        match.getPatternDefinitions(),
        match.getMeasures(),
        match.getAfter(),
        match.getSubsets(),
        match.isAllRows(),
        match.getPartitionKeys(),
        match.getOrderKeys(),
        match.getInterval());
  }
}
