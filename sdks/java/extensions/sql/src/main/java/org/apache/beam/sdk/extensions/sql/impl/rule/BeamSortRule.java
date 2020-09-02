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
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSortRel;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.Convention;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.convert.ConverterRule;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.Sort;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.logical.LogicalSort;

/** {@code ConverterRule} to replace {@code Sort} with {@code BeamSortRel}. */
public class BeamSortRule extends ConverterRule {
  public static final BeamSortRule INSTANCE = new BeamSortRule();

  private BeamSortRule() {
    super(LogicalSort.class, Convention.NONE, BeamLogicalConvention.INSTANCE, "BeamSortRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    Sort sort = (Sort) rel;
    final RelNode input = sort.getInput();
    return new BeamSortRel(
        sort.getCluster(),
        sort.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
        convert(input, input.getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        sort.getCollation(),
        sort.offset,
        sort.fetch);
  }
}
