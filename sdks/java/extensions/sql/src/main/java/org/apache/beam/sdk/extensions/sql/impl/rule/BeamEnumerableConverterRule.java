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

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamEnumerableConverter;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.convert.ConverterRule;

/** A {@code ConverterRule} to Convert {@link BeamRelNode} to {@link EnumerableConvention}. */
public class BeamEnumerableConverterRule extends ConverterRule {
  public static final BeamEnumerableConverterRule INSTANCE = new BeamEnumerableConverterRule();

  private BeamEnumerableConverterRule() {
    super(
        RelNode.class,
        BeamLogicalConvention.INSTANCE,
        EnumerableConvention.INSTANCE,
        "BeamEnumerableConverterRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutTrait());
    return new BeamEnumerableConverter(rel.getCluster(), newTraitSet, rel);
  }
}
