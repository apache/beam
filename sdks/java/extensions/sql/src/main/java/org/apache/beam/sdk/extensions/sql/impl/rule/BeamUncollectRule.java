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
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamUncollectRel;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.Convention;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.convert.ConverterRule;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.core.Uncollect;

/** A {@code ConverterRule} to replace {@link Uncollect} with {@link BeamUncollectRule}. */
public class BeamUncollectRule extends ConverterRule {
  public static final BeamUncollectRule INSTANCE = new BeamUncollectRule();

  private BeamUncollectRule() {
    super(Uncollect.class, Convention.NONE, BeamLogicalConvention.INSTANCE, "BeamUncollectRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    Uncollect uncollect = (Uncollect) rel;

    return new BeamUncollectRel(
        uncollect.getCluster(),
        uncollect.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
        convert(
            uncollect.getInput(),
            uncollect.getInput().getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        uncollect.withOrdinality);
  }
}
