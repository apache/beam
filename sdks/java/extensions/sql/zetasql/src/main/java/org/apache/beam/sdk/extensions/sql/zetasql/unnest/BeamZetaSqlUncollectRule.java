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
package org.apache.beam.sdk.extensions.sql.zetasql.unnest;

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.plan.Convention;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.convert.ConverterRule;

/**
 * A {@code ConverterRule} to replace {@link ZetaSqlUnnest} with {@link BeamZetaSqlUncollectRel}.
 *
 * <p>This class is a copy of {@link org.apache.beam.sdk.extensions.sql.impl.rel.BeamUncollectRel}
 * except that it works on {@link ZetaSqlUnnest} instead of Calcite Uncollect.
 *
 * <p>Details of why unwrapping structs breaks ZetaSQL UNNEST syntax is in
 * https://issues.apache.org/jira/browse/BEAM-10896.
 */
public class BeamZetaSqlUncollectRule extends ConverterRule {
  public static final BeamZetaSqlUncollectRule INSTANCE = new BeamZetaSqlUncollectRule();

  private BeamZetaSqlUncollectRule() {
    super(
        ZetaSqlUnnest.class, Convention.NONE, BeamLogicalConvention.INSTANCE, "BeamUncollectRule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    ZetaSqlUnnest uncollect = (ZetaSqlUnnest) rel;

    return new BeamZetaSqlUncollectRel(
        uncollect.getCluster(),
        uncollect.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
        convert(
            uncollect.getInput(),
            uncollect.getInput().getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        uncollect.withOrdinality);
  }
}
