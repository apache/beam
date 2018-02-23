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

import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamJoinRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalJoin;

/** {@code ConverterRule} to replace {@code Join} with {@code BeamJoinRel}. */
public class BeamJoinRule extends ConverterRule {
  private final BeamSqlEnv sqlEnv;

  public static BeamJoinRule forSqlEnv(BeamSqlEnv sqlEnv) {
    return new BeamJoinRule(sqlEnv);
  }

  private BeamJoinRule(BeamSqlEnv sqlEnv) {
    super(LogicalJoin.class, Convention.NONE, BeamLogicalConvention.INSTANCE, "BeamJoinRule");
    this.sqlEnv = sqlEnv;
  }

  @Override
  public RelNode convert(RelNode rel) {
    Join join = (Join) rel;
    return new BeamJoinRel(
        sqlEnv,
        join.getCluster(),
        join.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
        convert(
            join.getLeft(), join.getLeft().getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        convert(
            join.getRight(), join.getRight().getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        join.getCondition(),
        join.getVariablesSet(),
        join.getJoinType());
  }
}
