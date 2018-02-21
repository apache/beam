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
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;

/** A {@code ConverterRule} to replace {@link TableScan} with {@link BeamIOSourceRel}. */
public class BeamIOSourceRule extends ConverterRule {

  private final BeamSqlEnv sqlEnv;

  public static BeamIOSourceRule forSqlEnv(BeamSqlEnv sqlEnv) {
    return new BeamIOSourceRule(sqlEnv);
  }

  private BeamIOSourceRule(BeamSqlEnv sqlEnv) {
    super(
        LogicalTableScan.class,
        Convention.NONE,
        BeamLogicalConvention.INSTANCE,
        "BeamIOSourceRule");
    this.sqlEnv = sqlEnv;
  }

  @Override
  public RelNode convert(RelNode rel) {
    final TableScan scan = (TableScan) rel;

    return new BeamIOSourceRel(
        sqlEnv,
        scan.getCluster(),
        scan.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
        scan.getTable());
  }
}
