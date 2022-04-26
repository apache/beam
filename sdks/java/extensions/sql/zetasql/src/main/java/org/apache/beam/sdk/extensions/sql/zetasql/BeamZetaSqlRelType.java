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
package org.apache.beam.sdk.extensions.sql.zetasql;

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.extensions.sql.impl.rel.CalcRelSplitter;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.ZetaSqlScalarFunctionImpl;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexDynamicParam;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexFieldAccess;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.rex.RexProgram;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.beam.vendor.calcite.v1_28_0.org.apache.calcite.tools.RelBuilder;

/** {@link CalcRelSplitter.RelType} for {@link BeamZetaSqlCalcRel}. */
class BeamZetaSqlRelType extends CalcRelSplitter.RelType {
  BeamZetaSqlRelType(String name) {
    super(name);
  }

  @Override
  protected boolean canImplement(RexFieldAccess field) {
    return true;
  }

  @Override
  protected boolean canImplement(RexDynamicParam param) {
    return true;
  }

  @Override
  protected boolean canImplement(RexLiteral literal) {
    return true;
  }

  @Override
  protected boolean canImplement(RexCall call) {
    if (call.getOperator() instanceof SqlUserDefinedFunction) {
      SqlUserDefinedFunction udf = (SqlUserDefinedFunction) call.op;
      if (udf.function instanceof ZetaSqlScalarFunctionImpl) {
        ZetaSqlScalarFunctionImpl scalarFunction = (ZetaSqlScalarFunctionImpl) udf.function;
        if (scalarFunction.functionGroup.equals(
            BeamZetaSqlCatalog.USER_DEFINED_JAVA_SCALAR_FUNCTIONS)) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  protected RelNode makeRel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelBuilder relBuilder,
      RelNode input,
      RexProgram program) {
    RexProgram normalizedProgram = program.normalize(cluster.getRexBuilder(), false);
    return new BeamZetaSqlCalcRel(
        cluster,
        traitSet.replace(BeamLogicalConvention.INSTANCE),
        RelOptRule.convert(input, input.getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        normalizedProgram);
  }
}
