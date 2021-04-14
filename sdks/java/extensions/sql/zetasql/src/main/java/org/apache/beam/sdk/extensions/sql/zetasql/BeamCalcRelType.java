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

import org.apache.beam.sdk.extensions.sql.impl.rel.BeamCalcRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.extensions.sql.impl.rel.CalcRelSplitter;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.ZetaSqlScalarFunctionImpl;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptCluster;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelTraitSet;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexDynamicParam;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexFieldAccess;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLocalRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexProgram;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link CalcRelSplitter.RelType} for {@link BeamCalcRel}. */
class BeamCalcRelType extends CalcRelSplitter.RelType {
  private static final Logger LOG = LoggerFactory.getLogger(BeamCalcRelType.class);

  BeamCalcRelType(String name) {
    super(name);
  }

  @Override
  protected boolean canImplement(RexFieldAccess field) {
    // Don't implement freestanding field accesses. Only implement field accesses that are operands
    // to UDF calls.
    return false;
  }

  @Override
  protected boolean canImplement(RexLiteral literal) {
    // Don't implement freestanding literals. Only implement literals that are operands to UDF
    // calls.
    return false;
  }

  @Override
  protected boolean canImplement(RexDynamicParam param) {
    return false;
  }

  @Override
  protected boolean canImplement(RexCall call) {
    final SqlOperator operator = call.getOperator();

    CallImplementor implementor = RexImpTable.INSTANCE.get(operator);
    if (implementor == null) {
      // Reject methods with no implementation
      return false;
    }

    if (operator instanceof SqlUserDefinedFunction) {
      SqlUserDefinedFunction udf = (SqlUserDefinedFunction) call.op;
      if (udf.function instanceof ZetaSqlScalarFunctionImpl) {
        ZetaSqlScalarFunctionImpl scalarFunction = (ZetaSqlScalarFunctionImpl) udf.function;
        if (!scalarFunction.functionGroup.equals(
            BeamZetaSqlCatalog.USER_DEFINED_JAVA_SCALAR_FUNCTIONS)) {
          // Reject ZetaSQL Builtin Scalar Functions
          return false;
        }
        for (RexNode operand : call.getOperands()) {
          if (operand instanceof RexLocalRef) {
            if (!(BeamJavaUdfCalcRule.udfSupportsLiteralType(operand.getType())
                || BeamJavaUdfCalcRule.udfSupportsInputType(operand.getType()))) {
              LOG.error(
                  "User-defined function {} received unsupported operand type {}.",
                  call.op.getName(),
                  ((RexLocalRef) operand).getType());
              return false;
            }
          } else {
            LOG.error(
                "User-defined function {} received unrecognized operand kind {}.",
                call.op.getName(),
                operand.getKind());
            return false;
          }
        }
      } else {
        // Reject other UDFs
        return false;
      }
    } else {
      // Reject Calcite implementations
      return false;
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
    return new BeamCalcRel(
        cluster,
        traitSet.replace(BeamLogicalConvention.INSTANCE),
        RelOptRule.convert(input, input.getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        normalizedProgram);
  }
}
