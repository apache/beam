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

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamCalcRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamLogicalConvention;
import org.apache.beam.sdk.extensions.sql.zetasql.translation.ZetaSqlScalarFunctionImpl;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.Convention;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.plan.RelOptRuleCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.convert.ConverterRule;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.core.Calc;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexCall;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexInputRef;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexLiteral;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.validate.SqlUserDefinedFunction;

/** {@link ConverterRule} to replace {@link Calc} with {@link BeamCalcRel}. */
public class BeamJavaUdfCalcRule extends ConverterRule {
  public static final BeamJavaUdfCalcRule INSTANCE = new BeamJavaUdfCalcRule();

  private BeamJavaUdfCalcRule() {
    super(
        LogicalCalc.class, Convention.NONE, BeamLogicalConvention.INSTANCE, "BeamJavaUdfCalcRule");
  }

  /**
   * Returns true if all the following are true: All RexCalls can be implemented by codegen, All
   * RexCalls only contain ZetaSQL user-defined Java functions, All RexLiterals pass ZetaSQL
   * compliance tests, All RexInputRefs pass ZetaSQL compliance tests, No other RexNode types
   * Otherwise returns false. ZetaSQL user-defined Java functions are in the category whose function
   * group is equal to {@code SqlAnalyzer.USER_DEFINED_JAVA_SCALAR_FUNCTIONS}
   */
  @Override
  public boolean matches(RelOptRuleCall x) {
    List<RelNode> resList = x.getRelList();
    for (RelNode relNode : resList) {
      if (relNode instanceof LogicalCalc) {
        LogicalCalc logicalCalc = (LogicalCalc) relNode;
        for (RexNode rexNode : logicalCalc.getProgram().getExprList()) {
          if (rexNode instanceof RexCall) {
            RexCall call = (RexCall) rexNode;
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
              } else {
                // Reject other UDFs
                return false;
              }
            } else {
              // Reject Calcite implementations
              return false;
            }
          } else if (rexNode instanceof RexLiteral) {
            if (!udfSupportsLiteralType(rexNode.getType())) {
              return false;
            }
          } else if (rexNode instanceof RexInputRef) {
            if (!udfSupportsInputType(rexNode.getType())) {
              return false;
            }
          } else {
            // Reject everything else
            return false;
          }
        }
      }
    }
    return true;
  }

  @Override
  public RelNode convert(RelNode rel) {
    final Calc calc = (Calc) rel;
    final RelNode input = calc.getInput();

    return new BeamCalcRel(
        calc.getCluster(),
        calc.getTraitSet().replace(BeamLogicalConvention.INSTANCE),
        RelOptRule.convert(input, input.getTraitSet().replace(BeamLogicalConvention.INSTANCE)),
        calc.getProgram());
  }

  /**
   * Returns true only if the literal can be correctly implemented by {@link
   * org.apache.beam.sdk.extensions.sql.impl.rel.BeamCalcRel} in ZetaSQL.
   */
  private static boolean udfSupportsLiteralType(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case BIGINT:
      case BOOLEAN:
      case DECIMAL:
      case DOUBLE:
      case TIMESTAMP:
      case VARBINARY:
      case VARCHAR:
      case CHAR:
      case BINARY:
      case NULL:
        return true;
      case DATE: // BEAM-11990
      case TIME: // BEAM-12086
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE: // BEAM-12087
      default:
        return false;
    }
  }

  /**
   * Returns true only if the input type can be correctly implemented by {@link
   * org.apache.beam.sdk.extensions.sql.impl.rel.BeamCalcRel} in ZetaSQL.
   */
  private static boolean udfSupportsInputType(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case BIGINT:
      case BOOLEAN:
      case DECIMAL:
      case DOUBLE:
      case TIMESTAMP:
      case VARBINARY:
      case VARCHAR:
        return true;
      case ARRAY:
        return udfSupportsInputType(type.getComponentType());
      case ROW:
        return type.getFieldList().stream()
            .allMatch((field) -> udfSupportsInputType(field.getType()));
      case DATE: // BEAM-11990
      case TIME: // BEAM-12086
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE: // BEAM-12087
      default:
        return false;
    }
  }
}
