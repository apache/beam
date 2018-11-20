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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.math;

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironment;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;

/** Base class for all binary functions such as POWER, MOD, RAND_INTEGER, ATAN2, ROUND, TRUNCATE. */
public abstract class BeamSqlMathBinaryExpression extends BeamSqlExpression {

  public BeamSqlMathBinaryExpression(List<BeamSqlExpression> operands, SqlTypeName outputType) {
    super(operands, outputType);
  }

  @Override
  public boolean accept() {
    return numberOfOperands() == 2 && isOperandNumeric(opType(0)) && isOperandNumeric(opType(1));
  }

  @Override
  public BeamSqlPrimitive<? extends Number> evaluate(
      Row inputRow, BoundedWindow window, BeamSqlExpressionEnvironment env) {
    BeamSqlExpression leftOp = op(0);
    BeamSqlExpression rightOp = op(1);
    return calculate(
        leftOp.evaluate(inputRow, window, env), rightOp.evaluate(inputRow, window, env));
  }

  /**
   * The base method for implementation of math binary functions.
   *
   * @param leftOp {@link BeamSqlPrimitive}
   * @param rightOp {@link BeamSqlPrimitive}
   * @return {@link BeamSqlPrimitive}
   */
  public abstract BeamSqlPrimitive<? extends Number> calculate(
      BeamSqlPrimitive leftOp, BeamSqlPrimitive rightOp);

  /** The method to check whether operands are numeric or not. */
  public boolean isOperandNumeric(SqlTypeName opType) {
    return SqlTypeName.NUMERIC_TYPES.contains(opType);
  }
}
