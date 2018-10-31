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

/**
 * Base class for all unary functions such as ABS, SQRT, LN, LOG10, EXP, CEIL, FLOOR, RAND, ACOS,
 * ASIN, ATAN, COS, COT, DEGREES, RADIANS, SIGN, SIN, TAN.
 */
public abstract class BeamSqlMathUnaryExpression extends BeamSqlExpression {

  public BeamSqlMathUnaryExpression(List<BeamSqlExpression> operands, SqlTypeName outputType) {
    super(operands, outputType);
  }

  @Override
  public boolean accept() {
    boolean acceptance = false;

    if (numberOfOperands() == 1 && SqlTypeName.NUMERIC_TYPES.contains(opType(0))) {
      acceptance = true;
    }
    return acceptance;
  }

  @Override
  public BeamSqlPrimitive<? extends Number> evaluate(
      Row inputRow, BoundedWindow window, BeamSqlExpressionEnvironment env) {
    BeamSqlExpression operand = op(0);
    return calculate(operand.evaluate(inputRow, window, env));
  }

  /** For the operands of other type {@link SqlTypeName#NUMERIC_TYPES}. */
  public abstract BeamSqlPrimitive calculate(BeamSqlPrimitive op);
}
