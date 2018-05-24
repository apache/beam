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

import java.math.BigDecimal;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;

/** {@code BeamSqlMathBinaryExpression} for 'ROUND' function. */
public class BeamSqlRoundExpression extends BeamSqlMathBinaryExpression {

  private final BeamSqlPrimitive zero = BeamSqlPrimitive.of(SqlTypeName.INTEGER, 0);

  public BeamSqlRoundExpression(List<BeamSqlExpression> operands) {
    super(operands, operands.get(0).getOutputType());
    checkForSecondOperand(operands);
  }

  private void checkForSecondOperand(List<BeamSqlExpression> operands) {
    if (numberOfOperands() == 1) {
      operands.add(1, zero);
    }
  }

  @Override
  public BeamSqlPrimitive<? extends Number> calculate(
      BeamSqlPrimitive leftOp, BeamSqlPrimitive rightOp) {
    BeamSqlPrimitive result = null;
    switch (leftOp.getOutputType()) {
      case SMALLINT:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.SMALLINT,
                (short) roundInt(toInt(leftOp.getValue()), toInt(rightOp.getValue())));
        break;
      case TINYINT:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.TINYINT,
                (byte) roundInt(toInt(leftOp.getValue()), toInt(rightOp.getValue())));
        break;
      case INTEGER:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.INTEGER, roundInt(leftOp.getInteger(), toInt(rightOp.getValue())));
        break;
      case BIGINT:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.BIGINT, roundLong(leftOp.getLong(), toInt(rightOp.getValue())));
        break;
      case DOUBLE:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.DOUBLE, roundDouble(leftOp.getDouble(), toInt(rightOp.getValue())));
        break;
      case FLOAT:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.FLOAT,
                (float) roundDouble(leftOp.getFloat(), toInt(rightOp.getValue())));
        break;
      case DECIMAL:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.DECIMAL,
                roundBigDecimal(toBigDecimal(leftOp.getValue()), toInt(rightOp.getValue())));
        break;
      default:
        break;
    }
    return result;
  }

  private int roundInt(int v1, int v2) {
    return SqlFunctions.sround(v1, v2);
  }

  private double roundDouble(double v1, int v2) {
    return SqlFunctions.sround(v1, v2);
  }

  private BigDecimal roundBigDecimal(BigDecimal v1, int v2) {
    return SqlFunctions.sround(v1, v2);
  }

  private long roundLong(long v1, int v2) {
    return SqlFunctions.sround(v1, v2);
  }

  private int toInt(Object value) {
    return SqlFunctions.toInt(value);
  }

  private BigDecimal toBigDecimal(Object value) {
    return SqlFunctions.toBigDecimal(value);
  }
}
