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

package org.apache.beam.dsls.sql.interpreter.operator.arithmetic;

import java.util.List;

import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Base class for all arithmetic operators.
 */
public abstract class BeamSqlArithmeticExpression extends BeamSqlExpression {
  private BeamSqlArithmeticExpression(List<BeamSqlExpression> operands, SqlTypeName outputType) {
    super(operands, outputType);
  }

  public BeamSqlArithmeticExpression(List<BeamSqlExpression> operands) {
    // the outputType can not be determined in constructor
    // will be determined in evaluate() method. ANY here is just a placeholder.
    super(operands, SqlTypeName.ANY);
  }

  @Override public boolean accept() {
    if (operands.size() != 2) {
      return false;
    }

    for (BeamSqlExpression operand : operands) {
      if (!SqlTypeName.NUMERIC_TYPES.contains(operand.getOutputType())) {
        return false;
      }
    }
    return true;
  }

  /**
   * https://dev.mysql.com/doc/refman/5.7/en/arithmetic-functions.html.
   */
  @Override public BeamSqlPrimitive<? extends Number> evaluate(BeamSqlRow inputRecord) {
    BeamSqlExpression leftOp = operands.get(0);
    BeamSqlExpression rightOp = operands.get(1);

    // In the case of -, +, and *, the result is calculated as Long if both
    // operands are INT_TYPES(byte, short, integer, long).
    if (SqlTypeName.INT_TYPES.contains(leftOp.getOutputType())
        && SqlTypeName.INT_TYPES.contains(rightOp.getOutputType())) {
      Long leftValue = Long.valueOf(leftOp.evaluate(inputRecord).getValue().toString());
      Long rightValue = Long.valueOf(rightOp.evaluate(inputRecord).getValue().toString());
      Long ret = calc(leftValue, rightValue);
      return BeamSqlPrimitive.of(SqlTypeName.BIGINT, ret);
    } else {
      // If any of the operands of a +, -, /, *, % is a real
      //  OR
      // It is a division calculation
      // we treat them as Double
      double leftValue = getDouble(inputRecord, leftOp);
      double rightValue = getDouble(inputRecord, rightOp);
      return BeamSqlPrimitive.of(SqlTypeName.DOUBLE, calc(leftValue, rightValue));
    }
  }

  private double getDouble(BeamSqlRow inputRecord, BeamSqlExpression op) {
    Object raw = op.evaluate(inputRecord).getValue();
    Double ret = null;
    if (SqlTypeName.NUMERIC_TYPES.contains(op.getOutputType())) {
      ret = ((Number) raw).doubleValue();
    }

    return ret;
  }

  /**
   * For {@link SqlTypeName#INT_TYPES} calculation of '+', '-', '*'.
   */
  public abstract Long calc(Long left, Long right);


  /**
   * For other {@link SqlTypeName#NUMERIC_TYPES} of '+', '-', '*', '/'.
   */
  public abstract Double calc(Number left, Number right);
}
