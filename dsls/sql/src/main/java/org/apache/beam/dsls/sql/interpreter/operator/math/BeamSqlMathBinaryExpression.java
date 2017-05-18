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

package org.apache.beam.dsls.sql.interpreter.operator.math;

import java.util.List;

import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Base class for all binary functions such as
 * POWER, MOD, RAND_INTEGER, ATAN2, ROUND, TRUNCATE.
 */
public abstract class BeamSqlMathBinaryExpression extends BeamSqlExpression {

  private BeamSqlMathBinaryExpression(List<BeamSqlExpression> operands, SqlTypeName outputType) {
    super(operands, outputType);
  }

  public BeamSqlMathBinaryExpression(List<BeamSqlExpression> operands) {
    this(operands, SqlTypeName.ANY);
  }

  @Override public boolean accept() {
    boolean acceptance = false;

    if (numberOfOperands() == 2 && SqlTypeName.NUMERIC_TYPES.contains(opType(0))
        && SqlTypeName.NUMERIC_TYPES.contains(opType(1))) {
      acceptance = true;
    }
    return acceptance;
  }

  @Override public BeamSqlPrimitive<? extends Number> evaluate(BeamSQLRow inputRecord) {
    BeamSqlExpression leftOp = op(0);
    BeamSqlExpression rightOp = op(1);
    if (SqlTypeName.INT_TYPES.contains(leftOp.getOutputType()) && SqlTypeName.INT_TYPES
        .contains(rightOp.getOutputType())) {
      Long result = calculate(Long.valueOf(leftOp.evaluate(inputRecord).getValue().toString()),
          Long.valueOf(rightOp.evaluate(inputRecord).getValue().toString()));
      return BeamSqlPrimitive.of(SqlTypeName.BIGINT, result);
    } else {
      Double result = calculate(Double.valueOf(leftOp.evaluate(inputRecord).getValue().toString()),
          Double.valueOf(rightOp.evaluate(inputRecord).getValue().toString()));
      return BeamSqlPrimitive.of(SqlTypeName.DOUBLE, result);
    }
  }

  /**
   * For the operands of type {@link SqlTypeName#INT_TYPES}.
   * */
  public abstract Long calculate(Long leftOp, Long rightOp);

  /**
   * For the operands of other type {@link SqlTypeName#NUMERIC_TYPES}.
   * */
  public abstract Double calculate(Number leftOp, Number rightOp);
}
