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
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;

/** {@code BeamSqlMathBinaryExpression} for 'TRUNCATE' function. */
public class BeamSqlTruncateExpression extends BeamSqlMathBinaryExpression {

  public BeamSqlTruncateExpression(List<BeamSqlExpression> operands) {
    super(operands, operands.get(0).getOutputType());
  }

  @Override
  public BeamSqlPrimitive<? extends Number> calculate(
      BeamSqlPrimitive leftOp, BeamSqlPrimitive rightOp) {
    BeamSqlPrimitive result = null;
    int rightIntOperand = SqlFunctions.toInt(rightOp.getValue());
    switch (leftOp.getOutputType()) {
      case SMALLINT:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.SMALLINT,
                (short)
                    SqlFunctions.struncate(SqlFunctions.toInt(leftOp.getValue()), rightIntOperand));
        break;
      case TINYINT:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.TINYINT,
                (byte)
                    SqlFunctions.struncate(SqlFunctions.toInt(leftOp.getValue()), rightIntOperand));
        break;
      case INTEGER:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.INTEGER,
                SqlFunctions.struncate(SqlFunctions.toInt(leftOp.getValue()), rightIntOperand));
        break;
      case BIGINT:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.BIGINT, SqlFunctions.struncate(leftOp.getLong(), rightIntOperand));
        break;
      case FLOAT:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.FLOAT,
                (float)
                    SqlFunctions.struncate(
                        SqlFunctions.toFloat(leftOp.getValue()), rightIntOperand));
        break;
      case DOUBLE:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.DOUBLE,
                SqlFunctions.struncate(SqlFunctions.toDouble(leftOp.getValue()), rightIntOperand));
        break;
      case DECIMAL:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.DECIMAL, SqlFunctions.struncate(leftOp.getDecimal(), rightIntOperand));
        break;
      default:
        break;
    }
    return result;
  }
}
