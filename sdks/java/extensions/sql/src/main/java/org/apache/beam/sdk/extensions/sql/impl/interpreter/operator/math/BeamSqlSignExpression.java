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

/** {@code BeamSqlMathUnaryExpression} for 'SIGN' function. */
public class BeamSqlSignExpression extends BeamSqlMathUnaryExpression {

  public BeamSqlSignExpression(List<BeamSqlExpression> operands) {
    super(operands, operands.get(0).getOutputType());
  }

  @Override
  public BeamSqlPrimitive calculate(BeamSqlPrimitive op) {
    BeamSqlPrimitive result = null;
    switch (op.getOutputType()) {
      case TINYINT:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.TINYINT, (byte) SqlFunctions.sign(SqlFunctions.toByte(op.getValue())));
        break;
      case SMALLINT:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.SMALLINT,
                (short) SqlFunctions.sign(SqlFunctions.toShort(op.getValue())));
        break;
      case INTEGER:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.INTEGER, SqlFunctions.sign(SqlFunctions.toInt(op.getValue())));
        break;
      case BIGINT:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.BIGINT, SqlFunctions.sign(SqlFunctions.toLong(op.getValue())));
        break;
      case FLOAT:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.FLOAT, (float) SqlFunctions.sign(SqlFunctions.toFloat(op.getValue())));
        break;
      case DOUBLE:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.DOUBLE, SqlFunctions.sign(SqlFunctions.toDouble(op.getValue())));
        break;
      case DECIMAL:
        result =
            BeamSqlPrimitive.of(
                SqlTypeName.DECIMAL, SqlFunctions.sign(SqlFunctions.toBigDecimal(op.getValue())));
        break;
      default:
        break;
    }
    return result;
  }
}
