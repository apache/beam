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
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * {@code BeamSqlMathUnaryExpression} for 'ABS' function.
 */
public class BeamSqlAbsExpression extends BeamSqlMathUnaryExpression {

  public BeamSqlAbsExpression(List<BeamSqlExpression> operands) {
    super(operands);
  }

  @Override
  public BeamSqlPrimitive calculate(BeamSqlPrimitive op) {
    switch (op.getOutputType()) {
      case INTEGER:
        return BeamSqlPrimitive.of(SqlTypeName.INTEGER, SqlFunctions.abs(op.getInteger()));
      case BIGINT:
        return BeamSqlPrimitive.of(SqlTypeName.BIGINT, SqlFunctions.abs(op.getLong()));
      case TINYINT:
        return BeamSqlPrimitive.of(SqlTypeName.TINYINT, SqlFunctions.abs(op.getByte()));
      case SMALLINT:
        return BeamSqlPrimitive.of(SqlTypeName.SMALLINT, SqlFunctions.abs(op.getShort()));
      case FLOAT:
        return BeamSqlPrimitive.of(SqlTypeName.FLOAT, SqlFunctions.abs(op.getFloat()));
      default:
        return BeamSqlPrimitive.of(SqlTypeName.DOUBLE, SqlFunctions.abs(op.getDouble()));
    }
  }
}
