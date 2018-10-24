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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator;

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironment;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;

/** {@code BeamSqlCaseExpression} represents CASE, NULLIF, COALESCE in SQL. */
public class BeamSqlCaseExpression extends BeamSqlExpression {

  public BeamSqlCaseExpression(List<BeamSqlExpression> operands) {
    // the return type of CASE is the type of the `else` condition
    super(operands, operands.get(operands.size() - 1).getOutputType());
  }

  @Override
  public boolean accept() {
    // `when`-`then` pair + `else`
    if (operands.size() % 2 != 1) {
      return false;
    }

    for (int i = 0; i < operands.size() - 1; i += 2) {
      if (opType(i) != SqlTypeName.BOOLEAN) {
        return false;
      } else if (opType(i + 1) != outputType) {
        return false;
      }
    }

    return true;
  }

  @Override
  public BeamSqlPrimitive evaluate(
      Row inputRow, BoundedWindow window, BeamSqlExpressionEnvironment env) {
    for (int i = 0; i < operands.size() - 1; i += 2) {
      Boolean wasOpEvaluated = (Boolean) opValueEvaluated(i, inputRow, window, env);
      if (wasOpEvaluated != null && wasOpEvaluated) {
        return BeamSqlPrimitive.of(outputType, opValueEvaluated(i + 1, inputRow, window, env));
      }
    }
    return BeamSqlPrimitive.of(
        outputType, opValueEvaluated(operands.size() - 1, inputRow, window, env));
  }
}
