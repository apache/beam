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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.map;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironment;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;

/** Represents MAP expression in SQL. */
public class BeamSqlMapExpression extends BeamSqlExpression {
  public BeamSqlMapExpression(List<BeamSqlExpression> operands) {
    super(operands, SqlTypeName.MAP);
  }

  @Override
  public boolean accept() {
    int distinctCount = 2;
    if (operands.size() < 2) {
      return false;
    }
    if (operands.get(0).getOutputType().equals(operands.get(1).getOutputType())) {
      distinctCount = 1;
    }
    return operands.stream().map(BeamSqlExpression::getOutputType).distinct().count()
        == distinctCount;
  }

  @Override
  public BeamSqlPrimitive evaluate(
      Row inputRow, BoundedWindow window, BeamSqlExpressionEnvironment env) {
    Map<Object, Object> elements = new HashMap<>();
    for (int idx = 0; idx < operands.size() / 2; ++idx) {
      elements.put(
          operands.get(idx * 2).evaluate(inputRow, window, env).getValue(),
          operands.get(idx * 2 + 1).evaluate(inputRow, window, env).getValue());
    }
    return BeamSqlPrimitive.of(outputType, elements);
  }
}
