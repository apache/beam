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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.array;

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironment;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;

/** Implements array element access expression. */
public class BeamSqlArrayItemExpression extends BeamSqlExpression {

  public BeamSqlArrayItemExpression(List<BeamSqlExpression> operands, SqlTypeName sqlTypeName) {

    super(operands, sqlTypeName);
  }

  @Override
  public boolean accept() {
    return operands.size() == 2 && op(0).getOutputType().equals(SqlTypeName.ARRAY);
  }

  @Override
  public BeamSqlPrimitive evaluate(
      Row inputRow, BoundedWindow window, BeamSqlExpressionEnvironment env) {
    List<Object> array = (List) opValueEvaluated(0, inputRow, window, env);
    Integer index = (Integer) opValueEvaluated(1, inputRow, window, env);

    // SQL array indexing is 1 based
    return BeamSqlPrimitive.of(outputType, array.get(index - 1));
  }
}
