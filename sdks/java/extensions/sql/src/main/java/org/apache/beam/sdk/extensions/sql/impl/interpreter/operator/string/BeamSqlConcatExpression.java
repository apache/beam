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

package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.string;

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * String concat operator.
 */
public class BeamSqlConcatExpression extends BeamSqlExpression {

  protected BeamSqlConcatExpression(List<BeamSqlExpression> operands, SqlTypeName outputType) {
    super(operands, outputType);
  }

  public BeamSqlConcatExpression(List<BeamSqlExpression> operands) {
    super(operands, SqlTypeName.VARCHAR);
  }

  @Override public boolean accept() {
    if (operands.size() != 2) {
      return false;
    }

    for (BeamSqlExpression exp : getOperands()) {
      if (!SqlTypeName.CHAR_TYPES.contains(exp.getOutputType())) {
        return false;
      }
    }

    return true;
  }

  @Override public BeamSqlPrimitive evaluate(Row inputRow, BoundedWindow window) {
    String left = opValueEvaluated(0, inputRow, window);
    String right = opValueEvaluated(1, inputRow, window);

    return BeamSqlPrimitive.of(SqlTypeName.VARCHAR,
        new StringBuilder(left.length() + right.length())
            .append(left).append(right).toString());
  }
}
