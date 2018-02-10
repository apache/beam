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
 * 'OVERLAY' operator.
 *
 * <p>
 *   OVERLAY(string1 PLACING string2 FROM integer [ FOR integer2 ])
 * </p>
 */
public class BeamSqlOverlayExpression extends BeamSqlExpression {
  public BeamSqlOverlayExpression(List<BeamSqlExpression> operands) {
    super(operands, SqlTypeName.VARCHAR);
  }

  @Override public boolean accept() {
    if (operands.size() < 3 || operands.size() > 4) {
      return false;
    }

    if (!SqlTypeName.CHAR_TYPES.contains(opType(0))
        || !SqlTypeName.CHAR_TYPES.contains(opType(1))
        || !SqlTypeName.INT_TYPES.contains(opType(2))) {
      return false;
    }

    if (operands.size() == 4 && !SqlTypeName.INT_TYPES.contains(opType(3))) {
      return false;
    }

    return true;
  }

  @Override public BeamSqlPrimitive evaluate(Row inputRow, BoundedWindow window) {
    String str = opValueEvaluated(0, inputRow, window);
    String replaceStr = opValueEvaluated(1, inputRow, window);
    int idx = opValueEvaluated(2, inputRow, window);
    // the index is 1 based.
    idx -= 1;
    int length = replaceStr.length();
    if (operands.size() == 4) {
      length = opValueEvaluated(3, inputRow, window);
    }

    StringBuilder result = new StringBuilder(
        str.length() + replaceStr.length() - length);
    result.append(str.substring(0, idx))
        .append(replaceStr)
        .append(str.substring(idx + length));

    return BeamSqlPrimitive.of(SqlTypeName.VARCHAR, result.toString());
  }
}
