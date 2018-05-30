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

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Trim operator.
 *
 * <p>TRIM( { BOTH | LEADING | TRAILING } string1 FROM string2)
 */
public class BeamSqlTrimExpression extends BeamSqlExpression {
  public BeamSqlTrimExpression(List<BeamSqlExpression> operands) {
    super(operands, SqlTypeName.VARCHAR);
  }

  @Override
  public boolean accept() {
    if (operands.size() != 1 && operands.size() != 3) {
      return false;
    }

    if (operands.size() == 1 && !SqlTypeName.CHAR_TYPES.contains(opType(0))) {
      return false;
    }

    if (operands.size() == 3
        && (SqlTypeName.SYMBOL != opType(0)
            || !SqlTypeName.CHAR_TYPES.contains(opType(1))
            || !SqlTypeName.CHAR_TYPES.contains(opType(2)))) {
      return false;
    }

    return true;
  }

  @Override
  public BeamSqlPrimitive evaluate(
      Row inputRow, BoundedWindow window, ImmutableMap<Integer, Object> correlateEnv) {
    if (operands.size() == 1) {
      return BeamSqlPrimitive.of(
          SqlTypeName.VARCHAR,
          opValueEvaluated(0, inputRow, window, correlateEnv).toString().trim());
    } else {
      SqlTrimFunction.Flag type = opValueEvaluated(0, inputRow, window, correlateEnv);
      String targetStr = opValueEvaluated(1, inputRow, window, correlateEnv);
      String containingStr = opValueEvaluated(2, inputRow, window, correlateEnv);

      switch (type) {
        case LEADING:
          return BeamSqlPrimitive.of(SqlTypeName.VARCHAR, leadingTrim(containingStr, targetStr));
        case TRAILING:
          return BeamSqlPrimitive.of(SqlTypeName.VARCHAR, trailingTrim(containingStr, targetStr));
        case BOTH:
        default:
          return BeamSqlPrimitive.of(
              SqlTypeName.VARCHAR, trailingTrim(leadingTrim(containingStr, targetStr), targetStr));
      }
    }
  }

  static String leadingTrim(String containingStr, String targetStr) {
    int idx = 0;
    while (containingStr.startsWith(targetStr, idx)) {
      idx += targetStr.length();
    }

    return containingStr.substring(idx);
  }

  static String trailingTrim(String containingStr, String targetStr) {
    int idx = containingStr.length() - targetStr.length();
    while (containingStr.startsWith(targetStr, idx)) {
      idx -= targetStr.length();
    }

    idx += targetStr.length();
    return containingStr.substring(0, idx);
  }
}
