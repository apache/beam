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
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * 'SUBSTRING' operator.
 *
 * <p>SUBSTRING(string FROM integer) SUBSTRING(string FROM integer FOR integer)
 */
public class BeamSqlSubstringExpression extends BeamSqlExpression {
  public BeamSqlSubstringExpression(List<BeamSqlExpression> operands) {
    super(operands, SqlTypeName.VARCHAR);
  }

  @Override
  public boolean accept() {
    if (operands.size() < 2 || operands.size() > 3) {
      return false;
    }

    if (!SqlTypeName.CHAR_TYPES.contains(opType(0)) || !SqlTypeName.INT_TYPES.contains(opType(1))) {
      return false;
    }

    if (operands.size() == 3 && !SqlTypeName.INT_TYPES.contains(opType(2))) {
      return false;
    }

    return true;
  }

  @Override
  public BeamSqlPrimitive evaluate(
      Row inputRow, BoundedWindow window, ImmutableMap<Integer, Object> correlateEnv) {
    String str = opValueEvaluated(0, inputRow, window, correlateEnv);
    int idx = opValueEvaluated(1, inputRow, window, correlateEnv);
    int startIdx = idx;
    if (startIdx > 0) {
      // NOTE: SQL substring is 1 based(rather than 0 based)
      startIdx -= 1;
    } else if (startIdx < 0) {
      // NOTE: SQL also support negative index...
      startIdx += str.length();
    } else {
      return BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "");
    }

    if (operands.size() == 3) {
      int length = opValueEvaluated(2, inputRow, window, correlateEnv);
      if (length < 0) {
        length = 0;
      }
      int endIdx = Math.min(startIdx + length, str.length());
      return BeamSqlPrimitive.of(SqlTypeName.VARCHAR, str.substring(startIdx, endIdx));
    } else {
      return BeamSqlPrimitive.of(SqlTypeName.VARCHAR, str.substring(startIdx));
    }
  }
}
