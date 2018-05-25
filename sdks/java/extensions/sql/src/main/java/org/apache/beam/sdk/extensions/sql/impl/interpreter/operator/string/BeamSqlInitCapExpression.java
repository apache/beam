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

/** 'INITCAP' operator. */
public class BeamSqlInitCapExpression extends BeamSqlStringUnaryExpression {
  public BeamSqlInitCapExpression(List<BeamSqlExpression> operands) {
    super(operands, SqlTypeName.VARCHAR);
  }

  @Override
  public BeamSqlPrimitive evaluate(
      Row inputRow, BoundedWindow window, ImmutableMap<Integer, Object> correlateEnv) {
    String str = opValueEvaluated(0, inputRow, window, correlateEnv);

    StringBuilder ret = new StringBuilder(str);
    boolean isInit = true;
    for (int i = 0; i < str.length(); i++) {
      if (Character.isWhitespace(str.charAt(i))) {
        isInit = true;
        continue;
      }

      if (isInit) {
        ret.setCharAt(i, Character.toUpperCase(str.charAt(i)));
        isInit = false;
      } else {
        ret.setCharAt(i, Character.toLowerCase(str.charAt(i)));
      }
    }
    return BeamSqlPrimitive.of(SqlTypeName.VARCHAR, ret.toString());
  }
}
