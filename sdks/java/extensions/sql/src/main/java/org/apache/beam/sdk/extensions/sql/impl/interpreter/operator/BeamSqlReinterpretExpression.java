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

import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * {@code BeamSqlExpression} for REINTERPRET.
 *
 * <p>Currently only converting from {@link SqlTypeName#DATETIME_TYPES}
 * to {@code BIGINT} is supported.
 */
public class BeamSqlReinterpretExpression extends BeamSqlExpression {
  public BeamSqlReinterpretExpression(List<BeamSqlExpression> operands, SqlTypeName outputType) {
    super(operands, outputType);
  }

  @Override public boolean accept() {
    return getOperands().size() == 1
        && outputType == SqlTypeName.BIGINT
        && SqlTypeName.DATETIME_TYPES.contains(opType(0));
  }

  @Override public BeamSqlPrimitive evaluate(BeamRecord inputRow, BoundedWindow window) {
    if (opType(0) == SqlTypeName.TIME) {
      GregorianCalendar date = opValueEvaluated(0, inputRow, window);
      return BeamSqlPrimitive.of(outputType, date.getTimeInMillis());

    } else {
      Date date = opValueEvaluated(0, inputRow, window);
      return BeamSqlPrimitive.of(outputType, date.getTime());
    }
  }
}
