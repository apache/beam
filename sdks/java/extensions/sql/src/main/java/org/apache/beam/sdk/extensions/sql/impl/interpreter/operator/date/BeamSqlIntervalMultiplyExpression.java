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

package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date;

import static org.apache.beam.sdk.extensions.sql.impl.utils.SqlTypeUtils.findExpressionOfType;

import java.math.BigDecimal;
import java.util.List;

import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Multiplication operator for intervals.
 * For example, allows to express things like '3 years'. Enables TIMESTAMPADD() functionality.
 */
public class BeamSqlIntervalMultiplyExpression extends BeamSqlExpression {
  public BeamSqlIntervalMultiplyExpression(List<BeamSqlExpression> operands) {
    super(operands, deduceOutputType(operands));
  }

  /**
   * Output type is null if no operands found with matching types.
   * Execution will later fail when calling accept()
   */
  private static SqlTypeName deduceOutputType(List<BeamSqlExpression> operands) {
    return findExpressionOfType(operands, SqlTypeName.INTERVAL_TYPES).orNull();
  }

  /**
   * Requires exactly 2 operands. One should be integer, another should be interval
   */
  @Override
  public boolean accept() {
    return operands.size() == 2
        && findExpressionOfType(operands, SqlTypeName.INTEGER).isPresent()
        && findExpressionOfType(operands, SqlTypeName.INTERVAL_TYPES).isPresent();
  }
  /**
   * Evaluates the number of times the interval should be repeated, as BigDecimal.
   * For example for '3 * MONTH' this will return an object with type INTERVAL_MONTH and value 3
   */
  @Override
  public BeamSqlPrimitive evaluate(BeamRecord inputRow, BoundedWindow window) {
    int multiplier = operands.get(1).evaluate(inputRow, window).getInteger();
    return BeamSqlPrimitive.of(outputType, new BigDecimal(multiplier));
  }
}
