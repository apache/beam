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

import static org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.TimeUnitUtils.timeUnitInternalMultiplier;
import static org.apache.beam.sdk.extensions.sql.impl.utils.SqlTypeUtils.findExpressionOfType;

import com.google.common.base.Optional;
import java.math.BigDecimal;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Multiplication operator for intervals.
 * For example, allows to express things like '3 years'.
 *
 * <p>One use case of this is implementation of TIMESTAMPADD().
 * Calcite converts TIMESTAMPADD(date, multiplier, inteval) into
 * DATETIME_PLUS(date, multiplier * interval).
 * The 'multiplier * interval' part is what this class implements. It's not a regular
 * numerical multiplication because the return type is expected to be an interval, and the value
 * is expected to use corresponding TimeUnit's internal value (e.g. 12 for YEAR, 60000 for MINUTE).
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
    Optional<BeamSqlExpression> intervalOperand =
        findExpressionOfType(operands, SqlTypeName.INTERVAL_TYPES);

    return intervalOperand.isPresent()
        ? intervalOperand.get().getOutputType()
        : null;
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
   * Evaluates the number of times the interval should be repeated, times the TimeUnit multiplier.
   * For example for '3 * MONTH' this will return an object with type INTERVAL_MONTH and value 36.
   *
   * <p>This is due to the fact that TimeUnit has different internal multipliers for each interval,
   * e.g. MONTH is 12, but MINUTE is 60000. When Calcite parses SQL interval literals, it returns
   * those internal multipliers. This means we need to do similar thing, so that this multiplication
   * expression behaves the same way as literal interval expression.
   *
   * <p>That is, we need to make sure that this:
   *   "TIMESTAMP '1984-04-19 01:02:03' + INTERVAL '2' YEAR"
   * is equivalent tot this:
   *   "TIMESTAMPADD(YEAR, 2, TIMESTAMP '1984-04-19 01:02:03')"
   */
  @Override
  public BeamSqlPrimitive evaluate(Row inputRow, BoundedWindow window) {
    BeamSqlPrimitive intervalOperandPrimitive =
        findExpressionOfType(operands, SqlTypeName.INTERVAL_TYPES).get().evaluate(inputRow, window);
    SqlTypeName intervalOperandType = intervalOperandPrimitive.getOutputType();

    BeamSqlPrimitive integerOperandPrimitive =
        findExpressionOfType(operands, SqlTypeName.INTEGER).get().evaluate(inputRow, window);
    BigDecimal integerOperandValue = new BigDecimal(integerOperandPrimitive.getInteger());

    BigDecimal multiplicationResult =
        integerOperandValue.multiply(
            timeUnitInternalMultiplier(intervalOperandType));

    return BeamSqlPrimitive.of(outputType, multiplicationResult);
  }
}
