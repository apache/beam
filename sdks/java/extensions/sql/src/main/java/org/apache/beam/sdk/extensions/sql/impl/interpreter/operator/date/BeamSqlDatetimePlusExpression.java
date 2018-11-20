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

import java.math.BigDecimal;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlExpressionEnvironment;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;

/**
 * DATETIME_PLUS operation. Calcite converts 'TIMESTAMPADD(..)' or 'DATE + INTERVAL' from the user
 * input into DATETIME_PLUS.
 *
 * <p>Input and output are expected to be of type TIMESTAMP.
 */
public class BeamSqlDatetimePlusExpression extends BeamSqlExpression {
  public BeamSqlDatetimePlusExpression(List<BeamSqlExpression> operands) {
    super(operands, SqlTypeName.TIMESTAMP);
  }

  /** Requires exactly 2 operands. One should be a timestamp, another an interval */
  @Override
  public boolean accept() {
    return operands.size() == 2
        && SqlTypeName.DATETIME_TYPES.contains(operands.get(0).getOutputType())
        && TimeUnitUtils.INTERVALS_DURATIONS_TYPES.containsKey(operands.get(1).getOutputType());
  }

  /**
   * Adds interval to the timestamp.
   *
   * <p>Interval has a value of 'multiplier * TimeUnit.multiplier'.
   *
   * <p>For example, '3 years' is going to have a type of INTERVAL_YEAR, and a value of 36. And '2
   * minutes' is going to be an INTERVAL_MINUTE with a value of 120000. This is the way Calcite
   * handles interval expressions, and {@link BeamSqlIntervalMultiplyExpression} also works the same
   * way.
   */
  @Override
  public BeamSqlPrimitive evaluate(
      Row inputRow, BoundedWindow window, BeamSqlExpressionEnvironment env) {
    DateTime timestamp = getTimestampOperand(inputRow, window, env);
    BeamSqlPrimitive intervalOperandPrimitive = getIntervalOperand(inputRow, window, env);
    SqlTypeName intervalOperandType = intervalOperandPrimitive.getOutputType();
    int intervalMultiplier = getIntervalMultiplier(intervalOperandPrimitive);

    DateTime newDate = addInterval(timestamp, intervalOperandType, intervalMultiplier);
    return BeamSqlPrimitive.of(SqlTypeName.TIMESTAMP, newDate);
  }

  private int getIntervalMultiplier(BeamSqlPrimitive intervalOperandPrimitive) {
    BigDecimal intervalOperandValue = intervalOperandPrimitive.getDecimal();
    BigDecimal multiplier =
        intervalOperandValue.divide(
            timeUnitInternalMultiplier(intervalOperandPrimitive.getOutputType()),
            BigDecimal.ROUND_CEILING);
    return multiplier.intValueExact();
  }

  private BeamSqlPrimitive getIntervalOperand(
      Row inputRow, BoundedWindow window, BeamSqlExpressionEnvironment env) {
    return findExpressionOfType(operands, TimeUnitUtils.INTERVALS_DURATIONS_TYPES.keySet())
        .get()
        .evaluate(inputRow, window, env);
  }

  private DateTime getTimestampOperand(
      Row inputRow, BoundedWindow window, BeamSqlExpressionEnvironment env) {
    BeamSqlPrimitive timestampOperandPrimitive =
        findExpressionOfType(operands, SqlTypeName.DATETIME_TYPES)
            .get()
            .evaluate(inputRow, window, env);
    return new DateTime(timestampOperandPrimitive.getDate());
  }

  private DateTime addInterval(DateTime dateTime, SqlTypeName intervalType, int numberOfIntervals) {
    switch (intervalType) {
      case INTERVAL_SECOND:
        return dateTime.plusSeconds(numberOfIntervals);
      case INTERVAL_MINUTE:
        return dateTime.plusMinutes(numberOfIntervals);
      case INTERVAL_HOUR:
        return dateTime.plusHours(numberOfIntervals);
      case INTERVAL_DAY:
        return dateTime.plusDays(numberOfIntervals);
      case INTERVAL_MONTH:
        return dateTime.plusMonths(numberOfIntervals);
      case INTERVAL_YEAR:
        return dateTime.plusYears(numberOfIntervals);
      default:
        throw new IllegalArgumentException(
            "Adding " + intervalType.getName() + " to date is not supported");
    }
  }
}
