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

import static org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.date.BeamSqlDatetimeMinusExpression.INTERVALS_DURATIONS_TYPES;

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;
import org.joda.time.DurationFieldType;
import org.joda.time.Period;
import org.joda.time.PeriodType;

/**
 * Infix '-' operation for timestamps.
 *
 * <p>Currently this implementation is specific to how Calcite parses 'TIMESTAMPDIFF(..)'.
 * It converts the TIMESTAMPDIFF() call into infix minus and normalizes it
 * with corresponding TimeUnit's multiplier.
 *
 * <p>See {@link BeamSqlDatetimeMinusExpression} for other kinds of datetime types subtraction.
 */
public class BeamSqlTimestampMinusTimestampExpression extends BeamSqlExpression {
  private SqlTypeName intervalType;

  public BeamSqlTimestampMinusTimestampExpression(
      List<BeamSqlExpression> operands, SqlTypeName intervalType) {
    super(operands, SqlTypeName.BIGINT);
    this.intervalType = intervalType;
  }

  /**
   * Requires exactly 2 operands. One should be a timestamp, another an interval
   */
  @Override
  public boolean accept() {
    return accept(operands, intervalType);
  }

  static boolean accept(List<BeamSqlExpression> operands, SqlTypeName intervalType) {
    return INTERVALS_DURATIONS_TYPES.containsKey(intervalType)
        && operands.size() == 2
        && SqlTypeName.TIMESTAMP.equals(operands.get(0).getOutputType())
        && SqlTypeName.TIMESTAMP.equals(operands.get(1).getOutputType());
  }

  /**
   * Returns the count of intervals between dates, times TimeUnit.multiplier of the interval type.
   * Calcite deals with all intervals this way. Whenever there is an interval, its value is always
   * multiplied by the corresponding TimeUnit.multiplier
   */
  public BeamSqlPrimitive evaluate(Row inputRow, BoundedWindow window) {
    DateTime timestampStart = new DateTime((Object) opValueEvaluated(1, inputRow, window));
    DateTime timestampEnd = new DateTime((Object) opValueEvaluated(0, inputRow, window));

    long numberOfIntervals = numberOfIntervalsBetweenDates(timestampStart, timestampEnd);
    long multiplier = TimeUnitUtils.timeUnitInternalMultiplier(intervalType).longValue();

    return BeamSqlPrimitive.of(SqlTypeName.BIGINT, multiplier * numberOfIntervals);
  }

  private long numberOfIntervalsBetweenDates(DateTime timestampStart, DateTime timestampEnd) {
    Period period = new Period(timestampStart, timestampEnd,
        PeriodType.forFields(new DurationFieldType[] { durationFieldType(intervalType) }));
    return period.get(durationFieldType(intervalType));
  }

  private static DurationFieldType durationFieldType(SqlTypeName intervalTypeToCount) {
    if (!INTERVALS_DURATIONS_TYPES.containsKey(intervalTypeToCount)) {
      throw new IllegalArgumentException("Counting "
          + intervalTypeToCount.getName() + "s between dates is not supported");
    }

    return INTERVALS_DURATIONS_TYPES.get(intervalTypeToCount);
  }
}
