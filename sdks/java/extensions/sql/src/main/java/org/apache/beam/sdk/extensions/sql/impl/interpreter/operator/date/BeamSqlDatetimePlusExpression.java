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

import com.google.common.collect.ImmutableSet;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;

/**
 * DATETIME_PLUS operation.
 * Implements TIMESTAMPADD().
 * Input and output are expected to be of type TIMESTAMP
 */
public class BeamSqlDatetimePlusExpression extends BeamSqlExpression {

  private static final Set<SqlTypeName> SUPPORTED_INTERVAL_TYPES = ImmutableSet.of(
      SqlTypeName.INTERVAL_SECOND,
      SqlTypeName.INTERVAL_MINUTE,
      SqlTypeName.INTERVAL_HOUR,
      SqlTypeName.INTERVAL_DAY,
      SqlTypeName.INTERVAL_MONTH,
      SqlTypeName.INTERVAL_YEAR);

  public BeamSqlDatetimePlusExpression(List<BeamSqlExpression> operands) {
    super(operands, SqlTypeName.TIMESTAMP);
  }

  /**
   * Requires exactly 2 operands. One should be a timestamp, another an interval
   */
  @Override
  public boolean accept() {
    return operands.size() == 2
        && SqlTypeName.TIMESTAMP.equals(operands.get(0).getOutputType())
        && SUPPORTED_INTERVAL_TYPES.contains(operands.get(1).getOutputType());
  }

  /**
   * Adds interval to the timestamp.
   */
  @Override
  public BeamSqlPrimitive evaluate(BeamRecord inputRow, BoundedWindow window) {
    DateTime date = new DateTime(operands.get(0).evaluate(inputRow, window).getDate());
    BeamSqlPrimitive intervalPrimitive = operands.get(1).evaluate(inputRow, window);
    SqlTypeName interval = intervalPrimitive.getOutputType();
    BigDecimal intervalMultiplier = intervalPrimitive.getDecimal();

    DateTime newDate = addInterval(date, interval, intervalMultiplier.intValueExact());
    return BeamSqlPrimitive.of(SqlTypeName.TIMESTAMP, newDate.toDate());
  }

  private DateTime addInterval(
      DateTime dateTime, SqlTypeName intervalType, int numberOfIntervals) {

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
        throw new IllegalArgumentException("Adding "
            + intervalType.getName() + " to date is not supported");
    }
  }
}
