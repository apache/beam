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

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;
import org.joda.time.DurationFieldType;
import org.joda.time.Period;

/**
 * '-' operator for 'timestamp - interval' expressions.
 *
 * <p>See {@link BeamSqlDatetimeMinusExpression} for other kinds of datetime types subtraction.
 */
public class BeamSqlTimestampMinusIntervalExpression extends BeamSqlExpression {

  public BeamSqlTimestampMinusIntervalExpression(
      List<BeamSqlExpression> operands, SqlTypeName outputType) {
    super(operands, outputType);
  }

  @Override
  public boolean accept() {
    return accept(operands, outputType);
  }

  static boolean accept(List<BeamSqlExpression> operands, SqlTypeName outputType) {
    return operands.size() == 2
        && SqlTypeName.TIMESTAMP.equals(outputType)
        && SqlTypeName.TIMESTAMP.equals(operands.get(0).getOutputType())
        && INTERVALS_DURATIONS_TYPES.containsKey(operands.get(1).getOutputType());
  }

  @Override
  public BeamSqlPrimitive evaluate(Row row, BoundedWindow window) {
    DateTime date = new DateTime((Object) opValueEvaluated(0, row, window));
    Period period = intervalToPeriod(op(1).evaluate(row, window));

    Date subtractionResult = date.minus(period).toDate();

    return BeamSqlPrimitive.of(outputType, subtractionResult);
  }

  private Period intervalToPeriod(BeamSqlPrimitive operand) {
    BigDecimal intervalValue = operand.getDecimal();
    SqlTypeName intervalType = operand.getOutputType();

    int numberOfIntervals = intervalValue
        .divide(TimeUnitUtils.timeUnitInternalMultiplier(intervalType)).intValueExact();

    return new Period().withField(durationFieldType(intervalType), numberOfIntervals);
  }

  private static DurationFieldType durationFieldType(SqlTypeName intervalTypeToCount) {
    return INTERVALS_DURATIONS_TYPES.get(intervalTypeToCount);
  }
}
