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

import java.util.List;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;
import org.joda.time.ReadableInstant;

/** Date operator implementations. */
public class DateOperators {

  /**
   * Implementation of CEIL(<i>date or time</i> TO <i>unit</i>).
   *
   * <p>Only supports months and years.
   */
  public static final BeamSqlOperator DATETIME_CEIL =
      new BeamSqlOperator() {
        @Override
        public boolean accept(List<BeamSqlExpression> arguments) {
          // TODO: https://issues.apache.org/jira/browse/BEAM-4621
          // return acceptMonthOrLarger((TimeUnitRange) ((BeamSqlPrimitive)
          // arguments.get(1)).getValue());
          return true;
        }

        @Override
        public SqlTypeName getOutputType() {
          return SqlTypeName.TIMESTAMP;
        }

        @Override
        public BeamSqlPrimitive apply(List<BeamSqlPrimitive> arguments) {
          ReadableInstant date = arguments.get(0).getDate();
          TimeUnitRange unit = (TimeUnitRange) arguments.get(1).getValue();
          return BeamSqlPrimitive.of(
              getOutputType(),
              new DateTime(
                  DateTimeUtils.unixTimestampCeil(unit, date.getMillis()), date.getZone()));
        }
      };

  /**
   * Implementation of FLOOR(<i>date or time</i> TO <i>unit</i>).
   *
   * <p>Only supports months and years.
   */
  public static final BeamSqlOperator DATETIME_FLOOR =
      new BeamSqlOperator() {
        @Override
        public boolean accept(List<BeamSqlExpression> arguments) {
          // TODO: https://issues.apache.org/jira/browse/BEAM-4621
          // return acceptMonthOrLarger((TimeUnitRange) ((BeamSqlPrimitive)
          // arguments.get(1)).getValue());
          return true;
        }

        @Override
        public SqlTypeName getOutputType() {
          return SqlTypeName.TIMESTAMP;
        }

        @Override
        public BeamSqlPrimitive apply(List<BeamSqlPrimitive> arguments) {
          ReadableInstant date = arguments.get(0).getDate();
          TimeUnitRange unit = (TimeUnitRange) arguments.get(1).getValue();
          return BeamSqlPrimitive.of(
              getOutputType(),
              new DateTime(
                  DateTimeUtils.unixTimestampFloor(unit, date.getMillis()), date.getZone()));
        };
      };

  private static boolean acceptMonthOrLarger(TimeUnitRange unitRange) {
    TimeUnit smallestUnit = unitRange.endUnit == null ? unitRange.startUnit : unitRange.endUnit;
    return smallestUnit.multiplier != null
        && (smallestUnit.multiplier.compareTo(TimeUnit.MONTH.multiplier) >= 0);
  }

  /**
   * {@link BeamSqlOperator} for EXTRACT.
   *
   * <p>The following date functions also implicitly converted to {@code EXTRACT}:
   *
   * <ul>
   *   <li>YEAR(date) =&gt; EXTRACT(YEAR FROM date)
   *   <li>MONTH(date) =&gt; EXTRACT(MONTH FROM date)
   *   <li>DAY(date) =&gt; EXTRACT(DAY FROM date)
   *   <li>QUARTER(date) =&gt; EXTRACT(QUARTER FROM date)
   *   <li>WEEK(date) =&gt; EXTRACT(WEEK FROM date)
   *   <li>DAYOFYEAR(date) =&gt; EXTRACT(DOY FROM date)
   *   <li>DAYOFMONTH(date) =&gt; EXTRACT(DAY FROM date)
   *   <li>DAYOFWEEK(date) =&gt; EXTRACT(DOW FROM date)
   *   <li>HOUR(date) =&gt; EXTRACT(HOUR FROM date)
   *   <li>MINUTE(date) =&gt; EXTRACT(MINUTE FROM date)
   *   <li>SECOND(date) =&gt; EXTRACT(SECOND FROM date)
   * </ul>
   */
  public static final BeamSqlOperator EXTRACT =
      new BeamSqlOperator() {
        @Override
        public boolean accept(List<BeamSqlExpression> arguments) {
          return arguments.size() == 2
              && arguments.get(0).getOutputType() == SqlTypeName.SYMBOL
              && SqlTypeName.DATETIME_TYPES.contains(arguments.get(1).getOutputType());
        }

        @Override
        public SqlTypeName getOutputType() {
          return SqlTypeName.BIGINT;
        }

        @Override
        public BeamSqlPrimitive apply(List<BeamSqlPrimitive> arguments) {
          ReadableInstant time = arguments.get(1).getDate();
          TimeUnitRange unit = (TimeUnitRange) arguments.get(0).getValue();

          switch (unit) {
            case YEAR:
            case QUARTER:
            case MONTH:
            case DAY:
            case DOW:
            case WEEK:
            case DOY:
            case CENTURY:
            case MILLENNIUM:
              Long timeByDay = time.getMillis() / DateTimeUtils.MILLIS_PER_DAY;
              Long extracted = DateTimeUtils.unixDateExtract(unit, timeByDay);
              return BeamSqlPrimitive.of(getOutputType(), extracted);

            case HOUR:
            case MINUTE:
            case SECOND:
              int timeInDay = (int) (time.getMillis() % DateTimeUtils.MILLIS_PER_DAY);
              extracted = (long) DateTimeUtils.unixTimeExtract(unit, timeInDay);
              return BeamSqlPrimitive.of(getOutputType(), extracted);

            default:
              throw new UnsupportedOperationException(
                  "Extract for time unit: " + unit + " not supported!");
          }
        }
      };
}
