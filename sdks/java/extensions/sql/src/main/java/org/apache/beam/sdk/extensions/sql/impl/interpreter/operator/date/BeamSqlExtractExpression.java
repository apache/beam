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

import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.ReadableInstant;

/**
 * {@code BeamSqlExpression} for EXTRACT.
 *
 * <p>The following date functions also implicitly converted to {@code EXTRACT}:
 * <ul>
 *   <li>YEAR(date) =&gt; EXTRACT(YEAR FROM date)</li>
 *   <li>MONTH(date) =&gt; EXTRACT(MONTH FROM date)</li>
 *   <li>DAY(date) =&gt; EXTRACT(DAY FROM date)</li>
 *   <li>QUARTER(date) =&gt; EXTRACT(QUARTER FROM date)</li>
 *   <li>WEEK(date) =&gt; EXTRACT(WEEK FROM date)</li>
 *   <li>DAYOFYEAR(date) =&gt; EXTRACT(DOY FROM date)</li>
 *   <li>DAYOFMONTH(date) =&gt; EXTRACT(DAY FROM date)</li>
 *   <li>DAYOFWEEK(date) =&gt; EXTRACT(DOW FROM date)</li>
 *   <li>HOUR(date) =&gt; EXTRACT(HOUR FROM date)</li>
 *   <li>MINUTE(date) =&gt; EXTRACT(MINUTE FROM date)</li>
 *   <li>SECOND(date) =&gt; EXTRACT(SECOND FROM date)</li>
 * </ul>
 */
public class BeamSqlExtractExpression extends BeamSqlExpression {
  public BeamSqlExtractExpression(List<BeamSqlExpression> operands) {
    super(operands, SqlTypeName.BIGINT);
  }

  @Override public boolean accept() {
    return operands.size() == 2
        && opType(1) == SqlTypeName.TIMESTAMP;
  }

  @Override public BeamSqlPrimitive evaluate(Row inputRow, BoundedWindow window) {
    ReadableInstant time = opValueEvaluated(1, inputRow, window);

    TimeUnitRange unit = ((BeamSqlPrimitive<TimeUnitRange>) op(0)).getValue();

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
        Long extracted = DateTimeUtils.unixDateExtract(
            unit,
            timeByDay
        );
        return BeamSqlPrimitive.of(outputType, extracted);

      case HOUR:
      case MINUTE:
      case SECOND:
        int timeInDay = (int) (time.getMillis() % DateTimeUtils.MILLIS_PER_DAY);
        extracted = (long) DateTimeUtils.unixTimeExtract(
            unit,
            timeInDay
        );
        return BeamSqlPrimitive.of(outputType, extracted);

      default:
        throw new UnsupportedOperationException(
            "Extract for time unit: " + unit + " not supported!");
    }
  }
}
