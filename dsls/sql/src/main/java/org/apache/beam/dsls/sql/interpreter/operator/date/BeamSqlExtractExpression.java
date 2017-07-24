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

package org.apache.beam.dsls.sql.interpreter.operator.date;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlExpression;
import org.apache.beam.dsls.sql.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.sd.BeamRow;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.type.SqlTypeName;

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
 * </ul>
 */
public class BeamSqlExtractExpression extends BeamSqlExpression {
  private static final Map<TimeUnitRange, Integer> typeMapping = new HashMap<>();
  static {
    typeMapping.put(TimeUnitRange.DOW, Calendar.DAY_OF_WEEK);
    typeMapping.put(TimeUnitRange.DOY, Calendar.DAY_OF_YEAR);
    typeMapping.put(TimeUnitRange.WEEK, Calendar.WEEK_OF_YEAR);
  }

  public BeamSqlExtractExpression(List<BeamSqlExpression> operands) {
    super(operands, SqlTypeName.BIGINT);
  }
  @Override public boolean accept() {
    return operands.size() == 2
        && opType(1) == SqlTypeName.BIGINT;
  }

  @Override public BeamSqlPrimitive evaluate(BeamRow inputRow) {
    Long time = opValueEvaluated(1, inputRow);

    TimeUnitRange unit = ((BeamSqlPrimitive<TimeUnitRange>) op(0)).getValue();

    switch (unit) {
      case YEAR:
      case MONTH:
      case DAY:
        Long timeByDay = time / 1000 / 3600 / 24;
        Long extracted = DateTimeUtils.unixDateExtract(
            unit,
            timeByDay
        );
        return BeamSqlPrimitive.of(outputType, extracted);

      case DOY:
      case DOW:
      case WEEK:
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date(time));
        return BeamSqlPrimitive.of(outputType, (long) calendar.get(typeMapping.get(unit)));

      case QUARTER:
        calendar = Calendar.getInstance();
        calendar.setTime(new Date(time));
        long ret = calendar.get(Calendar.MONTH) / 3;
        if (ret * 3 < calendar.get(Calendar.MONTH)) {
          ret += 1;
        }
        return BeamSqlPrimitive.of(outputType, ret);

      default:
        throw new UnsupportedOperationException(
            "Extract for time unit: " + unit + " not supported!");
    }
  }
}
