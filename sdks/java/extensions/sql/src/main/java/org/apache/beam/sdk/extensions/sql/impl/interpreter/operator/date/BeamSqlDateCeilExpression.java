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

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;
import org.joda.time.ReadableInstant;

/**
 * {@code BeamSqlExpression} for CEIL(date).
 *
 * <p>NOTE: only support CEIL for {@link TimeUnitRange#YEAR} and {@link TimeUnitRange#MONTH}.
 */
public class BeamSqlDateCeilExpression extends BeamSqlExpression {
  public BeamSqlDateCeilExpression(List<BeamSqlExpression> operands) {
    super(operands, SqlTypeName.TIMESTAMP);
  }

  @Override
  public boolean accept() {
    return operands.size() == 2 && opType(1) == SqlTypeName.SYMBOL;
  }

  @Override
  public BeamSqlPrimitive evaluate(
      Row inputRow, BoundedWindow window, ImmutableMap<Integer, Object> correlateEnv) {
    ReadableInstant date = opValueEvaluated(0, inputRow, window, correlateEnv);
    long time = date.getMillis();
    TimeUnitRange unit = ((BeamSqlPrimitive<TimeUnitRange>) op(1)).getValue();

    long newTime = DateTimeUtils.unixTimestampCeil(unit, time);
    DateTime newDate = new DateTime(newTime, date.getZone());

    return BeamSqlPrimitive.of(outputType, newDate);
  }
}
