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

package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.reinterpret;

import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.GregorianCalendar;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

/**
 * Unit test for {@link DatetimeReinterpretConversions}.
 */
public class DatetimeReinterpretConversionsTest {
  private static final long DATE_LONG = 1000L;
  private static final Date DATE = new Date(DATE_LONG);
  private static final GregorianCalendar CALENDAR = new GregorianCalendar(2017, 8, 9);

  private static final BeamSqlPrimitive DATE_PRIMITIVE = BeamSqlPrimitive.of(
      SqlTypeName.DATE, DATE);

  private static final BeamSqlPrimitive TIME_PRIMITIVE = BeamSqlPrimitive.of(
      SqlTypeName.TIME, CALENDAR);

  private static final BeamSqlPrimitive TIMESTAMP_PRIMITIVE = BeamSqlPrimitive.of(
      SqlTypeName.TIMESTAMP, DATE);

  @Test public void testTimeToBigint() {
    BeamSqlPrimitive conversionResultPrimitive =
        DatetimeReinterpretConversions.TIME_TO_BIGINT
          .convert(TIME_PRIMITIVE);

    assertEquals(SqlTypeName.BIGINT, conversionResultPrimitive.getOutputType());
    assertEquals(CALENDAR.getTimeInMillis(), conversionResultPrimitive.getLong());
  }

  @Test public void testDateToBigint() {
    BeamSqlPrimitive conversionResultPrimitive =
        DatetimeReinterpretConversions.DATE_TYPES_TO_BIGINT
            .convert(DATE_PRIMITIVE);

    assertEquals(SqlTypeName.BIGINT, conversionResultPrimitive.getOutputType());
    assertEquals(DATE_LONG, conversionResultPrimitive.getLong());
  }

  @Test public void testTimestampToBigint() {
    BeamSqlPrimitive conversionResultPrimitive =
        DatetimeReinterpretConversions.DATE_TYPES_TO_BIGINT
            .convert(TIMESTAMP_PRIMITIVE);

    assertEquals(SqlTypeName.BIGINT, conversionResultPrimitive.getOutputType());
    assertEquals(DATE_LONG, conversionResultPrimitive.getLong());
  }
}
