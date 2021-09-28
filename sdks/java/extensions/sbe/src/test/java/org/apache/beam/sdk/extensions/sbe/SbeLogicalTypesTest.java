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
package org.apache.beam.sdk.extensions.sbe;

import static org.apache.beam.sdk.extensions.sbe.SbeLogicalTypes.LOCAL_MKT_DATE;
import static org.apache.beam.sdk.extensions.sbe.SbeLogicalTypes.TZ_TIMESTAMP;
import static org.apache.beam.sdk.extensions.sbe.SbeLogicalTypes.TZ_TIME_ONLY;
import static org.apache.beam.sdk.extensions.sbe.SbeLogicalTypes.UTC_DATE_ONLY;
import static org.apache.beam.sdk.extensions.sbe.SbeLogicalTypes.UTC_TIMESTAMP;
import static org.apache.beam.sdk.extensions.sbe.SbeLogicalTypes.UTC_TIME_ONLY;
import static org.apache.beam.sdk.extensions.sbe.Schemas.TZ_TIME_SCHEMA;
import static org.apache.beam.sdk.extensions.sbe.Schemas.TimeFieldNames.TIME_FIELD_NAME;
import static org.apache.beam.sdk.extensions.sbe.Schemas.TimeFieldNames.TIME_ZONE_HOUR_FIELD_NAME;
import static org.apache.beam.sdk.extensions.sbe.Schemas.TimeFieldNames.TIME_ZONE_MINUTE_FIELD_NAME;
import static org.apache.beam.sdk.extensions.sbe.Schemas.TimeFieldNames.UNIT_FIELD_NAME;
import static org.apache.beam.sdk.extensions.sbe.Schemas.UTC_TIME_SCHEMA;
import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.time.LocalDate;
import org.apache.beam.sdk.extensions.sbe.TimeValues.SbeTimeUnit;
import org.apache.beam.sdk.extensions.sbe.TimeValues.TZTimeOnlyValue;
import org.apache.beam.sdk.extensions.sbe.TimeValues.TZTimestampValue;
import org.apache.beam.sdk.extensions.sbe.TimeValues.UTCTimeOnlyValue;
import org.apache.beam.sdk.extensions.sbe.TimeValues.UTCTimestampValue;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class SbeLogicalTypesTest {
  private static final long TIME = Instant.now().toEpochMilli();
  private static final byte UNIT = SbeTimeUnit.MILLISECONDS.asByte();
  private static final byte ZONE_HOUR = -2;
  private static final byte ZONE_MINUTE = 30;

  private static final Row UTC_ROW =
      Row.withSchema(UTC_TIME_SCHEMA)
          .withFieldValue(TIME_FIELD_NAME, TIME)
          .withFieldValue(UNIT_FIELD_NAME, UNIT)
          .build();
  private static final UTCTimestampValue UTC_TIMESTAMP_VALUE = UTCTimestampValue.fromRow(UTC_ROW);
  private static final UTCTimeOnlyValue UTC_TIME_ONLY_VALUE = UTCTimeOnlyValue.fromRow(UTC_ROW);

  private static final Row TZ_ROW =
      Row.withSchema(TZ_TIME_SCHEMA)
          .withFieldValue(TIME_FIELD_NAME, TIME)
          .withFieldValue(UNIT_FIELD_NAME, UNIT)
          .withFieldValue(TIME_ZONE_HOUR_FIELD_NAME, ZONE_HOUR)
          .withFieldValue(TIME_ZONE_MINUTE_FIELD_NAME, ZONE_MINUTE)
          .build();
  private static final TZTimestampValue TZ_TIMESTAMP_VALUE = TZTimestampValue.fromRow(TZ_ROW);
  private static final TZTimeOnlyValue TZ_TIME_ONLY_VALUE = TZTimeOnlyValue.fromRow(TZ_ROW);

  private static final int DAYS_SINCE_UNIX_EPOCH = 20000;
  private static final LocalDate LOCAL_DATE = LocalDate.ofEpochDay(DAYS_SINCE_UNIX_EPOCH);

  @Test
  public void testTimeTypeConversions() {
    // UTC
    assertEquals(UTC_TIMESTAMP_VALUE, UTC_TIMESTAMP.toInputType(UTC_ROW));
    assertEquals(UTC_ROW, UTC_TIMESTAMP.toBaseType(UTC_TIMESTAMP_VALUE));
    assertEquals(UTC_TIME_ONLY_VALUE, UTC_TIME_ONLY.toInputType(UTC_ROW));
    assertEquals(UTC_ROW, UTC_TIME_ONLY.toBaseType(UTC_TIME_ONLY_VALUE));

    // Time Zone
    assertEquals(TZ_TIMESTAMP_VALUE, TZ_TIMESTAMP.toInputType(TZ_ROW));
    assertEquals(TZ_ROW, TZ_TIMESTAMP.toBaseType(TZ_TIMESTAMP_VALUE));
    assertEquals(TZ_TIME_ONLY_VALUE, TZ_TIME_ONLY.toInputType(TZ_ROW));
    assertEquals(TZ_ROW, TZ_TIME_ONLY.toBaseType(TZ_TIME_ONLY_VALUE));

    // Date
    assertEquals(LOCAL_DATE, UTC_DATE_ONLY.toInputType(DAYS_SINCE_UNIX_EPOCH));
    assertEquals(DAYS_SINCE_UNIX_EPOCH, (int) UTC_DATE_ONLY.toBaseType(LOCAL_DATE));
    assertEquals(LOCAL_DATE, LOCAL_MKT_DATE.toInputType(DAYS_SINCE_UNIX_EPOCH));
    assertEquals(DAYS_SINCE_UNIX_EPOCH, (int) LOCAL_MKT_DATE.toBaseType(LOCAL_DATE));
  }
}
