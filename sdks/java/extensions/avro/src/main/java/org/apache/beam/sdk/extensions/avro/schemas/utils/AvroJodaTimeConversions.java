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
package org.apache.beam.sdk.extensions.avro.schemas.utils;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

/**
 * Avro 1.8 & 1.9 ship joda time conversions.
 *
 * <p>Since avro 1.10, only java time conversions are included. As beam is still joda time based,
 * and user may work with avro joda time generated classes, Provide joda time logical conversions.
 *
 * <p>This code is copied from avro 1.8.2 TimeConversions.
 */
public class AvroJodaTimeConversions {

  public static class DateConversion extends Conversion<LocalDate> {
    private static final LocalDate EPOCH_DATE = new LocalDate(1970, 1, 1);

    @Override
    public Class<LocalDate> getConvertedType() {
      return LocalDate.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "date";
    }

    @Override
    public LocalDate fromInt(Integer daysFromEpoch, Schema schema, LogicalType type) {
      return EPOCH_DATE.plusDays(daysFromEpoch);
    }

    @Override
    public Integer toInt(LocalDate date, Schema schema, LogicalType type) {
      return Days.daysBetween(EPOCH_DATE, date).getDays();
    }
  }

  public static class TimeConversion extends Conversion<LocalTime> {
    @Override
    public Class<LocalTime> getConvertedType() {
      return LocalTime.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "time-millis";
    }

    @Override
    public LocalTime fromInt(Integer millisFromMidnight, Schema schema, LogicalType type) {
      return LocalTime.fromMillisOfDay(millisFromMidnight);
    }

    @Override
    public Integer toInt(LocalTime time, Schema schema, LogicalType type) {
      return time.millisOfDay().get();
    }
  }

  public static class TimeMicrosConversion extends Conversion<LocalTime> {
    @Override
    public Class<LocalTime> getConvertedType() {
      return LocalTime.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "time-micros";
    }

    @Override
    public LocalTime fromLong(Long microsFromMidnight, Schema schema, LogicalType type) {
      return LocalTime.fromMillisOfDay(microsFromMidnight / 1000);
    }
  }

  public static class LossyTimeMicrosConversion extends TimeMicrosConversion {
    @Override
    public Long toLong(LocalTime time, Schema schema, LogicalType type) {
      return 1000 * (long) time.millisOfDay().get();
    }
  }

  public static class TimestampConversion extends Conversion<DateTime> {
    @Override
    public Class<DateTime> getConvertedType() {
      return DateTime.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "timestamp-millis";
    }

    @Override
    public DateTime fromLong(Long millisFromEpoch, Schema schema, LogicalType type) {
      return new DateTime(millisFromEpoch, DateTimeZone.UTC);
    }

    @Override
    public Long toLong(DateTime timestamp, Schema schema, LogicalType type) {
      return timestamp.getMillis();
    }
  }

  public static class TimestampMicrosConversion extends Conversion<DateTime> {
    @Override
    public Class<DateTime> getConvertedType() {
      return DateTime.class;
    }

    @Override
    public String getLogicalTypeName() {
      return "timestamp-micros";
    }

    @Override
    public DateTime fromLong(Long microsFromEpoch, Schema schema, LogicalType type) {
      return new DateTime(microsFromEpoch / 1000, DateTimeZone.UTC);
    }
  }

  public static class LossyTimestampMicrosConversion extends TimestampMicrosConversion {
    @Override
    public Long toLong(DateTime timestamp, Schema schema, LogicalType type) {
      return 1000 * timestamp.getMillis();
    }
  }
}
