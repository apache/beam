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
package org.apache.beam.sdk.extensions.sql.zetasql.translation.impl;

import java.util.TimeZone;
import org.apache.beam.sdk.extensions.sql.zetasql.DateTimeUtils;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.linq4j.function.Strict;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/** TimestampFunctions. */
public class TimestampFunctions {
  public static DateTime timestamp(String timestampStr) {
    return timestamp(timestampStr, "UTC");
  }

  @Strict
  public static DateTime timestamp(String timestampStr, String timezone) {
    return DateTimeUtils.findDateTimePattern(timestampStr)
        .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone(timezone)))
        .parseDateTime(timestampStr);
  }

  @Strict
  public static DateTime timestamp(Integer numOfDays) {
    return timestamp(numOfDays, "UTC");
  }

  @Strict
  public static DateTime timestamp(Integer numOfDays, String timezone) {
    return new DateTime((long) numOfDays * DateTimeUtils.MILLIS_PER_DAY, DateTimeZone.UTC)
        .withZoneRetainFields(DateTimeZone.forTimeZone(TimeZone.getTimeZone(timezone)));
  }
}
