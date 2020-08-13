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
package org.apache.beam.sdk.extensions.sql.utils;

import java.time.Instant;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

/** DateTimeUtils. */
public class DateTimeUtils {

  public static Instant parseTimeStampWithoutTimeZone(String str) {
    return Instant.parse(str);
  }

  public static DateTime parseTimestampWithUTCTimeZone(String str) {
    if (str.indexOf('.') == -1) {
      return DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC().parseDateTime(str);
    } else {
      return DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZoneUTC().parseDateTime(str);
    }
  }

  public static DateTime parseTimestampWithTimeZone(String str) {
    // for example, accept "1990-10-20 13:24:01+0730"
    return DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssZ").parseDateTime(str);
  }

  public static DateTime parseTimestampWithoutTimeZone(String str) {
    return DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").parseDateTime(str);
  }

  public static DateTime parseDate(String str) {
    return DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC().parseDateTime(str);
  }

  public static DateTime parseTime(String str) {
    // DateTimeFormat does not parse "08:10:10" for pattern "HH:mm:ss.SSS". In this case, '.' must
    // appear.
    if (str.indexOf('.') == -1) {
      return DateTimeFormat.forPattern("HH:mm:ss").withZoneUTC().parseDateTime(str);
    } else {
      return DateTimeFormat.forPattern("HH:mm:ss.SSS").withZoneUTC().parseDateTime(str);
    }
  }
}
