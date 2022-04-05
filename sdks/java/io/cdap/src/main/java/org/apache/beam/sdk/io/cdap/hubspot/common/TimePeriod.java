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
package org.apache.beam.sdk.io.cdap.hubspot.common;

import java.util.Arrays;

/**
 * Convenience enum to map TimePeriod UI selections to meaningful values. The time period used to
 * group the data. Must be one of:
 *
 * <p>total - Data is rolled up to totals covering the specified time. daily - Grouped by day weekly
 * - Grouped by week monthly - Grouped by month summarize/daily - Grouped by day, data is rolled up
 * across all breakdowns summarize/weekly - Grouped by week, data is rolled up across all breakdowns
 * summarize/monthly - Grouped by month, data is rolled up across all breakdowns
 *
 * <p>NOTE: When using daily, weekly, or monthly for the time_period, you must include at least one
 * filter
 */
@SuppressWarnings("ImmutableEnumChecker")
public enum TimePeriod {
  TOTAL("total"),
  DAILY("daily"),
  WEEKLY("weekly"),
  MONTHLY("monthly"),
  SUMMARIZE_DAILY("summarize/daily"),
  SUMMARIZE_WEEKLY("summarize/weekly"),
  SUMMARIZE_MONTHLY("summarize/monthly");

  private String stringValue;

  TimePeriod(String stringValue) {
    this.stringValue = stringValue;
  }

  /**
   * Returns the TimePeriod.
   *
   * @param value the value is string type
   * @return the TimePeriod
   */
  public static TimePeriod fromString(String value) {
    return Arrays.stream(TimePeriod.values())
        .filter(type -> type.stringValue.equals(value))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(String.format("'%s' is invalid ObjectType.", value)));
  }

  public String getStringValue() {
    return stringValue;
  }
}
