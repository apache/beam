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
import java.math.BigDecimal;
import java.util.Map;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DurationFieldType;

/** Utils to convert between Calcite's TimeUnit and Sql intervals. */
public abstract class TimeUnitUtils {
  /** supported interval and duration type. */
  public static final Map<SqlTypeName, DurationFieldType> INTERVALS_DURATIONS_TYPES =
      ImmutableMap.<SqlTypeName, DurationFieldType>builder()
          .put(SqlTypeName.INTERVAL_SECOND, DurationFieldType.seconds())
          .put(SqlTypeName.INTERVAL_MINUTE, DurationFieldType.minutes())
          .put(SqlTypeName.INTERVAL_HOUR, DurationFieldType.hours())
          .put(SqlTypeName.INTERVAL_DAY, DurationFieldType.days())
          .put(SqlTypeName.INTERVAL_MONTH, DurationFieldType.months())
          .put(SqlTypeName.INTERVAL_YEAR, DurationFieldType.years())
          .build();

  /**
   * @return internal multiplier of a TimeUnit, e.g. YEAR is 12, MINUTE is 60000
   * @throws IllegalArgumentException if interval type is not supported
   */
  public static BigDecimal timeUnitInternalMultiplier(final SqlTypeName sqlIntervalType) {
    switch (sqlIntervalType) {
      case INTERVAL_SECOND:
        return TimeUnit.SECOND.multiplier;
      case INTERVAL_MINUTE:
        return TimeUnit.MINUTE.multiplier;
      case INTERVAL_HOUR:
        return TimeUnit.HOUR.multiplier;
      case INTERVAL_DAY:
        return TimeUnit.DAY.multiplier;
      case INTERVAL_MONTH:
        return TimeUnit.MONTH.multiplier;
      case INTERVAL_YEAR:
        return TimeUnit.YEAR.multiplier;
      default:
        throw new IllegalArgumentException(
            "Interval " + sqlIntervalType + " cannot be converted to TimeUnit");
    }
  }
}
