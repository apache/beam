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

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;

/** Schemas for SBE composite types. */
@Experimental(Kind.SCHEMAS)
public final class Schemas {

  private Schemas() {}

  /**
   * All the field names used in SBE's time-based composite types.
   *
   * <p>Not every composite type uses all these fields, but it does cover what is necessary for the
   * following composite types:
   *
   * <ul>
   *   <li>UTCTimestamp
   *   <li>TZTimestamp
   *   <li>UTCTimeOnly
   *   <li>TZTimeOnly
   * </ul>
   */
  public static final class TimeFieldNames {
    public static final String TIME_FIELD_NAME = "time";
    public static final String UNIT_FIELD_NAME = "unit";
    public static final String TIME_ZONE_HOUR_FIELD_NAME = "timezoneHour";
    public static final String TIME_ZONE_MINUTE_FIELD_NAME = "timezoneMinute";

    private TimeFieldNames() {}
  }

  // Note: We could potentially match an SBE composite type to a schema based on field matching, but
  // that might lead to a false positive if users intended those fields under a different semantic
  // meaning, so we don't try to match to schemas with that method.

  /** Schema that covers SBE's UTC-based times types. */
  public static final Schema UTC_TIME_SCHEMA =
      Schema.builder()
          .addInt64Field(TimeFieldNames.TIME_FIELD_NAME)
          .addByteField(TimeFieldNames.UNIT_FIELD_NAME)
          .build();

  /** Schema that covers SBE's time-zone-based time types. */
  public static final Schema TZ_TIME_SCHEMA =
      Schema.builder()
          .addInt64Field(TimeFieldNames.TIME_FIELD_NAME)
          .addByteField(TimeFieldNames.UNIT_FIELD_NAME)
          .addByteField(TimeFieldNames.TIME_ZONE_HOUR_FIELD_NAME)
          .addByteField(TimeFieldNames.TIME_ZONE_MINUTE_FIELD_NAME)
          .build();
}
