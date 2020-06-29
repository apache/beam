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
package org.apache.beam.sdk.io.snowflake.test.unit.data;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeDate;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeDateTime;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTime;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestamp;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestampLTZ;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestampNTZ;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestampTZ;
import org.junit.Test;

public class SnowflakeDateTimeTest {
  @Test
  public void testDate() {
    SnowflakeDate date = SnowflakeDate.of();

    assertEquals("DATE", date.sql());
  }

  @Test
  public void testDateTime() {
    SnowflakeDateTime dateTime = SnowflakeDateTime.of();

    assertEquals("TIMESTAMP_NTZ", dateTime.sql());
  }

  @Test
  public void testTime() {
    SnowflakeTime time = SnowflakeTime.of();

    assertEquals("TIME", time.sql());
  }

  @Test
  public void testTimestamp() {
    SnowflakeTimestamp timestamp = SnowflakeTimestamp.of();

    assertEquals("TIMESTAMP_NTZ", timestamp.sql());
  }

  @Test
  public void testTimestampNTZ() {
    SnowflakeTimestampNTZ timestamp = SnowflakeTimestampNTZ.of();

    assertEquals("TIMESTAMP_NTZ", timestamp.sql());
  }

  @Test
  public void testTimestampLTZ() {
    SnowflakeTimestampLTZ timestamp = SnowflakeTimestampLTZ.of();

    assertEquals("TIMESTAMP_LTZ", timestamp.sql());
  }

  @Test
  public void testTimestampTZ() {
    SnowflakeTimestampTZ timestamp = SnowflakeTimestampTZ.of();

    assertEquals("TIMESTAMP_TZ", timestamp.sql());
  }
}
