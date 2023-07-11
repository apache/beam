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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams;

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.TimestampConverter.microsecondToInstant;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.TimestampConverter.toJodaTime;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.TimestampConverter.toThreetenInstant;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TimestampConverterTest {
  @Test
  public void testToThreetenInstant() {
    org.joda.time.Instant jodaInstant = org.joda.time.Instant.ofEpochMilli(1_000_000_000L);
    assertEquals(1_000_000_000L, toThreetenInstant(jodaInstant).toEpochMilli());
  }

  @Test
  public void testToJodaInstant() {
    org.threeten.bp.Instant threetenInstant = org.threeten.bp.Instant.ofEpochMilli(1_000_000_000L);
    assertEquals(1_000_000_000L, toJodaTime(threetenInstant).getMillis());
  }

  @Test
  public void testToSeconds() {
    assertEquals(1, TimestampConverter.toSeconds(org.joda.time.Instant.ofEpochSecond(1)));
    assertEquals(1000, TimestampConverter.toSeconds(org.joda.time.Instant.ofEpochSecond(1000)));
  }

  @Test
  public void testMicrosecondToJodaInstant() {
    assertEquals(org.joda.time.Instant.ofEpochMilli(1_234L), microsecondToInstant(1_234_567L));
  }
}
