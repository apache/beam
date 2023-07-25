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

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import org.apache.beam.sdk.extensions.sbe.SbeLogicalTypes.LocalMktDate;
import org.apache.beam.sdk.extensions.sbe.SbeLogicalTypes.TZTimeOnly;
import org.apache.beam.sdk.extensions.sbe.SbeLogicalTypes.TZTimestamp;
import org.apache.beam.sdk.extensions.sbe.SbeLogicalTypes.UTCDateOnly;
import org.apache.beam.sdk.extensions.sbe.SbeLogicalTypes.UTCTimeOnly;
import org.apache.beam.sdk.extensions.sbe.SbeLogicalTypes.UTCTimestamp;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class SbeLogicalTypesTest {
  @Test
  public void testUtcTimestamp() {
    Instant instant = Instant.now();
    UTCTimestamp timestamp = new UTCTimestamp();

    Instant afterConversions = timestamp.toInputType(timestamp.toBaseType(instant));

    assertEquals(instant, afterConversions);
  }

  @Test
  public void testTzTimestamp() {
    OffsetDateTime dateTime = OffsetDateTime.now(ZoneId.systemDefault());
    TZTimestamp timestamp = new TZTimestamp();

    OffsetDateTime afterConversions = timestamp.toInputType(timestamp.toBaseType(dateTime));

    assertEquals(dateTime, afterConversions);
  }

  @Test
  public void testUtcTimeOnly() {
    LocalTime time = LocalTime.now();
    UTCTimeOnly timeOnly = new UTCTimeOnly();

    LocalTime afterConversions = timeOnly.toInputType(timeOnly.toBaseType(time));

    assertEquals(time, afterConversions);
  }

  @Test
  public void testTzTimeOnly() {
    OffsetTime time = OffsetTime.now(ZoneId.systemDefault());
    TZTimeOnly timeOnly = new TZTimeOnly();

    OffsetTime afterConversions = timeOnly.toInputType(timeOnly.toBaseType(time));

    assertEquals(time, afterConversions);
  }

  @Test
  public void testUtcDateOnly() {
    LocalDate date = LocalDate.now();
    UTCDateOnly dateOnly = new UTCDateOnly();

    LocalDate afterConversions = dateOnly.toInputType(dateOnly.toBaseType(date));

    assertEquals(date, afterConversions);
  }

  @Test
  public void testLocalMktDate() {
    LocalDate date = LocalDate.now();
    LocalMktDate localMktDate = new LocalMktDate();

    LocalDate afterConversions = localMktDate.toInputType(localMktDate.toBaseType(date));

    assertEquals(date, afterConversions);
  }
}
