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
package org.apache.beam.sdk.values;

import static org.junit.Assert.assertEquals;

import com.google.common.testing.EqualsTester;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link TimestampedValue}. */
@RunWith(JUnit4.class)
public class TimestampedValueTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testValues() {
    Instant now = Instant.now();
    TimestampedValue<String> tsv = TimestampedValue.of("foobar", now);

    assertEquals(now, tsv.getTimestamp());
    assertEquals("foobar", tsv.getValue());
  }

  @Test
  public void testAtMinimumTimestamp() {
    TimestampedValue<String> tsv = TimestampedValue.atMinimumTimestamp("foobar");
    assertEquals(BoundedWindow.TIMESTAMP_MIN_VALUE, tsv.getTimestamp());
  }

  @Test
  public void testNullTimestamp() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("timestamp");
    TimestampedValue.of("foobar", null);
  }

  @Test
  public void testNullValue() {
    TimestampedValue<String> tsv = TimestampedValue.atMinimumTimestamp(null);
    assertEquals(null, tsv.getValue());
  }

  @Test
  public void testEquality() {
    new EqualsTester()
        .addEqualityGroup(
            TimestampedValue.of("foo", new Instant(1000)),
            TimestampedValue.of("foo", new Instant(1000)))
        .addEqualityGroup(TimestampedValue.of("foo", new Instant(2000)))
        .addEqualityGroup(TimestampedValue.of("bar", new Instant(1000)))
        .addEqualityGroup(
            TimestampedValue.of("foo", BoundedWindow.TIMESTAMP_MIN_VALUE),
            TimestampedValue.atMinimumTimestamp("foo"))
        .testEquals();
  }

  private static final Coder<TimestampedValue<GlobalWindow>> CODER =
      TimestampedValue.TimestampedValueCoder.of(GlobalWindow.Coder.INSTANCE);

  @Test
  public void testCoderEncodeDecodeEquals() throws Exception {
    CoderProperties.coderDecodeEncodeEqual(
        CODER, TimestampedValue.of(GlobalWindow.INSTANCE, Instant.now()));
  }

  @Test
  public void testCoderIsSerializableWithWellKnownCoderType() {
    CoderProperties.coderSerializable(CODER);
  }
}
