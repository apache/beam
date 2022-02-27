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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import java.math.BigDecimal;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.util.TimestampGenerator;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitQuickcheck.class)
public class TimestampUtilsTest {
  @Property
  public void testTimestampToNanosAlwaysReturnsZeroOrPositiveNumber(
      @From(TimestampGenerator.class) Timestamp timestamp) {
    assertTrue(TimestampUtils.toNanos(timestamp).signum() >= 0);
  }

  @Test
  public void testToNanosConvertTimestampMin() {
    assertEquals(BigDecimal.valueOf(0L), TimestampUtils.toNanos(Timestamp.MIN_VALUE));
  }

  @Test
  public void testToNanosConvertTimestampMaxToNanos() {
    assertEquals(
        new BigDecimal("315537897599999999999"), TimestampUtils.toNanos(Timestamp.MAX_VALUE));
  }

  @Test
  public void testToNanosConvertTimestampToNanos() {
    assertEquals(
        new BigDecimal("62135596810000000009"),
        TimestampUtils.toNanos(Timestamp.ofTimeSecondsAndNanos(10L, 9)));
  }

  @Test
  public void testToTimestampConvertNanosToTimestampMin() {
    assertEquals(Timestamp.MIN_VALUE, TimestampUtils.toTimestamp(BigDecimal.valueOf(0L)));
  }

  @Test
  public void testToTimestampConvertNanosToTimestampMax() {
    assertEquals(
        Timestamp.MAX_VALUE, TimestampUtils.toTimestamp(new BigDecimal("315537897599999999999")));
  }

  @Test
  public void testToTimestampConvertNanosToTimestamp() {
    assertEquals(
        Timestamp.ofTimeSecondsAndNanos(10L, 9),
        TimestampUtils.toTimestamp(new BigDecimal("62135596810000000009")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testToTimestampThrowsExceptionWhenThereIsAnUnderflow() {
    TimestampUtils.toTimestamp(BigDecimal.valueOf(-1L));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testToTimestampThrowsExceptionWhenThereIsAnOverflow() {
    TimestampUtils.toTimestamp(new BigDecimal("315537897600000000000"));
  }

  @Test
  public void testNextReturnsMaxWhenTimestampIsAlreadyMax() {
    assertEquals(Timestamp.MAX_VALUE, TimestampUtils.next(Timestamp.MAX_VALUE));
  }

  @Test
  public void testNextIncrementsNanosWhenPossible() {
    assertEquals(
        Timestamp.ofTimeSecondsAndNanos(10L, 999999999),
        TimestampUtils.next(Timestamp.ofTimeSecondsAndNanos(10L, 999999998)));
  }

  @Test
  public void testNextIncrementsSecondsWhenNanosOverflow() {
    assertEquals(
        Timestamp.ofTimeSecondsAndNanos(11L, 0),
        TimestampUtils.next(Timestamp.ofTimeSecondsAndNanos(10L, 999999999)));
  }

  @Test
  public void testPreviousReturnsMinWhenTimestampIsAlreadyMin() {
    assertEquals(Timestamp.MIN_VALUE, TimestampUtils.previous(Timestamp.MIN_VALUE));
  }

  @Test
  public void testPreviousDecrementsNanosWhenPossible() {
    assertEquals(
        Timestamp.ofTimeSecondsAndNanos(10L, 0),
        TimestampUtils.previous(Timestamp.ofTimeSecondsAndNanos(10L, 1)));
  }

  @Test
  public void testPreviousDecrementsSecondsWhenNanosUnderflow() {
    assertEquals(
        Timestamp.ofTimeSecondsAndNanos(9L, 999999999),
        TimestampUtils.previous(Timestamp.ofTimeSecondsAndNanos(10L, 0)));
  }
}
