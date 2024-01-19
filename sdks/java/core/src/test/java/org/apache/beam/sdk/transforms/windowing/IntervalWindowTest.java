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
package org.apache.beam.sdk.transforms.windowing;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DurationCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.CoderProperties.TestElementByteSizeObserver;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Window}. */
@RunWith(JUnit4.class)
public class IntervalWindowTest {

  private static final Coder<IntervalWindow> TEST_CODER = IntervalWindow.getCoder();

  private static final List<IntervalWindow> TEST_VALUES =
      Lists.newArrayList(
          new IntervalWindow(new Instant(0), new Instant(0)),
          new IntervalWindow(new Instant(0), new Instant(1000)),
          new IntervalWindow(new Instant(-1000), new Instant(735)),
          new IntervalWindow(new Instant(350), new Instant(60 * 60 * 1000)),
          new IntervalWindow(new Instant(0), new Instant(24 * 60 * 60 * 1000)),
          new IntervalWindow(
              Instant.parse("2015-04-01T00:00:00Z"), Instant.parse("2015-04-01T11:45:13Z")));

  @Test
  public void testBasicEncoding() throws Exception {
    for (IntervalWindow window : TEST_VALUES) {
      CoderProperties.coderDecodeEncodeEqual(TEST_CODER, window);
    }
  }

  /**
   * This is a change detector test for the sizes of encoded windows. Since these are present for
   * every element of every windowed PCollection, the size matters.
   *
   * <p>This test documents the expectation that encoding as a (endpoint, duration) pair using big
   * endian for the endpoint and variable length long for the duration should be about 25% smaller
   * than encoding two big endian Long values.
   */
  @Test
  public void testLengthsOfEncodingChoices() throws Exception {
    Instant start = Instant.parse("2015-04-01T00:00:00Z");
    Instant minuteEnd = Instant.parse("2015-04-01T00:01:00Z");
    Instant hourEnd = Instant.parse("2015-04-01T01:00:00Z");
    Instant dayEnd = Instant.parse("2015-04-02T00:00:00Z");

    Coder<Instant> instantCoder = InstantCoder.of();
    byte[] encodedStart = CoderUtils.encodeToByteArray(instantCoder, start);
    byte[] encodedMinuteEnd = CoderUtils.encodeToByteArray(instantCoder, minuteEnd);
    byte[] encodedHourEnd = CoderUtils.encodeToByteArray(instantCoder, hourEnd);
    byte[] encodedDayEnd = CoderUtils.encodeToByteArray(instantCoder, dayEnd);

    byte[] encodedMinuteWindow =
        CoderUtils.encodeToByteArray(TEST_CODER, new IntervalWindow(start, minuteEnd));
    byte[] encodedHourWindow =
        CoderUtils.encodeToByteArray(TEST_CODER, new IntervalWindow(start, hourEnd));
    byte[] encodedDayWindow =
        CoderUtils.encodeToByteArray(TEST_CODER, new IntervalWindow(start, dayEnd));

    assertThat(
        encodedMinuteWindow.length, equalTo(encodedStart.length + encodedMinuteEnd.length - 5));
    assertThat(encodedHourWindow.length, equalTo(encodedStart.length + encodedHourEnd.length - 4));
    assertThat(encodedDayWindow.length, equalTo(encodedStart.length + encodedDayEnd.length - 4));
  }

  @Test
  public void testCoderRegisterByteSizeObserver() throws Exception {
    assertThat(TEST_CODER.isRegisterByteSizeObserverCheap(TEST_VALUES.get(0)), equalTo(true));
    TestElementByteSizeObserver observer = new TestElementByteSizeObserver();
    TestElementByteSizeObserver observer2 = new TestElementByteSizeObserver();
    for (IntervalWindow window : TEST_VALUES) {
      TEST_CODER.registerByteSizeObserver(window, observer);
      InstantCoder.of().registerByteSizeObserver(window.maxTimestamp(), observer2);
      DurationCoder.of()
          .registerByteSizeObserver(new Duration(window.start(), window.end()), observer2);
      observer.advance();
      observer2.advance();
      assertThat(observer.getSumAndReset(), equalTo(observer2.getSumAndReset()));
    }
  }
}
