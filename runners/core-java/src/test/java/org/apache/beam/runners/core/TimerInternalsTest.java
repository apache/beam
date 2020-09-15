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
package org.apache.beam.runners.core;

import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.TimerInternals.TimerDataCoderV2;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link TimerInternals}. */
@RunWith(JUnit4.class)
public class TimerInternalsTest {

  @Test
  public void testTimerDataCoder() throws Exception {
    CoderProperties.coderDecodeEncodeEqual(
        TimerDataCoderV2.of(GlobalWindow.Coder.INSTANCE),
        TimerData.of(
            "arbitrary-id",
            StateNamespaces.global(),
            new Instant(0),
            new Instant(0),
            TimeDomain.EVENT_TIME));

    Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();
    CoderProperties.coderDecodeEncodeEqual(
        TimerDataCoderV2.of(windowCoder),
        TimerData.of(
            "another-id",
            StateNamespaces.window(
                windowCoder, new IntervalWindow(new Instant(0), new Instant(100))),
            new Instant(99),
            new Instant(99),
            TimeDomain.PROCESSING_TIME));
  }

  @Test
  public void testCoderIsSerializableWithWellKnownCoderType() {
    CoderProperties.coderSerializable(TimerDataCoderV2.of(GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testCompareEqual() {
    Instant timestamp = new Instant(100);
    StateNamespace namespace = StateNamespaces.global();
    TimerData timer = TimerData.of("id", namespace, timestamp, timestamp, TimeDomain.EVENT_TIME);

    assertThat(
        timer,
        comparesEqualTo(
            TimerData.of("id", namespace, timestamp, timestamp, TimeDomain.EVENT_TIME)));
  }

  @Test
  public void testCompareByTimestamp() {
    Instant firstTimestamp = new Instant(100);
    Instant secondTimestamp = new Instant(200);
    StateNamespace namespace = StateNamespaces.global();

    TimerData firstTimer =
        TimerData.of(namespace, firstTimestamp, firstTimestamp, TimeDomain.EVENT_TIME);
    TimerData secondTimer =
        TimerData.of(namespace, secondTimestamp, secondTimestamp, TimeDomain.EVENT_TIME);

    assertThat(firstTimer, lessThan(secondTimer));
  }

  @Test
  public void testCompareByDomain() {
    Instant timestamp = new Instant(100);
    StateNamespace namespace = StateNamespaces.global();

    TimerData eventTimer = TimerData.of(namespace, timestamp, timestamp, TimeDomain.EVENT_TIME);
    TimerData procTimer = TimerData.of(namespace, timestamp, timestamp, TimeDomain.PROCESSING_TIME);
    TimerData synchronizedProcTimer =
        TimerData.of(namespace, timestamp, timestamp, TimeDomain.SYNCHRONIZED_PROCESSING_TIME);

    assertThat(eventTimer, lessThan(procTimer));
    assertThat(eventTimer, lessThan(synchronizedProcTimer));
    assertThat(procTimer, lessThan(synchronizedProcTimer));
  }

  @Test
  public void testCompareByNamespace() {
    Instant timestamp = new Instant(100);
    IntervalWindow firstWindow = new IntervalWindow(new Instant(0), timestamp);
    IntervalWindow secondWindow = new IntervalWindow(timestamp, new Instant(200));
    Coder<IntervalWindow> windowCoder = IntervalWindow.getCoder();

    StateNamespace firstWindowNs = StateNamespaces.window(windowCoder, firstWindow);
    StateNamespace secondWindowNs = StateNamespaces.window(windowCoder, secondWindow);

    TimerData secondEventTime =
        TimerData.of(firstWindowNs, timestamp, timestamp, TimeDomain.EVENT_TIME);
    TimerData thirdEventTime =
        TimerData.of(secondWindowNs, timestamp, timestamp, TimeDomain.EVENT_TIME);

    assertThat(secondEventTime, lessThan(thirdEventTime));
  }

  @Test
  public void testCompareByTimerId() {
    Instant timestamp = new Instant(100);
    StateNamespace namespace = StateNamespaces.global();

    TimerData id0Timer =
        TimerData.of("id0", namespace, timestamp, timestamp, TimeDomain.EVENT_TIME);
    TimerData id1Timer =
        TimerData.of("id1", namespace, timestamp, timestamp, TimeDomain.EVENT_TIME);

    assertThat(id0Timer, lessThan(id1Timer));
  }
}
