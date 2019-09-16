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
package org.apache.beam.runners.gearpump.translators;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.Iterator;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

/** Tests for {@link WindowAssignTranslator}. */
public class WindowAssignTranslatorTest {

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testAssignWindowsWithSlidingWindow() {
    WindowFn slidingWindows = SlidingWindows.of(Duration.millis(10)).every(Duration.millis(5));
    WindowAssignTranslator.AssignWindows<String> assignWindows =
        new WindowAssignTranslator.AssignWindows(slidingWindows);

    String value = "v1";
    Instant timestamp = new Instant(1);
    WindowedValue<String> windowedValue =
        WindowedValue.timestampedValueInGlobalWindow(value, timestamp);
    ArrayList<WindowedValue<String>> expected = new ArrayList<>();
    expected.add(
        WindowedValue.of(
            value,
            timestamp,
            new IntervalWindow(new Instant(0), new Instant(10)),
            PaneInfo.NO_FIRING));
    expected.add(
        WindowedValue.of(
            value,
            timestamp,
            new IntervalWindow(new Instant(-5), new Instant(5)),
            PaneInfo.NO_FIRING));

    Iterator<WindowedValue<String>> result = assignWindows.flatMap(windowedValue);
    assertThat(expected, equalTo(Lists.newArrayList(result)));
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testAssignWindowsWithSessions() {
    WindowFn slidingWindows = Sessions.withGapDuration(Duration.millis(10));
    WindowAssignTranslator.AssignWindows<String> assignWindows =
        new WindowAssignTranslator.AssignWindows(slidingWindows);

    String value = "v1";
    Instant timestamp = new Instant(1);
    WindowedValue<String> windowedValue =
        WindowedValue.timestampedValueInGlobalWindow(value, timestamp);
    ArrayList<WindowedValue<String>> expected = new ArrayList<>();
    expected.add(
        WindowedValue.of(
            value,
            timestamp,
            new IntervalWindow(new Instant(1), new Instant(11)),
            PaneInfo.NO_FIRING));

    Iterator<WindowedValue<String>> result = assignWindows.flatMap(windowedValue);
    assertThat(expected, equalTo(Lists.newArrayList(result)));
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testAssignWindowsGlobal() {
    WindowFn slidingWindows = new GlobalWindows();
    WindowAssignTranslator.AssignWindows<String> assignWindows =
        new WindowAssignTranslator.AssignWindows(slidingWindows);

    String value = "v1";
    Instant timestamp = new Instant(1);
    WindowedValue<String> windowedValue =
        WindowedValue.timestampedValueInGlobalWindow(value, timestamp);
    ArrayList<WindowedValue<String>> expected = new ArrayList<>();
    expected.add(WindowedValue.timestampedValueInGlobalWindow(value, timestamp));

    Iterator<WindowedValue<String>> result = assignWindows.flatMap(windowedValue);
    assertThat(expected, equalTo(Lists.newArrayList(result)));
  }
}
