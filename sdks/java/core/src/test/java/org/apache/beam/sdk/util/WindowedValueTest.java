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
package org.apache.beam.sdk.util;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for {@link WindowedValue}. */
@RunWith(JUnit4.class)
public class WindowedValueTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testWindowedValueCoder() throws CoderException {
    Instant timestamp = new Instant(1234);
    WindowedValue<String> value =
        WindowedValue.of(
            "abc",
            new Instant(1234),
            Arrays.asList(
                new IntervalWindow(timestamp, timestamp.plus(1000)),
                new IntervalWindow(timestamp.plus(1000), timestamp.plus(2000))),
            PaneInfo.NO_FIRING);

    Coder<WindowedValue<String>> windowedValueCoder =
        WindowedValue.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder());

    byte[] encodedValue = CoderUtils.encodeToByteArray(windowedValueCoder, value);
    WindowedValue<String> decodedValue =
        CoderUtils.decodeFromByteArray(windowedValueCoder, encodedValue);

    Assert.assertEquals(value.getValue(), decodedValue.getValue());
    Assert.assertEquals(value.getTimestamp(), decodedValue.getTimestamp());
    Assert.assertArrayEquals(value.getWindows().toArray(), decodedValue.getWindows().toArray());
  }

  @Test
  public void testFullWindowedValueCoderIsSerializableWithWellKnownCoderType() {
    CoderProperties.coderSerializable(
        WindowedValue.getFullCoder(GlobalWindow.Coder.INSTANCE, GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testParamWindowedValueCoderIsSerializableWithWellKnownCoderType() {
    CoderProperties.coderSerializable(
        WindowedValue.getParamWindowedValueCoder(GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testValueOnlyWindowedValueCoderIsSerializableWithWellKnownCoderType() {
    CoderProperties.coderSerializable(WindowedValue.getValueOnlyCoder(GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testExplodeWindowsInNoWindowsCrash() {
    thrown.expect(IllegalArgumentException.class);
    WindowedValue.of("foo", Instant.now(), ImmutableList.of(), PaneInfo.NO_FIRING);
  }

  @Test
  public void testExplodeWindowsInOneWindowEquals() {
    Instant now = Instant.now();
    BoundedWindow window = new IntervalWindow(now.minus(1000L), now.plus(1000L));
    WindowedValue<String> value =
        WindowedValue.of("foo", now, window, PaneInfo.ON_TIME_AND_ONLY_FIRING);

    assertThat(Iterables.getOnlyElement(value.explodeWindows()), equalTo(value));
  }

  @Test
  public void testExplodeWindowsManyWindowsMultipleWindowedValues() {
    Instant now = Instant.now();
    BoundedWindow centerWindow = new IntervalWindow(now.minus(1000L), now.plus(1000L));
    BoundedWindow pastWindow = new IntervalWindow(now.minus(1500L), now.plus(500L));
    BoundedWindow futureWindow = new IntervalWindow(now.minus(500L), now.plus(1500L));
    BoundedWindow futureFutureWindow = new IntervalWindow(now, now.plus(2000L));
    PaneInfo pane = PaneInfo.createPane(false, false, Timing.ON_TIME, 3L, 0L);
    WindowedValue<String> value =
        WindowedValue.of(
            "foo",
            now,
            ImmutableList.of(pastWindow, centerWindow, futureWindow, futureFutureWindow),
            pane);

    assertThat(
        value.explodeWindows(),
        containsInAnyOrder(
            WindowedValue.of("foo", now, futureFutureWindow, pane),
            WindowedValue.of("foo", now, futureWindow, pane),
            WindowedValue.of("foo", now, centerWindow, pane),
            WindowedValue.of("foo", now, pastWindow, pane)));

    assertThat(value.isSingleWindowedValue(), equalTo(false));
  }

  @Test
  public void testSingleWindowedValueInGlobalWindow() {
    WindowedValue<Integer> value =
        WindowedValue.of(1, Instant.now(), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);
    assertThat(value.isSingleWindowedValue(), equalTo(true));
    assertThat(
        ((WindowedValue.SingleWindowedValue) value).getWindow(), equalTo(GlobalWindow.INSTANCE));
  }

  @Test
  public void testSingleWindowedValueInFixedWindow() {
    Instant now = Instant.now();
    BoundedWindow w = new IntervalWindow(now, now.plus(1));
    WindowedValue<Integer> value = WindowedValue.of(1, now, w, PaneInfo.NO_FIRING);
    assertThat(value.isSingleWindowedValue(), equalTo(true));
    assertThat(((WindowedValue.SingleWindowedValue) value).getWindow(), equalTo(w));
  }
}
