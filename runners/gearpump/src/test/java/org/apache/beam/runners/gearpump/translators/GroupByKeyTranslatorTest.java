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

import io.gearpump.streaming.dsl.window.api.WindowFunction;
import io.gearpump.streaming.dsl.window.impl.Window;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import org.apache.beam.runners.gearpump.translators.GroupByKeyTranslator.GearpumpWindowFn;
import org.apache.beam.runners.gearpump.translators.utils.TranslatorUtils;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Tests for {@link GroupByKeyTranslator}. */
@RunWith(Parameterized.class)
public class GroupByKeyTranslatorTest {

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testGearpumpWindowFn() {
    GearpumpWindowFn windowFn = new GearpumpWindowFn(true);
    List<BoundedWindow> windows =
        Lists.newArrayList(
            new IntervalWindow(new org.joda.time.Instant(0), new org.joda.time.Instant(10)),
            new IntervalWindow(new org.joda.time.Instant(5), new org.joda.time.Instant(15)));

    WindowFunction.Context<WindowedValue<String>> context =
        new WindowFunction.Context<WindowedValue<String>>() {
          @Override
          public Instant timestamp() {
            return Instant.EPOCH;
          }

          @Override
          public WindowedValue<String> element() {
            return WindowedValue.of(
                "v1", new org.joda.time.Instant(6), windows, PaneInfo.NO_FIRING);
          }
        };

    Window[] result = windowFn.apply(context);
    List<Window> expected = Lists.newArrayList();
    for (BoundedWindow w : windows) {
      expected.add(TranslatorUtils.boundedWindowToGearpumpWindow(w));
    }
    assertThat(result, equalTo(expected.toArray()));
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable<TimestampCombiner> data() {
    return ImmutableList.of(
        TimestampCombiner.EARLIEST, TimestampCombiner.LATEST, TimestampCombiner.END_OF_WINDOW);
  }

  @Parameterized.Parameter(0)
  public TimestampCombiner timestampCombiner;

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testKeyedByTimestamp() {
    WindowFn slidingWindows = Sessions.withGapDuration(Duration.millis(10));
    BoundedWindow window =
        new IntervalWindow(new org.joda.time.Instant(0), new org.joda.time.Instant(10));
    GroupByKeyTranslator.KeyedByTimestamp keyedByTimestamp =
        new GroupByKeyTranslator.KeyedByTimestamp(slidingWindows, timestampCombiner);
    WindowedValue<KV<String, String>> value =
        WindowedValue.of(
            KV.of("key", "val"), org.joda.time.Instant.now(), window, PaneInfo.NO_FIRING);
    KV<org.joda.time.Instant, WindowedValue<KV<String, String>>> result =
        keyedByTimestamp.map(value);
    org.joda.time.Instant time =
        timestampCombiner.assign(
            window, slidingWindows.getOutputTime(value.getTimestamp(), window));
    assertThat(result, equalTo(KV.of(time, value)));
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testMerge() {
    WindowFn slidingWindows = Sessions.withGapDuration(Duration.millis(10));
    GroupByKeyTranslator.Merge merge =
        new GroupByKeyTranslator.Merge(slidingWindows, timestampCombiner);
    org.joda.time.Instant key1 = new org.joda.time.Instant(5);
    WindowedValue<KV<String, String>> value1 =
        WindowedValue.of(
            KV.of("key1", "value1"),
            key1,
            new IntervalWindow(new org.joda.time.Instant(5), new org.joda.time.Instant(10)),
            PaneInfo.NO_FIRING);

    org.joda.time.Instant key2 = new org.joda.time.Instant(10);
    WindowedValue<KV<String, String>> value2 =
        WindowedValue.of(
            KV.of("key2", "value2"),
            key2,
            new IntervalWindow(new org.joda.time.Instant(9), new org.joda.time.Instant(14)),
            PaneInfo.NO_FIRING);

    KV<org.joda.time.Instant, WindowedValue<KV<String, List<String>>>> result1 =
        merge.fold(
            KV.<org.joda.time.Instant, WindowedValue<KV<String, List<String>>>>of(null, null),
            KV.of(key1, value1));
    assertThat(result1.getKey(), equalTo(key1));
    assertThat(result1.getValue().getValue().getValue(), equalTo(Lists.newArrayList("value1")));

    KV<org.joda.time.Instant, WindowedValue<KV<String, List<String>>>> result2 =
        merge.fold(result1, KV.of(key2, value2));
    assertThat(result2.getKey(), equalTo(timestampCombiner.combine(key1, key2)));
    Collection<? extends BoundedWindow> resultWindows = result2.getValue().getWindows();
    assertThat(resultWindows.size(), equalTo(1));
    IntervalWindow expectedWindow =
        new IntervalWindow(new org.joda.time.Instant(5), new org.joda.time.Instant(14));
    assertThat(resultWindows.toArray()[0], equalTo(expectedWindow));
    assertThat(
        result2.getValue().getValue().getValue(), equalTo(Lists.newArrayList("value1", "value2")));
  }
}
