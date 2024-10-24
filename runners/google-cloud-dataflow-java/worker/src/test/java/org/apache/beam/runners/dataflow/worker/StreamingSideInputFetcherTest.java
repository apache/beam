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
package org.apache.beam.runners.dataflow.worker;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputState;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link StreamingSideInputFetcher}. */
@RunWith(JUnit4.class)
public class StreamingSideInputFetcherTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  private static final FixedWindows WINDOW_FN = FixedWindows.of(Duration.millis(10));

  static TupleTag<String> mainOutputTag = new TupleTag<>();
  @Mock StreamingModeExecutionContext execContext;
  @Mock StreamingModeExecutionContext.StepContext stepContext;
  @Mock SideInputReader mockSideInputReader;

  private final InMemoryStateInternals<String> state = InMemoryStateInternals.forKey("dummyKey");

  // Suppressing the rawtype cast to StateInternals. Because Mockito causes a covariant ?
  // to become a contravariant ?, it is not possible to cast state to an appropriate type
  // without rawtypes.
  @SuppressWarnings({
    "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
    "unchecked"
  })
  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(stepContext.stateInternals()).thenReturn((StateInternals) state);
  }

  @Test
  public void testStoreIfBlocked() throws Exception {
    PCollectionView<String> view = createView();

    IntervalWindow readyWindow = createWindow(0);
    Windmill.GlobalDataId id =
        Windmill.GlobalDataId.newBuilder()
            .setTag(view.getTagInternal().getId())
            .setVersion(
                ByteString.copyFrom(
                    CoderUtils.encodeToByteArray(IntervalWindow.getCoder(), readyWindow)))
            .build();

    when(stepContext.getSideInputNotifications())
        .thenReturn(Arrays.<Windmill.GlobalDataId>asList(id));
    when(stepContext.issueSideInputFetch(
            eq(view), any(BoundedWindow.class), eq(SideInputState.UNKNOWN)))
        .thenReturn(false);
    when(stepContext.issueSideInputFetch(
            eq(view), any(BoundedWindow.class), eq(SideInputState.KNOWN_READY)))
        .thenReturn(true);

    StreamingSideInputFetcher<String, IntervalWindow> fetcher = createFetcher(Arrays.asList(view));

    // Verify storeIfBlocked
    WindowedValue<String> datum1 = createDatum("e1", 0);
    assertTrue(fetcher.storeIfBlocked(datum1));
    assertThat(fetcher.getBlockedWindows(), Matchers.contains(createWindow(0)));

    WindowedValue<String> datum2 = createDatum("e2", 0);
    assertTrue(fetcher.storeIfBlocked(datum2));
    assertThat(fetcher.getBlockedWindows(), Matchers.contains(createWindow(0)));

    WindowedValue<String> datum3 = createDatum("e3", 10);
    assertTrue(fetcher.storeIfBlocked(datum3));
    assertThat(
        fetcher.getBlockedWindows(),
        Matchers.containsInAnyOrder(createWindow(0), createWindow(10)));

    TimerData timer1 = createTimer(0);
    assertTrue(fetcher.storeIfBlocked(timer1));

    TimerData timer2 = createTimer(15);
    assertTrue(fetcher.storeIfBlocked(timer2));

    // Verify ready windows
    assertThat(fetcher.getReadyWindows(), Matchers.contains(readyWindow));
    Set<WindowedValue<String>> actualElements = Sets.newHashSet();
    for (BagState<WindowedValue<String>> bag :
        fetcher.prefetchElements(ImmutableList.of(readyWindow))) {
      for (WindowedValue<String> elem : bag.read()) {
        actualElements.add(elem);
      }
      bag.clear();
    }
    assertThat(actualElements, Matchers.containsInAnyOrder(datum1, datum2));

    Set<TimerData> actualTimers = Sets.newHashSet();
    for (BagState<TimerData> bag : fetcher.prefetchTimers(ImmutableList.of(readyWindow))) {
      for (TimerData timer : bag.read()) {
        actualTimers.add(timer);
      }
      bag.clear();
    }
    assertThat(actualTimers, Matchers.contains(timer1));

    // Verify releaseBlockedWindows
    fetcher.releaseBlockedWindows(ImmutableList.of(readyWindow));
    assertThat(fetcher.getBlockedWindows(), Matchers.contains(createWindow(10)));

    // Verify rest elements and timers
    Set<WindowedValue<String>> restElements = Sets.newHashSet();
    for (BagState<WindowedValue<String>> bag :
        fetcher.prefetchElements(ImmutableList.of(createWindow(10), createWindow(15)))) {
      for (WindowedValue<String> elem : bag.read()) {
        restElements.add(elem);
      }
    }
    assertThat(restElements, Matchers.contains(datum3));

    Set<TimerData> restTimers = Sets.newHashSet();
    for (BagState<TimerData> bag :
        fetcher.prefetchTimers(ImmutableList.of(createWindow(10), createWindow(15)))) {
      for (TimerData timer : bag.read()) {
        restTimers.add(timer);
      }
    }
    assertThat(restTimers, Matchers.contains(timer2));
  }

  private <ReceiverT> StreamingSideInputFetcher<String, IntervalWindow> createFetcher(
      List<PCollectionView<String>> views) throws Exception {
    @SuppressWarnings({"unchecked", "rawtypes"})
    Iterable<PCollectionView<?>> typedViews = (Iterable) views;

    return new StreamingSideInputFetcher<String, IntervalWindow>(
        typedViews, StringUtf8Coder.of(), WindowingStrategy.of(WINDOW_FN), stepContext);
  }

  private PCollectionView<String> createView() {
    return TestPipeline.create()
        .apply(Create.empty(StringUtf8Coder.of()))
        .apply(Window.<String>into(WINDOW_FN))
        .apply(View.<String>asSingleton());
  }

  private WindowedValue<String> createDatum(String element, long timestamp) {
    return WindowedValue.of(
        element,
        new Instant(timestamp),
        Arrays.asList(createWindow(timestamp)),
        PaneInfo.NO_FIRING);
  }

  private TimerData createTimer(long timestamp) {
    return TimerData.of(
        StateNamespaces.window(IntervalWindow.getCoder(), createWindow(timestamp)),
        new Instant(timestamp),
        new Instant(timestamp),
        TimeDomain.EVENT_TIME);
  }

  private IntervalWindow createWindow(long timestamp) {
    return new IntervalWindow(
        new Instant(timestamp - timestamp % 10), new Instant(timestamp - timestamp % 10 + 10));
  }
}
