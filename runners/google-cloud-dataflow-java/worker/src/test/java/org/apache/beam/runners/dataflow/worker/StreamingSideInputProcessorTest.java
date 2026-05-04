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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link StreamingSideInputProcessor}. */
@RunWith(JUnit4.class)
public class StreamingSideInputProcessorTest {

  @Mock private StreamingSideInputFetcher<String, IntervalWindow> mockFetcher;
  private StreamingSideInputProcessor<String, IntervalWindow> processor;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    processor = new StreamingSideInputProcessor<>(mockFetcher);
  }

  @Test
  public void testTryUnblockElementsNoReadyWindows() {
    // Given
    doNothing().when(mockFetcher).prefetchBlockedMap();
    when(mockFetcher.getReadyWindows()).thenReturn(Collections.emptySet());

    // When
    Iterable<WindowedValue<String>> unblocked = processor.tryUnblockElements();

    // Then
    assertThat(unblocked, emptyIterable());
    verify(mockFetcher).prefetchBlockedMap();
    verify(mockFetcher).getReadyWindows();
  }

  @Test
  public void testTryUnblockElementsWithReadyWindows() {
    // Given
    IntervalWindow window1 = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow window2 = new IntervalWindow(new Instant(10), new Instant(20));
    Set<IntervalWindow> readyWindows = new HashSet<>(Arrays.asList(window1, window2));

    WindowedValue<String> element1 =
        WindowedValues.of("e1", new Instant(5), Arrays.asList(window1), PaneInfo.NO_FIRING);
    WindowedValue<String> element2 =
        WindowedValues.of("e2", new Instant(15), Arrays.asList(window2), PaneInfo.NO_FIRING);

    @SuppressWarnings("unchecked")
    BagState<WindowedValue<String>> mockBag1 = mock(BagState.class);
    @SuppressWarnings("unchecked")
    BagState<WindowedValue<String>> mockBag2 = mock(BagState.class);

    when(mockBag1.read()).thenReturn(Arrays.asList(element1));
    when(mockBag2.read()).thenReturn(Arrays.asList(element2));

    doNothing().when(mockFetcher).prefetchBlockedMap();
    when(mockFetcher.getReadyWindows()).thenReturn(readyWindows);
    when(mockFetcher.prefetchElements(readyWindows)).thenReturn(Arrays.asList(mockBag1, mockBag2));
    doNothing().when(mockFetcher).releaseBlockedWindows(readyWindows);

    // When
    Iterable<WindowedValue<String>> unblocked = processor.tryUnblockElements();

    // Then
    assertThat(unblocked, containsInAnyOrder(element1, element2));
    verify(mockFetcher).prefetchBlockedMap();
    verify(mockFetcher).getReadyWindows();
    verify(mockFetcher).prefetchElements(readyWindows);
    verify(mockBag1).read();
    verify(mockBag1).clear();
    verify(mockBag2).read();
    verify(mockBag2).clear();
    verify(mockFetcher).releaseBlockedWindows(readyWindows);
  }

  @Test
  public void testHandleFinishBundle() {
    // Given
    doNothing().when(mockFetcher).persist();

    // When
    processor.handleFinishBundle();

    // Then
    verify(mockFetcher).persist();
  }

  @Test
  public void testHandleProcessElementBlocked() {
    // Given
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(10));
    WindowedValue<String> compressedElement =
        WindowedValues.of("e", new Instant(5), Arrays.asList(window), PaneInfo.NO_FIRING);

    when(mockFetcher.storeIfBlocked(any(WindowedValue.class))).thenReturn(true);

    // When
    Iterable<WindowedValue<String>> unblocked = processor.handleProcessElement(compressedElement);

    // Then
    assertThat(unblocked, emptyIterable());
    for (WindowedValue<String> exploded : compressedElement.explodeWindows()) {
      verify(mockFetcher).storeIfBlocked(exploded);
    }
  }

  @Test
  public void testHandleProcessElementUnblocked() {
    // Given
    IntervalWindow window1 = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow window2 = new IntervalWindow(new Instant(10), new Instant(20));
    WindowedValue<String> compressedElement =
        WindowedValues.of("e", new Instant(5), Arrays.asList(window1, window2), PaneInfo.NO_FIRING);

    when(mockFetcher.storeIfBlocked(any(WindowedValue.class))).thenReturn(false);

    // When
    Iterable<WindowedValue<String>> unblocked = processor.handleProcessElement(compressedElement);

    // Then
    assertThat(
        unblocked,
        containsInAnyOrder(
            Iterables.toArray(compressedElement.explodeWindows(), WindowedValue.class)));
    for (WindowedValue<String> exploded : compressedElement.explodeWindows()) {
      verify(mockFetcher).storeIfBlocked(exploded);
    }
  }

  @Test
  public void testHandleProcessTimerSuccess() {
    // Given
    TimerData mockTimer = mock(TimerData.class);
    when(mockFetcher.storeIfBlocked(mockTimer)).thenReturn(false);

    // When
    processor.handleProcessTimer(mockTimer);

    // Then
    verify(mockFetcher).storeIfBlocked(mockTimer);
  }

  @Test
  public void testHandleProcessTimerThrowsPreconditionFail() {
    // Given
    TimerData mockTimer = mock(TimerData.class);
    when(mockFetcher.storeIfBlocked(mockTimer)).thenReturn(true);

    // When & Then
    assertThrows(
        IllegalStateException.class,
        () -> {
          processor.handleProcessTimer(mockTimer);
        });
    verify(mockFetcher).storeIfBlocked(mockTimer);
  }
}
