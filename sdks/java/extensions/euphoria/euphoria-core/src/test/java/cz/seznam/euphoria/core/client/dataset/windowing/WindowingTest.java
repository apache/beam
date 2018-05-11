/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.client.dataset.windowing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Iterables;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.util.Pair;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

public class WindowingTest {

  @Test
  public void testTimeBuilders() {
    assertTimeWindowing(Time.of(Duration.ofSeconds(100)), 100 * 1000, null);

    assertTimeWindowing(
        Time.of(Duration.ofSeconds(20)).earlyTriggering(Duration.ofSeconds(10)),
        20 * 1000,
        Duration.ofSeconds(10));

    assertTimeWindowing(
        Time.of(Duration.ofSeconds(4)).earlyTriggering(Duration.ofSeconds(10)),
        4 * 1000,
        Duration.ofSeconds(10));

    assertTimeWindowing(
        Time.of(Duration.ofSeconds(3)).earlyTriggering(Duration.ofHours(1)),
        3 * 1000,
        Duration.ofHours(1));

    assertTimeWindowing(Time.of(Duration.ofSeconds(8)), 8 * 1000, null);
  }

  private <T> void assertTimeWindowing(
      Time<T> w, long expectDurationMillis, Duration expectEarlyTriggeringPeriod) {

    assertNotNull(w);
    assertEquals(expectEarlyTriggeringPeriod, w.getEarlyTriggeringPeriod());
    assertEquals(expectDurationMillis, w.getDuration());
  }

  private <W extends Window<W>, T> Iterable<W> assignWindows(
      Windowing<T, W> windowing, T elem, UnaryFunction<T, Long> eventTimeAssigner) {
    return windowing.assignWindowsToElement(
        new Elem<W, T>(null, elem, eventTimeAssigner.apply(elem)));
  }

  @Test
  public void testWindowing_SessionMergeWindows() {
    Session<Long> windowing = Session.of(Duration.ofSeconds(10));

    UnaryFunction<Long, Long> eventTimeAssigner = e -> e;

    TimeInterval w1 =
        assertSessionWindow(assignWindows(windowing, 1_000L, eventTimeAssigner), 1_000L, 11_000L);
    TimeInterval w2 =
        assertSessionWindow(assignWindows(windowing, 10_000L, eventTimeAssigner), 10_000L, 20_000L);
    TimeInterval w3 =
        assertSessionWindow(assignWindows(windowing, 21_000L, eventTimeAssigner), 21_000L, 31_000L);
    // ~ a small window which is fully contained in w1
    TimeInterval w4 = new TimeInterval(3_000L, 8_000L);

    Map<TimeInterval, TimeInterval> merges =
        windowing
            .mergeWindows(Arrays.asList(w4, w3, w2, w1))
            .stream()
            .flatMap(p -> p.getFirst().stream().map(w -> Pair.of(w, p.getSecond())))
            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));

    assertEquals(3L, merges.size());
    assertNotNull(merges.get(w1));
    assertNotNull(merges.get(w4));
    assertNotNull(merges.get(w2));

    Set<TimeInterval> target = merges.values().stream().collect(Collectors.toSet());
    assertEquals(1, target.size());
    TimeInterval targetWindow = target.iterator().next();
    assertSessionWindow(targetWindow, 1_000L, 20_000L);
  }

  private TimeInterval assertSessionWindow(
      Iterable<TimeInterval> window, long expectedStartMillis, long expectedEndMillis) {
    TimeInterval w = Iterables.getOnlyElement(window);
    assertSessionWindow(w, expectedStartMillis, expectedEndMillis);
    return w;
  }

  private void assertSessionWindow(
      TimeInterval window, long expectedStartMillis, long expectedEndMillis) {

    assertNotNull(window);
    assertEquals(expectedStartMillis, window.getStartMillis());
    assertEquals(expectedEndMillis, window.getEndMillis());
  }

  @Test
  public void testTimeSlidingLabelAssignment() {

    TimeSliding<Long> windowing = TimeSliding.of(Duration.ofHours(1), Duration.ofMinutes(20));

    UnaryFunction<Long, Long> eventTimeAssigner = e -> e * 1000L;

    long[] data = {3590, 3600, 3610, 3800, 7190, 7200, 7210};

    for (long event : data) {
      Iterable<TimeInterval> labels =
          windowing.assignWindowsToElement(
              new Elem<>(GlobalWindowing.Window.get(), event, eventTimeAssigner.apply(event)));
      // verify window count
      assertEquals(3, Iterables.size(labels));
      // verify that each window contains the original event
      for (TimeInterval l : labels) {
        long stamp = event * 1000L;
        assertTrue(stamp >= l.getStartMillis());
        assertTrue(stamp <= l.getEndMillis());
      }
    }
  }

  private static class Elem<W extends Window, T> implements WindowedElement<W, T> {
    private final W window;
    private final T element;
    private final long timestamp;

    public Elem(W window, T element, long timestamp) {
      this.window = window;
      this.element = element;
      this.timestamp = timestamp;
    }

    @Override
    public W getWindow() {
      return window;
    }

    @Override
    public long getTimestamp() {
      return timestamp;
    }

    @Override
    public T getElement() {
      return element;
    }
  }
}
