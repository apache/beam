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
package org.apache.beam.sdk.testing;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.PaneInfo.Timing;
import org.apache.beam.sdk.util.WindowedValue;

import com.google.common.collect.ImmutableList;

import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link PaneExtractors}.
 */
@RunWith(JUnit4.class)
public class PaneExtractorsTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void onlyPaneNoFiring() {
    SerializableFunction<Iterable<WindowedValue<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.onlyPane();
    Iterable<WindowedValue<Integer>> noFiring =
        ImmutableList.of(
            WindowedValue.valueInGlobalWindow(9), WindowedValue.valueInEmptyWindows(19));
    assertThat(extractor.apply(noFiring), containsInAnyOrder(9, 19));
  }

  @Test
  public void onlyPaneOnlyOneFiring() {
    SerializableFunction<Iterable<WindowedValue<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.onlyPane();
    Iterable<WindowedValue<Integer>> onlyFiring =
        ImmutableList.of(
            WindowedValue.of(
                2, new Instant(0L), GlobalWindow.INSTANCE, PaneInfo.ON_TIME_AND_ONLY_FIRING),
            WindowedValue.of(
                1, new Instant(0L), GlobalWindow.INSTANCE, PaneInfo.ON_TIME_AND_ONLY_FIRING));

    assertThat(extractor.apply(onlyFiring), containsInAnyOrder(2, 1));
  }

  @Test
  public void onlyPaneMultiplePanesFails() {
    SerializableFunction<Iterable<WindowedValue<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.onlyPane();
    Iterable<WindowedValue<Integer>> multipleFiring =
        ImmutableList.of(
            WindowedValue.of(
                4,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(true, false, Timing.EARLY)),
            WindowedValue.of(
                2,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(false, false, Timing.ON_TIME, 1L, 0L)),
            WindowedValue.of(
                1,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(false, false, Timing.LATE, 2L, 1L)));

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("trigger that fires at most once");
    extractor.apply(multipleFiring);
  }

  @Test
  public void onTimePane() {
    SerializableFunction<Iterable<WindowedValue<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.onTimePane();
    Iterable<WindowedValue<Integer>> onlyOnTime =
        ImmutableList.of(
            WindowedValue.of(
                4,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(false, false, Timing.ON_TIME, 1L, 0L)),
            WindowedValue.of(
                2,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(false, false, Timing.ON_TIME, 1L, 0L)));

    assertThat(extractor.apply(onlyOnTime), containsInAnyOrder(2, 4));
  }

  @Test
  public void onTimePaneOnlyEarlyAndLate() {
    SerializableFunction<Iterable<WindowedValue<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.onTimePane();
    Iterable<WindowedValue<Integer>> onlyOnTime =
        ImmutableList.of(
            WindowedValue.of(
                8,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(false, false, Timing.LATE, 2L, 1L)),
            WindowedValue.of(
                4,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(false, false, Timing.ON_TIME, 1L, 0L)),
            WindowedValue.of(
                2,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(false, false, Timing.ON_TIME, 1L, 0L)),
            WindowedValue.of(
                1,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(true, false, Timing.EARLY)));

    assertThat(extractor.apply(onlyOnTime), containsInAnyOrder(2, 4));
  }

  @Test
  public void finalPane() {
    SerializableFunction<Iterable<WindowedValue<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.finalPane();
    Iterable<WindowedValue<Integer>> onlyOnTime =
        ImmutableList.of(
            WindowedValue.of(
                8,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(false, true, Timing.LATE, 2L, 1L)),
            WindowedValue.of(
                4,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(false, false, Timing.ON_TIME, 1L, 0L)),
            WindowedValue.of(
                1,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(true, false, Timing.EARLY)));

    assertThat(extractor.apply(onlyOnTime), containsInAnyOrder(8));
  }

  @Test
  public void finalPaneNoExplicitFinalEmpty() {
    SerializableFunction<Iterable<WindowedValue<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.finalPane();
    Iterable<WindowedValue<Integer>> onlyOnTime =
        ImmutableList.of(
            WindowedValue.of(
                8,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(false, false, Timing.LATE, 2L, 1L)),
            WindowedValue.of(
                4,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(false, false, Timing.ON_TIME, 1L, 0L)),
            WindowedValue.of(
                1,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(true, false, Timing.EARLY)));

    assertThat(extractor.apply(onlyOnTime), emptyIterable());
  }

  @Test
  public void nonLatePanesSingleOnTime() {
    SerializableFunction<Iterable<WindowedValue<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.nonLatePanes();
    Iterable<WindowedValue<Integer>> onlyOnTime =
        ImmutableList.of(
            WindowedValue.of(
                8, new Instant(0L), GlobalWindow.INSTANCE, PaneInfo.ON_TIME_AND_ONLY_FIRING),
            WindowedValue.of(
                4, new Instant(0L), GlobalWindow.INSTANCE, PaneInfo.ON_TIME_AND_ONLY_FIRING),
            WindowedValue.of(
                2, new Instant(0L), GlobalWindow.INSTANCE, PaneInfo.ON_TIME_AND_ONLY_FIRING));

    assertThat(extractor.apply(onlyOnTime), containsInAnyOrder(2, 4, 8));
  }

  @Test
  public void nonLatePanesSingleEarly() {
    SerializableFunction<Iterable<WindowedValue<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.nonLatePanes();
    Iterable<WindowedValue<Integer>> onlyOnTime =
        ImmutableList.of(
            WindowedValue.of(
                8,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(true, false, Timing.EARLY)),
            WindowedValue.of(
                4,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(true, false, Timing.EARLY)));

    assertThat(extractor.apply(onlyOnTime), containsInAnyOrder(4, 8));
  }

  @Test
  public void allPanesSingleLate() {
    SerializableFunction<Iterable<WindowedValue<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.nonLatePanes();
    Iterable<WindowedValue<Integer>> onlyOnTime =
        ImmutableList.of(
            WindowedValue.of(
                8,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(false, false, Timing.LATE, 0L, 0L)));

    assertThat(extractor.apply(onlyOnTime), emptyIterable());
  }

  @Test
  public void nonLatePanesMultiplePanes() {
    SerializableFunction<Iterable<WindowedValue<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.nonLatePanes();
    Iterable<WindowedValue<Integer>> onlyOnTime =
        ImmutableList.of(
            WindowedValue.of(
                8,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(false, false, Timing.LATE, 2L, 1L)),
            WindowedValue.of(7, new Instant(0L), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING),
            WindowedValue.of(
                4,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(false, false, Timing.ON_TIME, 1L, 0L)),
            WindowedValue.of(
                1,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(true, false, Timing.EARLY)));

    assertThat(extractor.apply(onlyOnTime), containsInAnyOrder(4, 1, 7));
  }

  @Test
  public void allPanesSinglePane() {
    SerializableFunction<Iterable<WindowedValue<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.allPanes();
    Iterable<WindowedValue<Integer>> onlyOnTime =
        ImmutableList.of(
            WindowedValue.of(
                8, new Instant(0L), GlobalWindow.INSTANCE, PaneInfo.ON_TIME_AND_ONLY_FIRING),
            WindowedValue.of(
                4, new Instant(0L), GlobalWindow.INSTANCE, PaneInfo.ON_TIME_AND_ONLY_FIRING),
            WindowedValue.of(
                2, new Instant(0L), GlobalWindow.INSTANCE, PaneInfo.ON_TIME_AND_ONLY_FIRING));

    assertThat(extractor.apply(onlyOnTime), containsInAnyOrder(2, 4, 8));
  }

  @Test
  public void allPanesMultiplePanes() {
    SerializableFunction<Iterable<WindowedValue<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.allPanes();
    Iterable<WindowedValue<Integer>> onlyOnTime =
        ImmutableList.of(
            WindowedValue.of(
                8,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(false, false, Timing.LATE, 2L, 1L)),
            WindowedValue.of(
                4,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(false, false, Timing.ON_TIME, 1L, 0L)),
            WindowedValue.of(
                1,
                new Instant(0L),
                GlobalWindow.INSTANCE,
                PaneInfo.createPane(true, false, Timing.EARLY)));

    assertThat(extractor.apply(onlyOnTime), containsInAnyOrder(4, 8, 1));
  }

  @Test
  public void allPanesEmpty() {
    SerializableFunction<Iterable<WindowedValue<Integer>>, Iterable<Integer>> extractor =
        PaneExtractors.allPanes();
    Iterable<WindowedValue<Integer>> noPanes = ImmutableList.of();

    assertThat(extractor.apply(noPanes), emptyIterable());
  }
}
