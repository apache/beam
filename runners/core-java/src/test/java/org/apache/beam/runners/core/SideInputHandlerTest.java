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

import static org.apache.beam.sdk.testing.PCollectionViewTesting.materializeValuesFor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SideInputHandler}. */
@RunWith(JUnit4.class)
public class SideInputHandlerTest {

  @Test
  public void testIsEmptyTrue() {
    // Create an empty handler
    SideInputHandler emptySideInputHandler =
        new SideInputHandler(ImmutableList.of(), InMemoryStateInternals.<Void>forKey(null));
    assertTrue(emptySideInputHandler.isEmpty());
  }

  @Test
  public void testIsEmptyFalse() {
    PCollectionView<Iterable<String>> view =
        Pipeline.create().apply(Create.of("1")).apply(View.asIterable());
    SideInputHandler sideInputHandler =
        new SideInputHandler(ImmutableList.of(view), InMemoryStateInternals.<Void>forKey(null));

    assertFalse(sideInputHandler.isEmpty());
  }

  @Test
  public void testContains() {
    PCollection<String> pc = Pipeline.create().apply(Create.of("1"));
    PCollectionView<Iterable<String>> view1 = pc.apply(View.asIterable());
    PCollectionView<Iterable<String>> view2 = pc.apply(View.asIterable());

    // Only contains view1
    SideInputHandler sideInputHandler =
        new SideInputHandler(ImmutableList.of(view1), InMemoryStateInternals.<Void>forKey(null));

    assertTrue(sideInputHandler.contains(view1));
    assertFalse(sideInputHandler.contains(view2));
  }

  @Test
  public void testIsReady() {
    long view1WindowSize = 100;
    PCollection<String> pc = Pipeline.create().apply(Create.of("1"));
    PCollectionView<Iterable<String>> view1 =
        pc.apply(Window.into(FixedWindows.of(Duration.millis(view1WindowSize))))
            .apply(View.asIterable());

    // Unused, just to have a non-trivial handler set up
    PCollectionView<Iterable<String>> view2 = pc.apply(View.asIterable());

    SideInputHandler sideInputHandler =
        new SideInputHandler(
            ImmutableList.of(view1, view2), InMemoryStateInternals.<Void>forKey(null));

    // Adjacent fixed windows
    IntervalWindow firstWindow = new IntervalWindow(new Instant(0), new Instant(view1WindowSize));
    IntervalWindow secondWindow =
        new IntervalWindow(new Instant(view1WindowSize), new Instant(view1WindowSize * 2));

    // side input should not yet be ready in first window
    assertFalse(sideInputHandler.isReady(view1, firstWindow));

    // add a value for view1
    sideInputHandler.addSideInputValue(
        view1,
        valuesInWindow(
            materializeValuesFor(view1.getPipeline().getOptions(), View.asIterable(), "Hello"),
            new Instant(0),
            firstWindow));

    // now side input should be ready in first window
    assertTrue(sideInputHandler.isReady(view1, firstWindow));

    // second window input should still not be ready
    assertFalse(sideInputHandler.isReady(view1, secondWindow));
  }

  @Test
  public void testNewInputReplacesPreviousInput() {
    long view1WindowSize = 100;
    PCollection<String> pc = Pipeline.create().apply(Create.of("1"));
    PCollectionView<Iterable<String>> view =
        pc.apply(Window.into(FixedWindows.of(Duration.millis(view1WindowSize))))
            .apply(View.asIterable());

    // new input should completely replace old input
    // the creation of the Iterable that has the side input
    // contents happens upstream. this is also where
    // accumulation/discarding is decided.

    SideInputHandler sideInputHandler =
        new SideInputHandler(ImmutableList.of(view), InMemoryStateInternals.<Void>forKey(null));

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(view1WindowSize));

    // add a first value for view
    sideInputHandler.addSideInputValue(
        view,
        valuesInWindow(
            materializeValuesFor(view.getPipeline().getOptions(), View.asIterable(), "Hello"),
            new Instant(0),
            window));

    assertThat(sideInputHandler.get(view, window), contains("Hello"));

    // subsequent values should replace existing values
    sideInputHandler.addSideInputValue(
        view,
        valuesInWindow(
            materializeValuesFor(
                view.getPipeline().getOptions(), View.asIterable(), "Ciao", "Buongiorno"),
            new Instant(0),
            window));

    assertThat(sideInputHandler.get(view, window), contains("Ciao", "Buongiorno"));
  }

  @Test
  public void testMultipleWindows() {
    long view1WindowSize = 100;
    PCollectionView<Iterable<String>> view1 =
        Pipeline.create()
            .apply(Create.of("1"))
            .apply(Window.into(FixedWindows.of(Duration.millis(view1WindowSize))))
            .apply(View.asIterable());

    SideInputHandler sideInputHandler =
        new SideInputHandler(ImmutableList.of(view1), InMemoryStateInternals.<Void>forKey(null));

    // two windows that we'll later use for adding elements/retrieving side input
    IntervalWindow firstWindow = new IntervalWindow(new Instant(0), new Instant(view1WindowSize));
    IntervalWindow secondWindow =
        new IntervalWindow(new Instant(view1WindowSize), new Instant(view1WindowSize * 2));

    // add a first value for view1 in the first window
    sideInputHandler.addSideInputValue(
        view1,
        valuesInWindow(
            materializeValuesFor(view1.getPipeline().getOptions(), View.asIterable(), "Hello"),
            new Instant(0),
            firstWindow));

    assertThat(sideInputHandler.get(view1, firstWindow), contains("Hello"));

    // add something for second window of view1
    sideInputHandler.addSideInputValue(
        view1,
        valuesInWindow(
            materializeValuesFor(
                view1.getPipeline().getOptions(), View.asIterable(), "Arrivederci"),
            new Instant(0),
            secondWindow));

    assertThat(sideInputHandler.get(view1, secondWindow), contains("Arrivederci"));

    // contents for first window should be unaffected
    assertThat(sideInputHandler.get(view1, firstWindow), contains("Hello"));
  }

  @Test
  public void testMultipleSideInputs() {
    long windowSize = 100;
    PCollectionView<Iterable<String>> view1;
    PCollectionView<Iterable<String>> view2;
    PCollection<String> pc = Pipeline.create().apply(Create.of("1"));
    view1 =
        pc.apply(Window.into(FixedWindows.of(Duration.millis(windowSize))))
            .apply(View.asIterable());
    view2 =
        pc.apply(Window.into(FixedWindows.of(Duration.millis(windowSize))))
            .apply(View.asIterable());

    SideInputHandler sideInputHandler =
        new SideInputHandler(
            ImmutableList.of(view1, view2), InMemoryStateInternals.<Void>forKey(null));

    // two windows that we'll later use for adding elements/retrieving side input
    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(windowSize));

    // add value for view1 in the first window
    sideInputHandler.addSideInputValue(
        view1,
        valuesInWindow(
            materializeValuesFor(view1.getPipeline().getOptions(), View.asIterable(), "Hello"),
            new Instant(0),
            window));

    assertThat(sideInputHandler.get(view1, window), contains("Hello"));

    // view2 should not have any data
    assertFalse(sideInputHandler.isReady(view2, window));

    // also add some data for view2
    sideInputHandler.addSideInputValue(
        view2,
        valuesInWindow(
            materializeValuesFor(view2.getPipeline().getOptions(), View.asIterable(), "Salut"),
            new Instant(0),
            window));

    assertTrue(sideInputHandler.isReady(view2, window));
    assertThat(sideInputHandler.get(view2, window), contains("Salut"));

    // view1 should not be affected by that
    assertThat(sideInputHandler.get(view1, window), contains("Hello"));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private WindowedValue<Iterable<?>> valuesInWindow(
      List<Object> values, Instant timestamp, BoundedWindow window) {
    return (WindowedValue) WindowedValue.of(values, timestamp, window, PaneInfo.NO_FIRING);
  }
}
