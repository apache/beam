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
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SideInputHandler}. */
@RunWith(JUnit4.class)
public class SideInputHandlerTest {

  private static final long WINDOW_MSECS_1 = 100;
  private static final long WINDOW_MSECS_2 = 500;
  private PCollectionView<Iterable<String>> view1;
  private PCollectionView<Iterable<String>> view2;

  @Before
  public void setUp() {
    PCollection<String> pc = Pipeline.create().apply(Create.of("1"));
    view1 =
        pc.apply(Window.into(FixedWindows.of(new Duration(WINDOW_MSECS_1))))
            .apply(View.asIterable());
    view2 =
        pc.apply(Window.into(FixedWindows.of(new Duration(WINDOW_MSECS_2))))
            .apply(View.asIterable());
  }

  @Test
  public void testIsEmpty() {
    SideInputHandler sideInputHandler =
        new SideInputHandler(ImmutableList.of(view1), InMemoryStateInternals.<Void>forKey(null));

    assertFalse(sideInputHandler.isEmpty());

    // create an empty handler
    SideInputHandler emptySideInputHandler =
        new SideInputHandler(ImmutableList.of(), InMemoryStateInternals.<Void>forKey(null));

    assertTrue(emptySideInputHandler.isEmpty());
  }

  @Test
  public void testContains() {
    SideInputHandler sideInputHandler =
        new SideInputHandler(ImmutableList.of(view1), InMemoryStateInternals.<Void>forKey(null));

    assertTrue(sideInputHandler.contains(view1));
    assertFalse(sideInputHandler.contains(view2));
  }

  @Test
  public void testIsReady() {
    SideInputHandler sideInputHandler =
        new SideInputHandler(
            ImmutableList.of(view1, view2), InMemoryStateInternals.<Void>forKey(null));

    IntervalWindow firstWindow = new IntervalWindow(new Instant(0), new Instant(WINDOW_MSECS_1));

    IntervalWindow secondWindow = new IntervalWindow(new Instant(0), new Instant(WINDOW_MSECS_2));

    // side input should not yet be ready
    assertFalse(sideInputHandler.isReady(view1, firstWindow));

    // add a value for view1
    sideInputHandler.addSideInputValue(
        view1,
        valuesInWindow(
            materializeValuesFor(view1.getPipeline().getOptions(), View.asIterable(), "Hello"),
            new Instant(0),
            firstWindow));

    // now side input should be ready
    assertTrue(sideInputHandler.isReady(view1, firstWindow));

    // second window input should still not be ready
    assertFalse(sideInputHandler.isReady(view1, secondWindow));
  }

  @Test
  public void testNewInputReplacesPreviousInput() {
    // new input should completely replace old input
    // the creation of the Iterable that has the side input
    // contents happens upstream. this is also where
    // accumulation/discarding is decided.

    SideInputHandler sideInputHandler =
        new SideInputHandler(ImmutableList.of(view1), InMemoryStateInternals.<Void>forKey(null));

    IntervalWindow window = new IntervalWindow(new Instant(0), new Instant(WINDOW_MSECS_1));

    // add a first value for view1
    sideInputHandler.addSideInputValue(
        view1,
        valuesInWindow(
            materializeValuesFor(view1.getPipeline().getOptions(), View.asIterable(), "Hello"),
            new Instant(0),
            window));

    assertThat(sideInputHandler.get(view1, window), contains("Hello"));

    // subsequent values should replace existing values
    sideInputHandler.addSideInputValue(
        view1,
        valuesInWindow(
            materializeValuesFor(
                view1.getPipeline().getOptions(), View.asIterable(), "Ciao", "Buongiorno"),
            new Instant(0),
            window));

    assertThat(sideInputHandler.get(view1, window), contains("Ciao", "Buongiorno"));
  }

  @Test
  public void testMultipleWindows() {
    SideInputHandler sideInputHandler =
        new SideInputHandler(ImmutableList.of(view1), InMemoryStateInternals.<Void>forKey(null));

    // two windows that we'll later use for adding elements/retrieving side input
    IntervalWindow firstWindow = new IntervalWindow(new Instant(0), new Instant(WINDOW_MSECS_1));
    IntervalWindow secondWindow =
        new IntervalWindow(new Instant(1000), new Instant(1000 + WINDOW_MSECS_2));

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
    SideInputHandler sideInputHandler =
        new SideInputHandler(
            ImmutableList.of(view1, view2), InMemoryStateInternals.<Void>forKey(null));

    // two windows that we'll later use for adding elements/retrieving side input
    IntervalWindow firstWindow = new IntervalWindow(new Instant(0), new Instant(WINDOW_MSECS_1));

    // add value for view1 in the first window
    sideInputHandler.addSideInputValue(
        view1,
        valuesInWindow(
            materializeValuesFor(view1.getPipeline().getOptions(), View.asIterable(), "Hello"),
            new Instant(0),
            firstWindow));

    assertThat(sideInputHandler.get(view1, firstWindow), contains("Hello"));

    // view2 should not have any data
    assertFalse(sideInputHandler.isReady(view2, firstWindow));

    // also add some data for view2
    sideInputHandler.addSideInputValue(
        view2,
        valuesInWindow(
            materializeValuesFor(view2.getPipeline().getOptions(), View.asIterable(), "Salut"),
            new Instant(0),
            firstWindow));

    assertTrue(sideInputHandler.isReady(view2, firstWindow));
    assertThat(sideInputHandler.get(view2, firstWindow), contains("Salut"));

    // view1 should not be affected by that
    assertThat(sideInputHandler.get(view1, firstWindow), contains("Hello"));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private WindowedValue<Iterable<?>> valuesInWindow(
      List<Object> values, Instant timestamp, BoundedWindow window) {
    return (WindowedValue) WindowedValue.of(values, timestamp, window, PaneInfo.NO_FIRING);
  }
}
