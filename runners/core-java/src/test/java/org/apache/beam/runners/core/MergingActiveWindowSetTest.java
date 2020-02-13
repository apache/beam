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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test NonMergingActiveWindowSet. */
@RunWith(JUnit4.class)
public class MergingActiveWindowSetTest {
  private Sessions windowFn;
  private StateInternals state;
  private MergingActiveWindowSet<IntervalWindow> set;
  private ActiveWindowSet.MergeCallback<IntervalWindow> callback;

  @Before
  public void setup() {
    windowFn = Sessions.withGapDuration(Duration.millis(10));
    state = InMemoryStateInternals.forKey("dummyKey");
    set = new MergingActiveWindowSet<>(windowFn, state);
    @SuppressWarnings("unchecked")
    ActiveWindowSet.MergeCallback<IntervalWindow> callback =
        mock(ActiveWindowSet.MergeCallback.class);
    this.callback = callback;
  }

  @After
  public void after() {
    set = null;
    state = null;
    windowFn = null;
  }

  private void add(long... instants) {
    for (final long instant : instants) {
      System.out.println("ADD " + instant);
      Sessions.AssignContext context =
          windowFn.new AssignContext() {
            @Override
            public Object element() {
              return (Object) instant;
            }

            @Override
            public Instant timestamp() {
              return new Instant(instant);
            }

            @Override
            public BoundedWindow window() {
              return GlobalWindow.INSTANCE;
            }
          };

      for (IntervalWindow window : windowFn.assignWindows(context)) {
        set.ensureWindowExists(window);
      }
    }
  }

  private Map<IntervalWindow, IntervalWindow> merge(
      List<IntervalWindow> toBeMerged, IntervalWindow mergeResult) throws Exception {
    IntervalWindow predictedPostMergeWriteStateAddress =
        set.mergedWriteStateAddress(toBeMerged, mergeResult);

    System.out.println("BEFORE MERGE");
    System.out.println(set);
    Map<IntervalWindow, IntervalWindow> map = new HashMap<>();
    for (IntervalWindow window : toBeMerged) {
      System.out.println("WILL MERGE " + window + " INTO " + mergeResult);
      map.put(window, mergeResult);
    }
    System.out.println("AFTER MERGE");
    set.merge(callback);
    verify(callback).onMerge(toBeMerged, mergeResult);
    System.out.println(set);

    assertEquals(predictedPostMergeWriteStateAddress, set.writeStateAddress(mergeResult));

    return map;
  }

  private void activate(Map<IntervalWindow, IntervalWindow> map, long... instants) {
    for (long instant : instants) {
      IntervalWindow window = window(instant, 10);
      IntervalWindow active = map.get(window);
      if (active == null) {
        active = window;
      }
      System.out.println("ACTIVATE " + active);
      set.ensureWindowIsActive(active);
    }
    set.checkInvariants();
  }

  private void cleanup() {
    System.out.println("CLEANUP");
    set.cleanupTemporaryWindows();
    set.checkInvariants();
    System.out.println(set);
    set.persist();
    MergingActiveWindowSet<IntervalWindow> reloaded = new MergingActiveWindowSet<>(windowFn, state);
    reloaded.checkInvariants();
    assertEquals(set, reloaded);
  }

  private IntervalWindow window(long start, long size) {
    return new IntervalWindow(new Instant(start), new Duration(size));
  }

  @Test
  public void testLifecycle() throws Exception {
    // Step 1: New elements show up, introducing NEW windows which are partially merged.
    // NEW 1+10
    // NEW 2+10
    // NEW 15+10
    // =>
    // ACTIVE 1+11 (target 1+11)
    // ACTIVE 15+10 (target 15+10)
    add(1, 2, 15);
    assertEquals(
        ImmutableSet.of(window(1, 10), window(2, 10), window(15, 10)),
        set.getActiveAndNewWindows());
    Map<IntervalWindow, IntervalWindow> map =
        merge(ImmutableList.of(window(1, 10), window(2, 10)), window(1, 11));
    activate(map, 1, 2, 15);
    assertEquals(ImmutableSet.of(window(1, 11), window(15, 10)), set.getActiveAndNewWindows());
    assertEquals(ImmutableSet.of(window(1, 11)), set.readStateAddresses(window(1, 11)));
    assertEquals(ImmutableSet.of(window(15, 10)), set.readStateAddresses(window(15, 10)));
    cleanup();

    // Step 2: Another element, merged into an existing ACTIVE window.
    // NEW 3+10
    // =>
    // ACTIVE 1+12 (target 1+11)
    // ACTIVE 15+10 (target 15+10)
    add(3);
    assertEquals(
        ImmutableSet.of(window(3, 10), window(1, 11), window(15, 10)),
        set.getActiveAndNewWindows());
    map = merge(ImmutableList.of(window(1, 11), window(3, 10)), window(1, 12));
    activate(map, 3);
    assertEquals(ImmutableSet.of(window(1, 12), window(15, 10)), set.getActiveAndNewWindows());
    assertEquals(ImmutableSet.of(window(1, 11)), set.readStateAddresses(window(1, 12)));
    assertEquals(ImmutableSet.of(window(15, 10)), set.readStateAddresses(window(15, 10)));
    cleanup();

    // Step 3: Another element, causing two ACTIVE windows to be merged.
    // NEW 8+10
    // =>
    // ACTIVE 1+24 (target 1+11)
    add(8);
    assertEquals(
        ImmutableSet.of(window(8, 10), window(1, 12), window(15, 10)),
        set.getActiveAndNewWindows());
    map = merge(ImmutableList.of(window(1, 12), window(8, 10), window(15, 10)), window(1, 24));
    activate(map, 8);
    assertEquals(ImmutableSet.of(window(1, 24)), set.getActiveAndNewWindows());
    assertEquals(ImmutableSet.of(window(1, 11)), set.readStateAddresses(window(1, 24)));
    cleanup();

    // Step 4: Another element, merged into an existing ACTIVE window.
    // NEW 9+10
    // =>
    // ACTIVE 1+24 (target 1+11)
    add(9);
    assertEquals(ImmutableSet.of(window(9, 10), window(1, 24)), set.getActiveAndNewWindows());
    map = merge(ImmutableList.of(window(1, 24), window(9, 10)), window(1, 24));
    activate(map, 9);
    assertEquals(ImmutableSet.of(window(1, 24)), set.getActiveAndNewWindows());
    assertEquals(ImmutableSet.of(window(1, 11)), set.readStateAddresses(window(1, 24)));
    cleanup();

    // Step 5: Another element reusing earlier window, merged into an existing ACTIVE window.
    // NEW 1+10
    // =>
    // ACTIVE 1+24 (target 1+11)
    add(1);
    assertEquals(ImmutableSet.of(window(1, 10), window(1, 24)), set.getActiveAndNewWindows());
    map = merge(ImmutableList.of(window(1, 10), window(1, 24)), window(1, 24));
    activate(map, 1);
    assertEquals(ImmutableSet.of(window(1, 24)), set.getActiveAndNewWindows());
    assertEquals(ImmutableSet.of(window(1, 11)), set.readStateAddresses(window(1, 24)));
    cleanup();

    // Step 6: Window is closed.
    set.remove(window(1, 24));
    cleanup();
    assertTrue(set.getActiveAndNewWindows().isEmpty());
  }

  @Test
  public void testLegacyState() {
    // Pre 1.4 we merged window state lazily.
    // Simulate loading an active window set with multiple state address windows.
    set.addActiveForTesting(
        window(1, 12), ImmutableList.of(window(1, 10), window(2, 10), window(3, 10)));

    // Make sure we can detect and repair the state.
    assertTrue(set.isActive(window(1, 12)));
    assertEquals(
        ImmutableSet.of(window(1, 10), window(2, 10), window(3, 10)),
        set.readStateAddresses(window(1, 12)));
    assertEquals(
        window(1, 10),
        set.mergedWriteStateAddress(
            ImmutableList.of(window(1, 10), window(2, 10), window(3, 10)), window(1, 12)));
    set.merged(window(1, 12));
    cleanup();

    // For then on we are back to the eager case.
    assertEquals(ImmutableSet.of(window(1, 10)), set.readStateAddresses(window(1, 12)));
  }
}
