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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.util.state.InMemoryStateInternals;
import org.apache.beam.sdk.util.state.StateInternals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collection;

/**
 * Test NonMergingActiveWindowSet.
 */
@RunWith(JUnit4.class)
public class MergingActiveWindowSetTest {
  private Sessions windowFn;
  private StateInternals<String> state;
  private MergingActiveWindowSet<IntervalWindow> set;

  @Before
  public void before() {
    windowFn = Sessions.withGapDuration(Duration.millis(10));
    state = InMemoryStateInternals.forKey("dummyKey");
    set = new MergingActiveWindowSet<>(windowFn, state);
  }

  @After
  public void after() {
    set = null;
    state = null;
    windowFn = null;
  }

  private void add(final long instant) {
    final Object element = new Long(instant);
    Sessions.AssignContext context = windowFn.new AssignContext() {
      @Override
      public Object element() {
        return element;
      }

      @Override
      public Instant timestamp() {
        return new Instant(instant);
      }

      @Override
      public Collection<? extends BoundedWindow> windows() {
        return ImmutableList.of();
      }
    };

    for (IntervalWindow window : windowFn.assignWindows(context)) {
      set.ensureWindowExists(window);
    }
  }

  private void merge(ActiveWindowSet.MergeCallback<IntervalWindow> callback) throws Exception {
    set.merge(callback);
    for (IntervalWindow window : set.getActiveWindows()) {
      set.ensureWindowIsActive(window);
    }
    set.checkInvariants();
  }

  private void pruneAndPersist() {
    set.cleanupTemporaryWindows();
    set.checkInvariants();
    set.persist();
  }

  private IntervalWindow window(long start, long size) {
    return new IntervalWindow(new Instant(start), new Duration(size));
  }

  @Test
  public void test() throws Exception {
    @SuppressWarnings("unchecked")
    ActiveWindowSet.MergeCallback<IntervalWindow> callback =
        mock(ActiveWindowSet.MergeCallback.class);

    // NEW 1+10
    // NEW 2+10
    // NEW 15+10
    // =>
    // ACTIVE 1+11 (target 1+11)
    // EPHEMERAL 1+10 -> 1+11
    // EPHEMERAL 2+10 -> 1+11
    // ACTIVE 15+10 (target 15+10)
    add(1);
    add(2);
    add(15);
    merge(callback);
    verify(callback).onMerge(ImmutableList.of(window(1, 10), window(2, 10)),
        ImmutableList.<IntervalWindow>of(), window(1, 11));
    assertEquals(ImmutableSet.of(window(1, 11), window(15, 10)), set.getActiveWindows());
    assertEquals(window(1, 11), set.mergeResultWindow(window(1, 10)));
    assertEquals(window(1, 11), set.mergeResultWindow(window(2, 10)));
    assertEquals(window(1, 11), set.mergeResultWindow(window(1, 11)));
    assertEquals(window(15, 10), set.mergeResultWindow(window(15, 10)));
    assertEquals(
        ImmutableSet.<IntervalWindow>of(window(1, 11)), set.readStateAddresses(window(1, 11)));
    assertEquals(
        ImmutableSet.<IntervalWindow>of(window(15, 10)), set.readStateAddresses(window(15, 10)));

    // NEW 3+10
    // =>
    // ACTIVE 1+12 (target 1+11)
    // EPHEMERAL 3+10 -> 1+12
    // ACTIVE 15+10 (target 15+10)
    add(3);
    merge(callback);
    verify(callback).onMerge(ImmutableList.of(window(1, 11), window(3, 10)),
        ImmutableList.<IntervalWindow>of(window(1, 11)), window(1, 12));
    assertEquals(ImmutableSet.of(window(1, 12), window(15, 10)), set.getActiveWindows());
    assertEquals(window(1, 12), set.mergeResultWindow(window(3, 10)));

    // NEW 8+10
    // =>
    // ACTIVE 1+24 (target 1+11, 15+10)
    // MERGED 1+11 -> 1+24
    // MERGED 15+10 -> 1+24
    // EPHEMERAL 1+12 -> 1+24
    add(8);
    merge(callback);
    verify(callback).onMerge(ImmutableList.of(window(1, 12), window(8, 10), window(15, 10)),
        ImmutableList.<IntervalWindow>of(window(1, 12), window(15, 10)), window(1, 24));
    assertEquals(ImmutableSet.of(window(1, 24)), set.getActiveWindows());
    assertEquals(window(1, 24), set.mergeResultWindow(window(1, 12)));
    assertEquals(window(1, 24), set.mergeResultWindow(window(1, 11)));
    assertEquals(window(1, 24), set.mergeResultWindow(window(15, 10)));

    // NEW 9+10
    // =>
    // ACTIVE 1+24 (target 1+11, 15+10)
    add(9);
    merge(callback);
    verify(callback).onMerge(ImmutableList.of(window(1, 24), window(9, 10)),
        ImmutableList.<IntervalWindow>of(window(1, 24)), window(1, 24));

    pruneAndPersist();
  }
}
