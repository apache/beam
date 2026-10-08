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
package org.apache.beam.sdk.io.iceberg.cdc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.util.Collection;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SnapshotWindowFn}. */
@RunWith(JUnit4.class)
public class SnapshotWindowFnTest {
  @Test
  public void identicalTimestampsShareWindowAndAdjacentTimestampsDoNot() throws Exception {
    SnapshotWindowFn fn = new SnapshotWindowFn();
    Instant timestamp = new Instant(1_000L);

    IntervalWindow first = onlyWindow(fn, timestamp);
    IntervalWindow second = onlyWindow(fn, timestamp);
    IntervalWindow adjacent = onlyWindow(fn, timestamp.plus(Duration.millis(1)));

    assertEquals(new IntervalWindow(timestamp, timestamp.plus(Duration.millis(1))), first);
    assertEquals(first, second);
    assertNotEquals(first, adjacent);
    assertEquals(
        new IntervalWindow(timestamp.plus(Duration.millis(1)), timestamp.plus(Duration.millis(2))),
        adjacent);
  }

  @Test
  public void sideInputMappingStartsAtMainWindowMaxTimestamp() {
    SnapshotWindowFn fn = new SnapshotWindowFn();
    IntervalWindow mainWindow = new IntervalWindow(new Instant(10L), new Instant(20L));

    IntervalWindow sideInputWindow = fn.getDefaultWindowMappingFn().getSideInputWindow(mainWindow);

    assertEquals(
        new IntervalWindow(
            mainWindow.maxTimestamp(), mainWindow.maxTimestamp().plus(Duration.millis(1L))),
        sideInputWindow);
  }

  @SuppressWarnings("NonCanonicalType")
  private static IntervalWindow onlyWindow(SnapshotWindowFn fn, Instant timestamp)
      throws Exception {
    Collection<IntervalWindow> windows =
        fn.assignWindows(
            fn.new AssignContext() {
              @Override
              public Object element() {
                return "element";
              }

              @Override
              public Instant timestamp() {
                return timestamp;
              }

              @Override
              public BoundedWindow window() {
                return GlobalWindow.INSTANCE;
              }
            });
    assertThat(
        windows, contains(new IntervalWindow(timestamp, timestamp.plus(Duration.millis(1L)))));
    return windows.iterator().next();
  }
}
