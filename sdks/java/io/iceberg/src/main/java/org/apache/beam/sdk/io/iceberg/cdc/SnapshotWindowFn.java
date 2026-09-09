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

import java.util.Collection;
import java.util.Collections;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A {@link WindowFn} that assigns each element to a 1-millisecond {@link IntervalWindow} anchored
 * at the element's event timestamp.
 *
 * <p>We set the element's timestamp as its snapshot commit timestamp. All tasks/records from the
 * same snapshot land in the same window.
 *
 * <p>With the per-snapshot watermark from {@link WatchForSnapshotsSdf}, the CoGroupByKey fires when
 * a snapshot is fully drained. The watermark advances past the snapshot's commit time only after
 * every downstream stage has finished processing that snapshot's records.
 *
 * <p>Two snapshots committed within the same millisecond may collapse into the same window. But
 * that's okay because {@link ReadFromChangelogs} includes snapshot sequence number in the key
 * before routing to the CoGBK, so it won't produce incorrect joins.
 */
public class SnapshotWindowFn extends NonMergingWindowFn<Object, IntervalWindow> {
  private static final Duration WINDOW_LENGTH = Duration.millis(1);

  @Override
  public Collection<IntervalWindow> assignWindows(AssignContext c) {
    Instant ts = c.timestamp();
    return Collections.singletonList(new IntervalWindow(ts, ts.plus(WINDOW_LENGTH)));
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    return other instanceof SnapshotWindowFn;
  }

  @Override
  public Coder<IntervalWindow> windowCoder() {
    return IntervalWindow.getCoder();
  }

  @Override
  public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
    // Just return a window covering the main-input window's end timestamp.
    return new WindowMappingFn<>() {
      @Override
      public IntervalWindow getSideInputWindow(BoundedWindow mainWindow) {
        Instant end = mainWindow.maxTimestamp();
        return new IntervalWindow(end, end.plus(WINDOW_LENGTH));
      }
    };
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    return obj instanceof SnapshotWindowFn;
  }

  @Override
  public int hashCode() {
    return SnapshotWindowFn.class.hashCode();
  }
}
