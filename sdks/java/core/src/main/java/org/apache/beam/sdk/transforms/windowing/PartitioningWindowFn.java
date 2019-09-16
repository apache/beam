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
package org.apache.beam.sdk.transforms.windowing;

import java.util.Arrays;
import java.util.Collection;
import org.joda.time.Instant;

/**
 * A {@link WindowFn} that places each value into exactly one window based on its timestamp and
 * never merges windows.
 *
 * @param <T> type of elements being windowed
 * @param <W> window type
 */
public abstract class PartitioningWindowFn<T, W extends BoundedWindow>
    extends NonMergingWindowFn<T, W> {
  /** Returns the single window to which elements with this timestamp belong. */
  public abstract W assignWindow(Instant timestamp);

  @Override
  public final Collection<W> assignWindows(AssignContext c) {
    return Arrays.asList(assignWindow(c.timestamp()));
  }

  @Override
  public WindowMappingFn<W> getDefaultWindowMappingFn() {
    return new WindowMappingFn<W>() {
      @Override
      public W getSideInputWindow(BoundedWindow mainWindow) {
        if (mainWindow instanceof GlobalWindow) {
          throw new IllegalArgumentException(
              "Attempted to get side input window for GlobalWindow from non-global WindowFn");
        }
        return assignWindow(mainWindow.maxTimestamp());
      }
    };
  }

  @Override
  public Instant getOutputTime(Instant inputTimestamp, W window) {
    return inputTimestamp;
  }

  @Override
  public final boolean assignsToOneWindow() {
    return true;
  }
}
