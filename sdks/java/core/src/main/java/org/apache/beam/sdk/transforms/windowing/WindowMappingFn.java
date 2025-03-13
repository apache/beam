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

import java.io.Serializable;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;

/**
 * A function that takes the windows of elements in a main input and maps them to the appropriate
 * window in a {@link PCollectionView} consumed as a {@link
 * org.apache.beam.sdk.transforms.ParDo.MultiOutput#withSideInputs(PCollectionView[]) side input}.
 */
public abstract class WindowMappingFn<TargetWindowT extends BoundedWindow> implements Serializable {
  private final Duration maximumLookback;

  /** Create a new {@link WindowMappingFn} with {@link Duration#ZERO zero} maximum lookback. */
  protected WindowMappingFn() {
    this(Duration.ZERO);
  }

  /** Create a new {@link WindowMappingFn} with the specified maximum lookback. */
  protected WindowMappingFn(Duration maximumLookback) {
    this.maximumLookback = maximumLookback;
  }

  /** Returns the window of the side input corresponding to the given window of the main input. */
  public abstract TargetWindowT getSideInputWindow(BoundedWindow mainWindow);

  /**
   * The maximum distance between the end of any main input window {@code mainWindow} and the end of
   * the side input window returned by {@link #getSideInputWindow(BoundedWindow)}
   *
   * <p>A side input window {@code w} becomes unreachable when the input watermarks for all
   * consumers surpasses the timestamp:
   *
   * <p>(end of side input window) + (maximum lookback) + (main input allowed lateness).
   *
   * <p>At this point, every main input window that could map to {@code w} is expired.
   */
  public final Duration maximumLookback() {
    return maximumLookback;
  }
}
