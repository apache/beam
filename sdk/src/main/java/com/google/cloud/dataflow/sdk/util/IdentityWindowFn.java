/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.NonMergingWindowFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import org.joda.time.Instant;

import java.util.Collection;

/**
 * A {@link WindowFn} that leaves all associations between elements and windows unchanged.
 *
 * <p>This {@link WindowFn} is applied when elements must be passed through a {@link GroupByKey},
 * but should maintain their existing {@link Window} assignments. Because windows may have been
 * merged, the earlier {@link WindowFn} may not appropriately maintain the existing window
 * assignments. For example, if the earlier {@link WindowFn} merges windows, after a
 * {@link GroupByKey} the {@link WindowingStrategy} uses {@link InvalidWindows}, and no further
 * {@link GroupByKey} can be applied without applying a new {@link WindowFn}. This {@link WindowFn}
 * allows existing window assignments to be maintained across a single group by key, at which point
 * the earlier {@link WindowingStrategy} should be restored.
 *
 * <p>This {@link WindowFn} is an internal implementation detail of sdk-provided utilities, and
 * should not be used by {@link Pipeline} writers.
 */
class IdentityWindowFn<T> extends NonMergingWindowFn<T, BoundedWindow> {

  /**
   * The coder of the type of windows of the input {@link PCollection}. This is not an arbitrary
   * {@link BoundedWindow} {@link Coder}, but is safe to use for all windows assigned by this
   * transform, as it should be the same coder used by the {@link WindowFn} that initially assigned
   * these windows.
   */
  private final Coder<BoundedWindow> coder;
  private final boolean assignsToSingleWindow;

  public IdentityWindowFn(Coder<? extends BoundedWindow> coder, boolean assignsToSingleWindow) {
    // Safe because it is only used privately here.
    // At every point where a window is returned or accepted, it has been provided
    // by priorWindowFn, so it is of the expected type.
    @SuppressWarnings("unchecked")
    Coder<BoundedWindow> windowCoder = (Coder<BoundedWindow>) coder;
    this.coder = windowCoder;
    this.assignsToSingleWindow = assignsToSingleWindow;
  }

  @Override
  public Collection<BoundedWindow> assignWindows(WindowFn<T, BoundedWindow>.AssignContext c)
      throws Exception {
    // The windows are provided by priorWindowFn, which also provides the coder for them
    @SuppressWarnings("unchecked")
    Collection<BoundedWindow> priorWindows = (Collection<BoundedWindow>) c.windows();
    return priorWindows;
  }

  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    throw new UnsupportedOperationException(
        String.format(
            "%s.isCompatible() should never be called."
                + " It is a private implementation detail of sdk utilities."
                + " This message indicates a bug in the Beam SDK.",
            getClass().getCanonicalName()));
  }

  @Override
  public Coder<BoundedWindow> windowCoder() {
    // Safe because the previous WindowFn provides both the windows and the coder.
    // The Coder is _not_ actually a coder for an arbitrary BoundedWindow.
    return coder;
  }

  @Override
  public boolean assignsToSingleWindow() {
    return assignsToSingleWindow;
  }

  @Override
  public BoundedWindow getSideInputWindow(BoundedWindow window) {
    throw new UnsupportedOperationException(
        String.format(
            "%s.getSideInputWindow() should never be called."
                + " It is a private implementation detail of sdk utilities."
                + " This message indicates a bug in the Beam SDK.",
            getClass().getCanonicalName()));
  }

  @Deprecated
  @Override
  public Instant getOutputTime(Instant inputTimestamp, BoundedWindow window) {
    return inputTimestamp;
  }
}
