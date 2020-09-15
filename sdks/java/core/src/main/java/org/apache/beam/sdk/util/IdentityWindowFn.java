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

import java.util.Collection;
import java.util.Collections;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IncompatibleWindowException;
import org.apache.beam.sdk.transforms.windowing.InvalidWindows;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

/**
 * A {@link WindowFn} that leaves all associations between elements and windows unchanged.
 *
 * <p>This {@link WindowFn} is applied when elements must be passed through a {@link GroupByKey},
 * but should maintain their existing {@link Window} assignments. Because windows may have been
 * merged, the earlier {@link WindowFn} may not appropriately maintain the existing window
 * assignments. For example, if the earlier {@link WindowFn} merges windows, after a {@link
 * GroupByKey} the {@link WindowingStrategy} uses {@link InvalidWindows}, and no further {@link
 * GroupByKey} can be applied without applying a new {@link WindowFn}. This {@link WindowFn} allows
 * existing window assignments to be maintained across a single group by key, at which point the
 * earlier {@link WindowingStrategy} should be restored.
 *
 * <p>This {@link WindowFn} is an internal implementation detail of sdk-provided utilities, and
 * should not be used by {@link Pipeline} writers.
 */
@Internal
public class IdentityWindowFn<T> extends NonMergingWindowFn<T, BoundedWindow> {

  /**
   * The coder of the type of windows of the input {@link PCollection}. This is not an arbitrary
   * {@link BoundedWindow} {@link Coder}, but is safe to use for all windows assigned by this
   * transform, as it should be the same coder used by the {@link WindowFn} that initially assigned
   * these windows.
   */
  private final Coder<BoundedWindow> coder;

  public IdentityWindowFn(Coder<? extends BoundedWindow> coder) {
    // Safe because it is only used privately here.
    // At every point where a window is returned or accepted, it has been provided
    // by the prior WindowFn, so it is of the expected type.
    @SuppressWarnings("unchecked")
    Coder<BoundedWindow> windowCoder = (Coder<BoundedWindow>) coder;
    this.coder = windowCoder;
  }

  @Override
  public Collection<BoundedWindow> assignWindows(WindowFn<T, BoundedWindow>.AssignContext c)
      throws Exception {
    // The window is provided by the prior WindowFn, which also provides the coder for them
    return Collections.singleton(c.window());
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
  public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
    throw new UnsupportedOperationException(
        String.format(
            "%s.verifyCompatibility() should never be called."
                + " It is a private implementation detail of sdk utilities."
                + " This message indicates a bug in the Beam SDK.",
            getClass().getCanonicalName()));
  }

  @Override
  public Coder<BoundedWindow> windowCoder() {
    // Safe because the prior WindowFn provides both the windows and the coder.
    // The Coder is _not_ actually a coder for an arbitrary BoundedWindow.
    return coder;
  }

  @Override
  public WindowMappingFn<BoundedWindow> getDefaultWindowMappingFn() {
    throw new UnsupportedOperationException(
        String.format(
            "%s.getSideInputWindow() should never be called."
                + " It is a private implementation detail of sdk utilities."
                + " This message indicates a bug in the Beam SDK.",
            getClass().getCanonicalName()));
  }

  @Override
  public Instant getOutputTime(Instant inputTimestamp, BoundedWindow window) {
    return inputTimestamp;
  }

  @Override
  public boolean assignsToOneWindow() {
    return true;
  }
}
