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

package com.google.cloud.dataflow.sdk.transforms.windowing;

import com.google.cloud.dataflow.sdk.coders.Coder;

import org.joda.time.Instant;

import java.util.Collection;

/**
 * A {@link WindowFn} that represents an invalid pipeline state.
 *
 * @param <W> window type
 */
public class InvalidWindows<W extends BoundedWindow> extends WindowFn<Object, W> {
  private String cause;
  private WindowFn<?, W> originalWindowFn;

  public InvalidWindows(String cause, WindowFn<?, W> originalWindowFn) {
    this.originalWindowFn = originalWindowFn;
    this.cause = cause;
  }

  /**
   * Returns the reason that this {@code WindowFn} is invalid.
   */
  public String getCause() {
    return cause;
  }

  /**
   * Returns the original windowFn that this InvalidWindows replaced.
   */
  public WindowFn<?, W> getOriginalWindowFn() {
    return originalWindowFn;
  }

  @Override
  public Collection<W> assignWindows(AssignContext c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void mergeWindows(MergeContext c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Coder<W> windowCoder() {
    return originalWindowFn.windowCoder();
  }

  /**
   * {@code InvalidWindows} objects with the same {@code originalWindowFn} are compatible.
   */
  @Override
  public boolean isCompatible(WindowFn<?, ?> other) {
    return getClass() == other.getClass()
        && getOriginalWindowFn().isCompatible(
            ((InvalidWindows<?>) other).getOriginalWindowFn());
  }

  @Override
  public W getSideInputWindow(BoundedWindow window) {
    throw new UnsupportedOperationException("InvalidWindows is not allowed in side inputs");
  }

  @Override
  public Instant getOutputTime(Instant inputTimestamp, W window) {
    return inputTimestamp;
  }
}
