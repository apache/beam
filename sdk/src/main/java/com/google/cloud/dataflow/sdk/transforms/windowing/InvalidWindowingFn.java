/*
 * Copyright (C) 2014 Google Inc.
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

import java.util.Collection;

/**
 * A {@link WindowingFn} that represents an invalid pipeline state.
 *
 * @param <W> window type
 */
public class InvalidWindowingFn<W extends BoundedWindow> extends WindowingFn<Object, W> {
  private String cause;
  private WindowingFn<?, W> originalWindowingFn;

  public InvalidWindowingFn(String cause, WindowingFn<?, W> originalWindowingFn) {
    this.originalWindowingFn = originalWindowingFn;
    this.cause = cause;
  }

  /**
   * Returns the reason that this {@code WindowingFn} is invalid.
   */
  public String getCause() {
    return cause;
  }

  /**
   * Returns the original windowingFn that this InvalidWindowingFn replaced.
   */
  public WindowingFn<?, W> getOriginalWindowingFn() {
    return originalWindowingFn;
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
    return originalWindowingFn.windowCoder();
  }

  /**
   * {@code InvalidWindowingFn} objects with the same {@code originalWindowingFn} are compatible.
   */
  @Override
  public boolean isCompatible(WindowingFn other) {
    return getClass() == other.getClass()
        && getOriginalWindowingFn().isCompatible(
            ((InvalidWindowingFn) other).getOriginalWindowingFn());
  }
}
