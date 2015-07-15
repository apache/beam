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

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;

import java.util.Collections;

/**
 * Implementation of {@link ActiveWindowSet} used with {@link WindowFn WindowFns} that don't support
 * merging.
 *
 * @param <W> the types of windows being managed
 */
public class NonMergingActiveWindowSet<W extends BoundedWindow>
    implements ActiveWindowSet<W> {

  @Override
  public void persist() {
    // Nothing to persist.
  }

  @Override
  public boolean add(W window) {
    // We don't track anything, so we cannot determine if the window is new or not.
    return true;
  }

  @Override
  public void remove(W window) {}

  @Override
  public boolean mergeIfAppropriate(W window, MergeCallback<W> reduceFnRunner)
      throws Exception {
    // We never merge, so there is nothing to do here.
    // The window (which existed before the merge) must still exist after the merge.
    return true;
  }

  @Override
  public Iterable<W> sourceWindows(W window) {
    // There is no merging, so the only source window is the window itself.
    return Collections.singleton(window);
  }
}
