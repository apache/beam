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
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Set;

/**
 * Implementation of {@link ActiveWindowSet} used with {@link WindowFn WindowFns} that don't support
 * merging.
 *
 * @param <W> the types of windows being managed
 */
public class NonMergingActiveWindowSet<W extends BoundedWindow> implements ActiveWindowSet<W> {
  @Override
  public void removeEphemeralWindows() {}

  @Override
  public void persist() {}

  @Override
  public W representative(W window) {
    // Always represented by itself.
    return window;
  }

  @Override
  public Set<W> getActiveWindows() {
    // Only supported when merging.
    throw new java.lang.UnsupportedOperationException();
  }

  @Override
  public boolean isActive(W window) {
    // Windows should never disappear, since we don't support merging.
    return true;
  }

  @Override
  public void addNew(W window) {}

  @Override
  public void addActive(W window) {}

  @Override
  public void remove(W window) {}

  @Override
  public void merge(MergeCallback<W> mergeCallback) throws Exception {}

  @Override
  public void merged(W window) {}

  @Override
  public Set<W> readStateAddresses(W window) {
    return ImmutableSet.of(window);
  }

  @Override
  public W writeStateAddress(W window) {
    return window;
  }

  @Override
  public W mergedWriteStateAddress(Collection<W> toBeMerged, W mergeResult) {
    return mergeResult;
  }
}
