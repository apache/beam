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
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.Set;

/**
 * A {@link SideInputReader} representing a well-defined set of views, but not storing
 * any values for them. Used to check if a side input is present when the data itself
 * comes from elsewhere.
 */
public class NullSideInputReader implements SideInputReader {

  private Set<PCollectionView<?>> views;

  public static NullSideInputReader empty() {
    return new NullSideInputReader(Collections.<PCollectionView<?>>emptySet());
  }

  public static NullSideInputReader of(Iterable<? extends PCollectionView<?>> views) {
    return new NullSideInputReader(views);
  }

  private NullSideInputReader(Iterable<? extends PCollectionView<?>> views) {
    this.views = Sets.newHashSet(views);
  }

  @Override
  public <T> T get(PCollectionView<T> view, BoundedWindow window) {
    throw new IllegalArgumentException("cannot call NullSideInputReader.get()");
  }

  @Override
  public boolean isEmpty() {
    return views.isEmpty();
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return views.contains(view);
  }
}
