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

import java.util.Objects;

/**
 * A pair of a {@link PCollectionView} and a {@link BoundedWindow}, which can
 * be thought of as window "of" the view. This is a value class for use e.g.
 * as a compound cache key.
 *
 * @param <T> the type of the underlying PCollectionView
 */
public final class PCollectionViewWindow<T> {

  private final PCollectionView<T> view;
  private final BoundedWindow window;

  private PCollectionViewWindow(PCollectionView<T> view, BoundedWindow window) {
    this.view = view;
    this.window = window;
  }

  public static <T> PCollectionViewWindow<T> of(PCollectionView<T> view, BoundedWindow window) {
    return new PCollectionViewWindow<>(view, window);
  }

  public PCollectionView<T> getView() {
    return view;
  }

  public BoundedWindow getWindow() {
    return window;
  }

  @Override
  public boolean equals(Object otherObject) {
    if (!(otherObject instanceof PCollectionViewWindow)) {
      return false;
    }
    @SuppressWarnings("unchecked")
    PCollectionViewWindow<T> other = (PCollectionViewWindow<T>) otherObject;
    return getView().equals(other.getView()) && getWindow().equals(other.getWindow());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getView(), getWindow());
  }
}
