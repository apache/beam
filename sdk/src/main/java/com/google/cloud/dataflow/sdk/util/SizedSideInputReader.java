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

/**
 * Extension to {@link SideInputReader} that can approximate the size of the side input.
 */
public interface SizedSideInputReader extends SideInputReader {

  /**
   * Returns the value of the requested {@link PCollectionView} for the given {@link BoundedWindow}
   * along with a rough estimate of the number of bytes of memory it consumes.
   *
   * <p>It is valid for a side input value to be {@code null}. In this case, the return
   * value of this method must still be non-{@code null}. It should be a {@link Sized}
   * object where {@link Sized#getValue()} returns {@code null} and {@link Sized#getSize()} may
   * still return any non-negative value.
   */
  public <T> Sized<T> getSized(PCollectionView<T> view, BoundedWindow window);

  /**
   * Abstract class providing default implementations for methods of {@link SizedSideInputReader}.
   */
  public abstract static class Defaults implements SizedSideInputReader {
    @Override
    public <T> T get(PCollectionView<T> view, BoundedWindow window) {
      return getSized(view, window).getValue();
    }
  }
}
