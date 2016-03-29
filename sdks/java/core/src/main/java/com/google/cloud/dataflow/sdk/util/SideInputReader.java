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

import javax.annotation.Nullable;

/**
 * The interface to objects that provide side inputs. Particular implementations
 * may read a side input directly or use appropriate sorts of caching, etc.
 */
public interface SideInputReader {
  /**
   * Returns the value of the given {@link PCollectionView} for the given {@link BoundedWindow}.
   *
   * <p>It is valid for a side input to be {@code null}. It is <i>not</i> valid for this to
   * return {@code null} for any other reason.
   */
  @Nullable
  <T> T get(PCollectionView<T> view, BoundedWindow window);

  /**
   * Returns true if the given {@link PCollectionView} is valid for this reader.
   */
  <T> boolean contains(PCollectionView<T> view);

  /**
   * Returns true if there are no side inputs in this reader.
   */
  boolean isEmpty();
}

