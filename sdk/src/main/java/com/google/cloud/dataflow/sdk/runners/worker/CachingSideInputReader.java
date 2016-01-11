/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.PCollectionViewWindow;
import com.google.cloud.dataflow.sdk.util.SideInputReader;
import com.google.cloud.dataflow.sdk.util.WeightedSideInputReader;
import com.google.cloud.dataflow.sdk.util.WeightedValue;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.common.cache.Cache;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * A {@link SideInputReader} that maintains a cache of side inputs per window.
 *
 * <p>For internal use only.
 *
 * <p>Package-private here so that the dependency on Guava does not leak into the public API
 * surface. Note that Guava is "shaded" so the {@code Cache} class here is not actually compatible
 * with a {@code Cache} created by anything other than the SDK.
 */
final class CachingSideInputReader
    extends WeightedSideInputReader.Defaults
    implements WeightedSideInputReader {
  private final WeightedSideInputReader subReader;
  private final Cache<PCollectionViewWindow<?>, WeightedValue<Object>> cache;

  private CachingSideInputReader(WeightedSideInputReader subReader,
      Cache<PCollectionViewWindow<?>, WeightedValue<Object>> cache) {
    this.subReader = subReader;
    this.cache = cache;
  }

  public static CachingSideInputReader of(WeightedSideInputReader subReader,
      Cache<PCollectionViewWindow<?>, WeightedValue<Object>> cache) {
    return new CachingSideInputReader(subReader, cache);
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return subReader.contains(view);
  }

  @Override
  public boolean isEmpty() {
    return subReader.isEmpty();
  }

  @Override
  public <T> WeightedValue<T> getWeighted(
      final PCollectionView<T> view, final BoundedWindow window) {
    PCollectionViewWindow<T> cacheKey = PCollectionViewWindow.of(view, window);

      try {
        @SuppressWarnings("unchecked") // safely uncasting the thing from the callback
        WeightedValue<T> sideInputContents = (WeightedValue<T>) cache.get(cacheKey,
            new Callable<WeightedValue<Object>>() {
              @Override
              public WeightedValue<Object> call() {
                @SuppressWarnings("unchecked") // safe covariant cast
                WeightedValue<Object> value =
                    (WeightedValue<Object>) subReader.getWeighted(view, window);
                return value;
              }
            });
        return sideInputContents;
      } catch (ExecutionException checkedException) {
        // The call to subReader.getWeighted() is not permitted to throw any checked exceptions,
        // so the Callable created above should not throw any either.
        throw new RuntimeException("Unexpected checked exception.", checkedException.getCause());
      }
    }
  }
