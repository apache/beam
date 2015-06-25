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
import com.google.cloud.dataflow.sdk.util.Sized;
import com.google.cloud.dataflow.sdk.util.SizedSideInputReader;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.util.concurrent.UncheckedExecutionException;

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
    extends SizedSideInputReader.Defaults
    implements SizedSideInputReader {
  private SizedSideInputReader subReader;
  private Cache<PCollectionViewWindow<?>, Sized<Object>> cache;

  private CachingSideInputReader(
      SizedSideInputReader subReader, Cache<PCollectionViewWindow<?>, Sized<Object>> cache) {
    this.subReader = subReader;
    this.cache = cache;
  }

  public static CachingSideInputReader of(
      SizedSideInputReader subReader, Cache<PCollectionViewWindow<?>, Sized<Object>> cache) {
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
  public <T> Sized<T> getSized(
      final PCollectionView<T> view, final BoundedWindow window) {
    PCollectionViewWindow<T> cacheKey = PCollectionViewWindow.of(view, window);

      try {
        @SuppressWarnings("unchecked") // safely uncasting the thing from the callback
        Sized<T> sideInputContents = (Sized<T>) cache.get(cacheKey,
            new Callable<Sized<Object>>() {
              @Override
              public Sized<Object> call() {
                @SuppressWarnings("unchecked") // safe covariant cast
                Sized<Object> value = (Sized<Object>) subReader.getSized(view, window);
                return value;
              }
            });
        return sideInputContents;
      } catch (ExecutionException | UncheckedExecutionException exc) {
        throw Throwables.propagate(exc.getCause());
      }
    }
  }
