/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.spark.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;

/** {@link SideInputReader} that caches materialized views. */
public class CachedSideInputReader implements SideInputReader {

  /**
   * Create a new cached {@link SideInputReader}.
   *
   * @param delegate wrapped reader
   * @return cached reader
   */
  public static CachedSideInputReader of(SideInputReader delegate) {
    return new CachedSideInputReader(delegate);
  }

  /**
   * Composite key of {@link PCollectionView} and {@link BoundedWindow} used to identify
   * materialized results.
   *
   * @param <T> type of result
   */
  private static class Key<T> {

    private final PCollectionView<T> view;
    private final BoundedWindow window;

    Key(PCollectionView<T> view, BoundedWindow window) {
      this.view = view;
      this.window = window;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Key<?> key = (Key<?>) o;
      return Objects.equals(view, key.view) && Objects.equals(window, key.window);
    }

    @Override
    public int hashCode() {
      return Objects.hash(view, window);
    }
  }

  /** Wrapped {@link SideInputReader} which results will be cached. */
  private final SideInputReader delegate;

  /** Materialized results. */
  private final Map<Key<?>, ?> materialized = new HashMap<>();

  private CachedSideInputReader(SideInputReader delegate) {
    this.delegate = delegate;
  }

  @Nullable
  @Override
  public <T> T get(PCollectionView<T> view, BoundedWindow window) {
    @SuppressWarnings("unchecked")
    final Map<Key<T>, T> materializedCasted = (Map) materialized;
    return materializedCasted.computeIfAbsent(
        new Key<>(view, window), key -> delegate.get(view, window));
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return delegate.contains(view);
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }
}
