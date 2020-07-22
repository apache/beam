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
package org.apache.beam.runners.spark.structuredstreaming.translation.utils;

import java.util.concurrent.ExecutionException;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.spark.structuredstreaming.translation.utils.SideInputStorage.Key;
import org.apache.beam.runners.spark.structuredstreaming.translation.utils.SideInputStorage.Value;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.Cache;
import org.apache.spark.util.SizeEstimator;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link SideInputReader} that caches materialized views. */
public class CachedSideInputReader implements SideInputReader {

  private static final Logger LOG = LoggerFactory.getLogger(CachedSideInputReader.class);

  /**
   * Create a new cached {@link SideInputReader}.
   *
   * @param delegate wrapped reader
   * @return cached reader
   */
  public static CachedSideInputReader of(SideInputReader delegate) {
    return new CachedSideInputReader(delegate);
  }

  /** Wrapped {@link SideInputReader} which results will be cached. */
  private final SideInputReader delegate;

  private CachedSideInputReader(SideInputReader delegate) {
    this.delegate = delegate;
  }

  @Nullable
  @Override
  public <T> T get(PCollectionView<T> view, BoundedWindow window) {
    @SuppressWarnings("unchecked")
    final Cache<Key<T>, Value<T>> materializedCasted =
        (Cache) SideInputStorage.getMaterializedSideInputs();

    Key<T> sideInputKey = new Key<>(view, window);

    try {
      Value<T> cachedResult =
          materializedCasted.get(
              sideInputKey,
              () -> {
                final T result = delegate.get(view, window);
                LOG.debug(
                    "Caching de-serialized side input for {} of size [{}B] in memory.",
                    sideInputKey,
                    SizeEstimator.estimate(result));

                return new Value<>(result);
              });
      return cachedResult.getValue();
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
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
