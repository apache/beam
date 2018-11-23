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

import com.google.common.cache.Cache;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.spark.util.SideInputStorage.Key;
import org.apache.beam.runners.spark.util.SideInputStorage.Value;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.spark.util.SizeEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link SideInputReader} that caches materialized views. */
public class CachedSideInputReader implements SideInputReader {

  private static final Logger LOG = LoggerFactory.getLogger(CachedSideInputReader.class);

  /**
   * Keep references for the whole lifecycle of CachedSideInputReader otherwise sideInput needs to
   * be de-serialized again.
   */
  private Set<?> sideInputReferences = new HashSet<>();

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
    @SuppressWarnings("unchecked")
    final Set<Value<T>> sideInputReferencesCasted = (Set<Value<T>>) sideInputReferences;

    Value<T> value;
    try {
      value =
          materializedCasted.get(
              sideInputKey,
              () -> {
                final T result = delegate.get(view, window);
                LOG.info(
                    "Caching de-serialized side input for {} of size [{}B] in memory.",
                    sideInputKey,
                    SizeEstimator.estimate(result));
                return new Value<>(sideInputKey, result);
              });
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
    sideInputReferencesCasted.add(value);
    return value.getData();
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
