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
package org.apache.beam.sdk.schemas;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A wrapper around a {@link Factory} that assumes the schema parameter never changes.
 *
 * <p>{@link Factory} objects take the schema as a parameter, as often the returned type varies by
 * schema (e.g. sometimes the returned-type is a list that must be in schema-field order). However
 * in many cases it's known by the caller that the schema parameter is always the same across all
 * calls to create. In these cases we want to save the cost of Schema comparison (which can be
 * significant for larger schemas) on each lookup. This wrapper caches the value returned by the
 * inner factory, so the schema comparison only need happen on the first lookup.
 */
class CachingFactory<CreatedT> implements Factory<CreatedT> {
  private transient @Nullable ConcurrentHashMap<Class, CreatedT> cache = null;

  private final Factory<CreatedT> innerFactory;

  public CachingFactory(Factory<CreatedT> innerFactory) {
    this.innerFactory = innerFactory;
  }

  @Override
  public CreatedT create(Class<?> clazz, Schema schema) {
    if (cache == null) {
      cache = new ConcurrentHashMap<>();
    }
    CreatedT cached = cache.get(clazz);
    if (cached != null) {
      return cached;
    }
    cached = innerFactory.create(clazz, schema);
    cache.put(clazz, cached);
    return cached;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CachingFactory<?> that = (CachingFactory<?>) o;
    return innerFactory.equals(that.innerFactory);
  }

  @Override
  public int hashCode() {
    return Objects.hash(innerFactory);
  }
}
