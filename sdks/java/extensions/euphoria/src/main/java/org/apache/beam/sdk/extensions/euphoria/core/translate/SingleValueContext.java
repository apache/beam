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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Counter;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Histogram;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Timer;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Context;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@code Context} that holds only single value. There is no window associated with the value, so
 * the {@code getWindow()} will always throw {@code UnsupportedOperationException}. This context
 * will free the value as soon as {@code getAndResetValue()} is called.
 *
 * @deprecated Use Java SDK directly, Euphoria is scheduled for removal in a future release.
 */
@Audience(Audience.Type.EXECUTOR)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Deprecated
public class SingleValueContext<T> implements Context, Collector<T> {

  private final @Nullable Context wrap;
  private T value;

  public SingleValueContext() {
    this(null);
  }

  public SingleValueContext(@Nullable Context wrap) {
    this.wrap = wrap;
  }

  /**
   * Replace the stored value with given one.
   *
   * @param elem the element to store
   */
  @Override
  public void collect(T elem) {
    value = elem;
  }

  @Override
  public Context asContext() {
    return this;
  }

  @Override
  public Counter getCounter(String name) {
    if (wrap == null) {
      throw new UnsupportedOperationException("Accumulators not supported in this context");
    }
    return wrap.getCounter(name);
  }

  @Override
  public Histogram getHistogram(String name) {
    if (wrap == null) {
      throw new UnsupportedOperationException("Accumulators not supported in this context");
    }
    return wrap.getHistogram(name);
  }

  @Override
  public Timer getTimer(String name) {
    if (wrap == null) {
      throw new UnsupportedOperationException("Accumulators not supported in this context");
    }
    return wrap.getTimer(name);
  }

  /**
   * Retrieve and reset the stored value to null.
   *
   * @return the stored value
   */
  public T getAndResetValue() {
    T ret = value;
    value = null;
    return ret;
  }

  /**
   * Retrieve value of this context.
   *
   * @return value
   */
  public T get() {
    return value;
  }
}
