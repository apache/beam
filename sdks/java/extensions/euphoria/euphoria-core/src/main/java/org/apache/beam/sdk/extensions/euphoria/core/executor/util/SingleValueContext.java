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
package org.apache.beam.sdk.extensions.euphoria.core.executor.util;

import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Counter;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Histogram;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Timer;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Context;

/**
 * A {@code Context} that holds only single value. There is no window associated with the value, so
 * the {@code getWindow()} will always throw {@code UnsupportedOperationException}. This context
 * will free the value as soon as {@code getAndResetValue()} is called.
 */
@Audience(Audience.Type.EXECUTOR)
public class SingleValueContext<T> implements Context, Collector<T> {

  @Nullable final Context wrap;
  T value;

  public SingleValueContext() {
    this(null);
  }

  public SingleValueContext(Context wrap) {
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

  /** Retrieve window associated with the stored element. */
  @Override
  public Window<?> getWindow() throws UnsupportedOperationException {
    if (wrap == null) {
      throw new UnsupportedOperationException("The window is unknown in this context");
    }
    return wrap.getWindow();
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
