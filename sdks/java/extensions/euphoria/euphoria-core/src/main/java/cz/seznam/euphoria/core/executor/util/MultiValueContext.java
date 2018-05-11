/*
 * Copyright 2016-2018 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.executor.util;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.client.accumulators.Counter;
import cz.seznam.euphoria.core.client.accumulators.Histogram;
import cz.seznam.euphoria.core.client.accumulators.Timer;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.Context;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

/** */
@Audience(Audience.Type.EXECUTOR)
public class MultiValueContext<T> implements Context, Collector<T> {

  @Nullable final Context wrap;
  private final List<T> elements = new ArrayList<>(1);

  public MultiValueContext() {
    this(null);
  }

  public MultiValueContext(Context wrap) {
    this.wrap = wrap;
  }

  /**
   * Replace the stored value with given one.
   *
   * @param elem the element to store
   */
  @Override
  public void collect(T elem) {
    elements.add(elem);
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
   * Retrieve and reset the stored elements.
   *
   * @return the stored value
   */
  public List<T> getAndResetValues() {
    List<T> copiedElements = new ArrayList<>(elements);
    elements.clear();
    return copiedElements;
  }

  /**
   * Retrieve value of this context.
   *
   * @return value
   */
  public List<T> get() {
    return Collections.unmodifiableList(elements);
  }
}
