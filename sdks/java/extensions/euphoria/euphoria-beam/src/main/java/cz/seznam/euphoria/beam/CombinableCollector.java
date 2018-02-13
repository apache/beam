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
package cz.seznam.euphoria.beam;

import cz.seznam.euphoria.core.client.accumulators.Counter;
import cz.seznam.euphoria.core.client.accumulators.Histogram;
import cz.seznam.euphoria.core.client.accumulators.Timer;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.io.Context;
import java.io.Serializable;

/**
 * {@code Collector} for combinable functors.
 */
public class CombinableCollector<T> implements Collector<T>, Serializable {

  private T elem;

  public T get() {
    return elem;
  }

  @Override
  public void collect(T elem) {
    this.elem = elem;
  }

  @Override
  public Context asContext() {
    // this is not needed, the underlaying functor does not have access to this
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Window<?> getWindow() {
    // this is not needed, the underlaying functor does not have access to this
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Counter getCounter(String name) {
    // this is not needed, the underlaying functor does not have access to this
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Histogram getHistogram(String name) {
    // this is not needed, the underlaying functor does not have access to this
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Timer getTimer(String name) {
    // this is not needed, the underlaying functor does not have access to this
    throw new UnsupportedOperationException("Not supported.");
  }

}
