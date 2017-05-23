/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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
package cz.seznam.euphoria.core.client.io;

import cz.seznam.euphoria.core.client.accumulators.Counter;
import cz.seznam.euphoria.core.client.accumulators.Histogram;
import cz.seznam.euphoria.core.client.accumulators.Timer;

/**
 * Defines basic methods available in user defined functions.
 */
public interface Environment {

  /**
   * Retrieves the window - if any - underlying the current
   * execution of this context.
   *
   * @return {@code null} if this context is not executed within a
   *          windowing strategy, otherwise the current window of
   *          this context
   */
  Object getWindow();

  // ---------------- Aggregator related methods ------------

  // FIXME remove default implementation
  // it's just temporary to make the whole project compilable

  /**
   * Get an existing instance of a counter or create a new one.
   *
   * @param name Unique name of the counter.
   * @return Instance of a counter.
   */
  default Counter getCounter(String name) {
    throw new IllegalStateException("Accumulators not supported yet.");
  }

  /**
   * Get an existing instance of a histogram or create a new one.
   *
   * @param name Unique name of the histogram.
   * @return Instance of a histogram.
   */
  default Histogram getHistogram(String name) {
    throw new IllegalStateException("Accumulators not supported yet.");
  }

  /**
   * Get an existing instance of a timer or create a new one.
   *
   * @param name Unique name of the timer.
   * @return Instance of a timer.
   */
  default Timer getTimer(String name) {
    throw new IllegalStateException("Accumulators not supported yet.");
  }
}
