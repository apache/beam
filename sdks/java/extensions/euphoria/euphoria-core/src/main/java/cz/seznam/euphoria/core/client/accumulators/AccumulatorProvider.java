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
package cz.seznam.euphoria.core.client.accumulators;

import cz.seznam.euphoria.core.annotation.audience.Audience;
import cz.seznam.euphoria.core.util.Settings;
import java.io.Serializable;

/**
 * Provides access to an accumulator backend service. It is intended to be implemented by third
 * party to support different type of services.
 */
@Audience(Audience.Type.EXECUTOR)
public interface AccumulatorProvider {

  /**
   * Get an existing instance of a counter or create a new one.
   *
   * @param name Unique name of the counter.
   * @return Instance of a counter.
   */
  Counter getCounter(String name);

  /**
   * Get an existing instance of a histogram or create a new one.
   *
   * @param name Unique name of the histogram.
   * @return Instance of a histogram.
   */
  Histogram getHistogram(String name);

  /**
   * Get an existing instance of a timer or create a new one.
   *
   * @param name Unique name of the timer.
   * @return Instance of a timer.
   */
  Timer getTimer(String name);

  /**
   * Creates a new instance of {@link AccumulatorProvider} initialized by given settings.
   *
   * <p>It is required this factory is thread-safe.
   */
  @FunctionalInterface
  interface Factory extends Serializable {
    AccumulatorProvider create(Settings settings);
  }
}
