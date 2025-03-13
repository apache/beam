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
package org.apache.beam.sdk.extensions.euphoria.core.client.accumulators;

import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.translate.EuphoriaOptions;

/**
 * Provides access to an accumulator backend service. It is intended to be implemented by third
 * party to support different type of services.
 *
 * @deprecated Use Java SDK directly, Euphoria is scheduled for removal in a future release.
 */
@Audience(Audience.Type.EXECUTOR)
@Deprecated
public interface AccumulatorProvider {

  static AccumulatorProvider.Factory of(Pipeline pipeline) {
    return pipeline.getOptions().as(EuphoriaOptions.class).getAccumulatorProviderFactory();
  }

  /**
   * Get an existing instance of a counter or create a new one.
   *
   * @param name Unique name of the counter.
   * @return Instance of a counter. @Deprecated use {@link #getCounter(String, String)} instead
   */
  Counter getCounter(String name);

  /**
   * Get an existing instance of a counter or create a new one.
   *
   * @param namespace of counter (e.g. operator name)
   * @param name of the counter
   * @return Instance of a counter.
   */
  Counter getCounter(String namespace, String name);
  /**
   * Get an existing instance of a histogram or create a new one.
   *
   * @param name Unique name of the histogram.
   * @return Instance of a histogram. @Deprecated use {@link #getHistogram(String, String)} instead
   */
  Histogram getHistogram(String name);

  /**
   * Get an existing instance of a histogram or create a new one.
   *
   * @param namespace of histogram (e.g. operator name)
   * @param name of the counter
   * @return Instance of a counter.
   */
  Histogram getHistogram(String namespace, String name);

  /**
   * Get an existing instance of a timer or create a new one.
   *
   * @param name Unique name of the timer.
   * @return Instance of a timer. @Deprecated use {@link #getHistogram(String, String)} instead
   */
  Timer getTimer(String name);

  /**
   * Creates a new instance of {@link AccumulatorProvider} initialized by given settings.
   *
   * <p>It is required this factory is thread-safe.
   */
  @FunctionalInterface
  interface Factory extends Serializable {

    AccumulatorProvider create();
  }
}
