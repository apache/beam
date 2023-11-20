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
package org.apache.beam.sdk.extensions.euphoria.core.client.io;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Counter;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Histogram;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Timer;

/**
 * Defines basic methods available in user defined functions.
 *
 * @deprecated Use Java SDK directly, Euphoria is scheduled for removal in a future release.
 */
@Audience(Audience.Type.CLIENT)
@Deprecated
public interface Environment {

  // ---------------- Aggregator related methods ------------

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
}
