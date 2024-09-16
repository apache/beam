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
package org.apache.beam.sdk.metrics;

import org.apache.beam.sdk.util.HistogramData;

/** A metric that reports information about the histogram of reported values. */
public interface Histogram extends Metric {
  /** Add an observation to this histogram. */
  void update(double value);

  /** Add observations to this histogram. */
  default void update(double... values) {
    for (double value : values) {
      update(value);
    }
  }
   
  /** Add a histogram to this histogram. Requires underlying implementation to implement this */
   default void update(HistogramData data) {}
}
