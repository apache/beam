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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.estimator;

import java.io.Serializable;
import org.apache.beam.sdk.annotations.Internal;
import org.joda.time.Instant;

/** An estimator to calculate the throughput of the outputted elements from a DoFn. */
@Internal
public interface ThroughputEstimator<T> extends Serializable {
  /**
   * Updates the estimator with the size of the records.
   *
   * @param timeOfRecords the committed timestamp of the records
   * @param element the element to estimate the byte size of
   */
  void update(Instant timeOfRecords, T element);

  /** Returns the estimated throughput for now. */
  default double get() {
    return getFrom(Instant.now());
  }

  /**
   * Returns the estimated throughput for a specified time.
   *
   * @param time the specified timestamp to check throughput
   */
  double getFrom(Instant time);
}
