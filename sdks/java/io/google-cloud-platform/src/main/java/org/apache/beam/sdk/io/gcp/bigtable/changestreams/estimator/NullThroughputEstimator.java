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

import org.apache.beam.sdk.annotations.Internal;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NoOp implementation of a throughput estimator. This will always return 0 as the throughput and it
 * will warn users that this is being used (it should not be used in production).
 */
@Internal
public class NullThroughputEstimator<T> implements ThroughputEstimator<T> {

  private static final long serialVersionUID = 7088120208289907630L;
  private static final Logger LOG = LoggerFactory.getLogger(NullThroughputEstimator.class);

  /**
   * NoOp.
   *
   * @param timeOfRecords ignored
   * @param element ignored
   */
  @Override
  public void update(Instant timeOfRecords, T element) {
    LOG.warn(
        "Trying to update throughput using {}, this operation will have no effect",
        this.getClass().getSimpleName());
  }

  /**
   * Always returns 0.
   *
   * @param time ignored
   * @return 0
   */
  @Override
  public double getFrom(Instant time) {
    LOG.warn(
        "Trying to retrieve throughput using {}, this operation will always return 0",
        this.getClass().getSimpleName());
    return 0;
  }
}
