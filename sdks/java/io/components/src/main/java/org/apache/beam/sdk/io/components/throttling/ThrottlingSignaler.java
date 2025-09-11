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
package org.apache.beam.sdk.io.components.throttling;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
/**
 * The ThrottlingSignaler is a utility class for IOs to signal to the runner
 * that a process is being throttled, preventing autoscaling. This is primarily
 * used when making calls to a remote service where quotas and rate limiting
 * are reasonable considerations.
 */
public class ThrottlingSignaler {
  private Counter throttle_counter;

  public ThrottlingSignaler(String namespace) {
    this.throttle_counter = Metrics.counter(namespace, Metrics.THROTTLE_TIME_COUNTER_NAME);
  }

  public ThrottlingSignaler() {
    ThrottlingSignaler("");
  }

  /** 
   * Signal that a transform has been throttled for an amount of time
   * represented in milliseconds.
   */
  public void signalThrottling(long milliseconds) {
    throttle_counter.inc(milliseconds);
  }
}
