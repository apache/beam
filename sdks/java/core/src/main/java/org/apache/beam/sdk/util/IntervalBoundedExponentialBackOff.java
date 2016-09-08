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
package org.apache.beam.sdk.util;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.util.BackOff;


/**
 * Implementation of {@link BackOff} that increases the back off period for each retry attempt
 * using a randomization function that grows exponentially.
 *
 * <p>Example: The initial interval is .5 seconds and the maximum interval is 60 secs.
 * For 14 tries the sequence will be (values in seconds):
 *
 * <pre>
 * retry#      retry_interval     randomized_interval
 * 1             0.5                [0.25,   0.75]
 * 2             0.75               [0.375,  1.125]
 * 3             1.125              [0.562,  1.687]
 * 4             1.687              [0.8435, 2.53]
 * 5             2.53               [1.265,  3.795]
 * 6             3.795              [1.897,  5.692]
 * 7             5.692              [2.846,  8.538]
 * 8             8.538              [4.269, 12.807]
 * 9            12.807              [6.403, 19.210]
 * 10           28.832              [14.416, 43.248]
 * 11           43.248              [21.624, 64.873]
 * 12           60.0                [30.0, 90.0]
 * 13           60.0                [30.0, 90.0]
 * 14           60.0                [30.0, 90.0]
 * </pre>
 *
 * <p>Implementation is not thread-safe.
 */
@Deprecated
public class IntervalBoundedExponentialBackOff implements BackOff {
  public static final double DEFAULT_MULTIPLIER = 1.5;
  public static final double DEFAULT_RANDOMIZATION_FACTOR = 0.5;
  private final long maximumIntervalMillis;
  private final long initialIntervalMillis;
  private int currentAttempt;

  public IntervalBoundedExponentialBackOff(long maximumIntervalMillis, long initialIntervalMillis) {
    checkArgument(maximumIntervalMillis > 0, "Maximum interval must be greater than zero.");
    checkArgument(initialIntervalMillis > 0, "Initial interval must be greater than zero.");
    this.maximumIntervalMillis = maximumIntervalMillis;
    this.initialIntervalMillis = initialIntervalMillis;
    reset();
  }

  @Override
  public void reset() {
    currentAttempt = 1;
  }

  @Override
  public long nextBackOffMillis() {
    double currentIntervalMillis =
        Math.min(
            initialIntervalMillis * Math.pow(DEFAULT_MULTIPLIER, currentAttempt - 1),
            maximumIntervalMillis);
    double randomOffset =
        (Math.random() * 2 - 1) * DEFAULT_RANDOMIZATION_FACTOR * currentIntervalMillis;
    currentAttempt += 1;
    return Math.round(currentIntervalMillis + randomOffset);
  }

  public boolean atMaxInterval() {
    return initialIntervalMillis * Math.pow(DEFAULT_MULTIPLIER, currentAttempt - 1)
        >= maximumIntervalMillis;
  }
}
