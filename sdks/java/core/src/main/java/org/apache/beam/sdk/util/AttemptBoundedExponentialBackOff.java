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
 * <p>Example: The initial interval is .5 seconds and the maximum number of retries is 10.
 * For 10 tries the sequence will be (values in seconds):
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
 * 10           {@link BackOff#STOP}
 * </pre>
 *
 * <p>Implementation is not thread-safe.
 */
@Deprecated
public class AttemptBoundedExponentialBackOff implements BackOff {
  public static final double DEFAULT_MULTIPLIER = 1.5;
  public static final double DEFAULT_RANDOMIZATION_FACTOR = 0.5;
  private final int maximumNumberOfAttempts;
  private final long initialIntervalMillis;
  private int currentAttempt;

  public AttemptBoundedExponentialBackOff(int maximumNumberOfAttempts, long initialIntervalMillis) {
    checkArgument(maximumNumberOfAttempts > 0,
        "Maximum number of attempts must be greater than zero.");
    checkArgument(initialIntervalMillis > 0, "Initial interval must be greater than zero.");
    this.maximumNumberOfAttempts = maximumNumberOfAttempts;
    this.initialIntervalMillis = initialIntervalMillis;
    reset();
  }

  @Override
  public void reset() {
    currentAttempt = 1;
  }

  @Override
  public long nextBackOffMillis() {
    if (currentAttempt >= maximumNumberOfAttempts) {
      return BackOff.STOP;
    }
    double currentIntervalMillis = initialIntervalMillis
        * Math.pow(DEFAULT_MULTIPLIER, currentAttempt - 1);
    double randomOffset = (Math.random() * 2 - 1)
        * DEFAULT_RANDOMIZATION_FACTOR * currentIntervalMillis;
    currentAttempt += 1;
    return Math.round(currentIntervalMillis + randomOffset);
  }

  public boolean atMaxAttempts() {
    return currentAttempt >= maximumNumberOfAttempts;
  }
}
