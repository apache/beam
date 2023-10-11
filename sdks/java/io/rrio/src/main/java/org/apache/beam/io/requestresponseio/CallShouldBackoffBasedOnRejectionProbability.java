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
package org.apache.beam.io.requestresponseio;

import org.checkerframework.checker.nullness.qual.Nullable;

/** Reports whether to apply backoff based on https://sre.google/sre-book/handling-overload/. */
class CallShouldBackoffBasedOnRejectionProbability<ResponseT>
    implements CallShouldBackoff<ResponseT> {

  // Default value recommended by https://sre.google/sre-book/handling-overload/
  private static final double DEFAULT_MULTIPLIER = 2.0;

  CallShouldBackoffBasedOnRejectionProbability() {
    this(DEFAULT_MULTIPLIER);
  }

  CallShouldBackoffBasedOnRejectionProbability(double multiplier) {
    this.multiplier = multiplier;
  }

  private @Nullable Double threshold;
  private final double multiplier;
  private double requests = 0;
  private double accepts = 0;

  CallShouldBackoffBasedOnRejectionProbability<ResponseT> setThreshold(double threshold) {
    this.threshold = threshold;
    return this;
  }

  /** Update the state of whether to backoff using information about the exception. */
  @Override
  public void update(UserCodeExecutionException exception) {
    this.requests++;
  }

  /** Update the state of whether to backoff using information about the response. */
  @Override
  public void update(ResponseT response) {
    this.requests++;
    this.accepts++;
  }

  /** Provide a threshold to evaluate backoff. */
  double threshold() {
    if (this.threshold != null) {
      return this.threshold;
    }
    return Math.random();
  }

  /**
   * Compute the probability of API call rejection based on
   * https://sre.google/sre-book/handling-overload/.
   */
  double pReject() {
    double numerator = requests - multiplier * accepts;
    double denominator = requests + 1;
    double ratio = numerator / denominator;
    return Math.max(0, ratio);
  }

  /** Report whether to backoff. */
  @Override
  public boolean value() {
    return pReject() > threshold();
  }
}
