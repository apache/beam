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
package org.apache.beam.io.requestresponse;

import org.checkerframework.checker.nullness.qual.Nullable;

/** Reports whether to apply backoff based on https://sre.google/sre-book/handling-overload/. */
class CallShouldBackoffBasedOnRejectionProbability<ResponseT>
    implements CallShouldBackoff<ResponseT> {

  // Default multiplier value recommended by https://sre.google/sre-book/handling-overload/
  private static final double DEFAULT_MULTIPLIER = 2.0;

  // The threshold is the value that the rejection probability must exceed in order to report a
  // value() of true. If null, then the computation relies on a random value.
  private @Nullable Double threshold;

  // The multiplier drives the impact of accepts on the rejection probability. See <a
  // https://sre.google/sre-book/handling-overload/ for details.
  private final double multiplier;

  // The number of total requests called to a remote API.
  private double requests = 0;

  // The number of total accepts called to a remote API.
  private double accepts = 0;

  /** Instantiate class with the {@link #DEFAULT_MULTIPLIER}. */
  CallShouldBackoffBasedOnRejectionProbability() {
    this(DEFAULT_MULTIPLIER);
  }

  /**
   * Instantiates class with the provided multiplier. The multiplier drives the impact of accepts on
   * the rejection probability. See https://sre.google/sre-book/handling-overload/ for details.
   */
  CallShouldBackoffBasedOnRejectionProbability(double multiplier) {
    this.multiplier = multiplier;
  }

  /**
   * Setter for the threshold that overrides usage of a random value. The threshold is the value
   * (within range [0, 1)) that {@link #getRejectionProbability()} must exceed in order to report a
   * value() of true.
   */
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
  double getThreshold() {
    if (this.threshold != null) {
      return this.threshold;
    }
    return Math.random();
  }

  /**
   * Compute the probability of API call rejection based on
   * https://sre.google/sre-book/handling-overload/.
   */
  double getRejectionProbability() {
    double numerator = requests - multiplier * accepts;
    double denominator = requests + 1;
    double ratio = numerator / denominator;
    return Math.max(0, ratio);
  }

  /** Report whether to backoff. */
  @Override
  public boolean isTrue() {
    return getRejectionProbability() > getThreshold();
  }
}
