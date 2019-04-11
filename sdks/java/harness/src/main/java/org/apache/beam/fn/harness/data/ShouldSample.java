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
package org.apache.beam.fn.harness.data;

/**
 * Class which decides whether or not sampling should occur on an element.
 *
 * <p>If the elementCoder is cheap, we always sample. Otherwise we sample with a probability.
 *
 * <p>Sampling probability decreases as the element count is increasing. We unconditionally sample
 * the first samplingCutoff elements. For the next samplingCutoff elements, the sampling probability
 * drops from 100% to 50%. The probability of sampling the Nth element is: min(1, samplingCutoff /
 * N), with an additional lower bound of samplingCutoff / samplingTokenUpperBound. This algorithm
 * may be refined later.
 */
public class ShouldSample<T> {
  private static final int SAMPLING_TOKEN_UPPER_BOUND = 1000000;
  private final org.apache.beam.sdk.coders.Coder elementCoder;
  private int samplingToken = 0;
  public static final int SAMPLING_CUTOFF = 10;
  private java.util.Random randomGenerator = new java.util.Random();

  public ShouldSample(org.apache.beam.sdk.coders.Coder<T> elementCoder) {
    this.elementCoder = elementCoder;
  }

  /** @returns true if the element should be sampled. */
  public boolean shouldSampleElement(T value) {
    if (this.elementCoder.isRegisterByteSizeObserverCheap(value)) {
      return true;
    }
    samplingToken = Math.min(samplingToken + 1, SAMPLING_TOKEN_UPPER_BOUND);
    int randomInt = randomGenerator.nextInt(samplingToken);
    return randomInt < SAMPLING_CUTOFF;
  }
}
