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

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * The result of a {@link Distribution} metric.
 */
@Experimental(Kind.METRICS)
@AutoValue
public abstract class DistributionResult {

  public abstract long getSum();
  public abstract long getCount();
  public abstract long getMin();
  public abstract long getMax();

  public double getMean() {
    return (1.0 * getSum()) / getCount();
  }

  /** The IDENTITY_ELEMENT is used to start accumulating distributions. */
  public static final DistributionResult IDENTITY_ELEMENT =
      create(0, 0, Long.MAX_VALUE, Long.MIN_VALUE);

  public static DistributionResult create(long sum, long count, long min, long max) {
    return new AutoValue_DistributionResult(sum, count, min, max);
  }
}
