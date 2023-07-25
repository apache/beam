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
package org.apache.beam.runners.core.metrics;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.metrics.DistributionResult;

/**
 * Data describing the distribution. This should retain enough detail that it can be combined with
 * other {@link DistributionData}.
 *
 * <p>This is kept distinct from {@link DistributionResult} since this may be extended to include
 * data necessary to approximate quantiles, etc. while {@link DistributionResult} would just include
 * the approximate value of those quantiles.
 */
@AutoValue
public abstract class DistributionData implements Serializable {

  public abstract long sum();

  public abstract long count();

  public abstract long min();

  public abstract long max();

  public static final DistributionData EMPTY = create(0, 0, Long.MAX_VALUE, Long.MIN_VALUE);

  public static DistributionData create(long sum, long count, long min, long max) {
    return new AutoValue_DistributionData(sum, count, min, max);
  }

  public static DistributionData singleton(long value) {
    return create(value, 1, value, value);
  }

  public DistributionData combine(long value) {
    return create(sum() + value, count() + 1, Math.min(value, min()), Math.max(value, max()));
  }

  public DistributionData combine(long sum, long count, long min, long max) {
    return create(sum() + sum, count() + count, Math.min(min, min()), Math.max(max, max()));
  }

  public DistributionData combine(DistributionData value) {
    return create(
        sum() + value.sum(),
        count() + value.count(),
        Math.min(value.min(), min()),
        Math.max(value.max(), max()));
  }

  public DistributionResult extractResult() {
    return DistributionResult.create(sum(), count(), min(), max());
  }
}
