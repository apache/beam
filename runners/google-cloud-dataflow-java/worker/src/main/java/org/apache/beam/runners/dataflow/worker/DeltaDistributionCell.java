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
package org.apache.beam.runners.dataflow.worker;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.core.metrics.DirtyState;
import org.apache.beam.runners.core.metrics.DistributionCell;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.MetricCell;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.MetricName;

/**
 * A version of {@link DistributionCell} that supports extracting the delta and clearing it out.
 *
 * <p>TODO: Modify the Beam DistributionCell to support extracting the delta.
 */
public class DeltaDistributionCell implements Distribution, MetricCell<DistributionData> {
  private final AtomicReference<DistributionData> value =
      new AtomicReference<>(DistributionData.EMPTY);
  private final MetricName name;

  public DeltaDistributionCell(MetricName name) {
    this.name = name;
  }

  /** Increment the distribution by the given amount. */
  @Override
  public void update(long n) {
    update(DistributionData.singleton(n));
  }

  void update(DistributionData data) {
    DistributionData original;
    do {
      original = value.get();
    } while (!value.compareAndSet(original, original.combine(data)));
  }

  @Override
  public void reset() {
    value.set(DistributionData.EMPTY);
  }

  @Override
  public void update(long sum, long count, long min, long max) {
    update(DistributionData.create(sum, count, min, max));
  }

  @Override
  public DirtyState getDirty() {
    throw new UnsupportedOperationException(
        String.format("%s doesn't support the getDirty", getClass().getSimpleName()));
  }

  @Override
  public DistributionData getCumulative() {
    throw new UnsupportedOperationException("getCumulative is not supported by Streaming Metrics");
  }

  public DistributionData getAndReset() {
    return value.getAndUpdate(unused -> DistributionData.EMPTY);
  }

  @Override
  public MetricName getName() {
    return name;
  }
}
