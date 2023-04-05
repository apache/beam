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

import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.runners.core.metrics.CounterCell;
import org.apache.beam.runners.core.metrics.DirtyState;
import org.apache.beam.runners.core.metrics.MetricCell;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricName;

/**
 * Version of {@link CounterCell} supporting multi-thread safe mutations and extraction of delta
 * values.
 */
public class DeltaCounterCell implements Counter, MetricCell<Long> {

  private final MetricName name;
  private final AtomicLong value = new AtomicLong();

  public DeltaCounterCell(MetricName name) {
    this.name = name;
  }

  @Override
  public void reset() {
    value.set(0L);
  }

  @Override
  public void inc(long n) {
    value.addAndGet(n);
  }

  @Override
  public void inc() {
    inc(1);
  }

  @Override
  public void dec() {
    inc(-1);
  }

  @Override
  public void dec(long n) {
    inc(-1 * n);
  }

  @Override
  public MetricName getName() {
    return name;
  }

  @Override
  public DirtyState getDirty() {
    throw new UnsupportedOperationException(
        String.format("%s doesn't support the getDirty", getClass().getSimpleName()));
  }

  @Override
  public Long getCumulative() {
    throw new UnsupportedOperationException("getCumulative is not supported by Streaming Metrics");
  }

  public Long getSum() {
    return value.get();
  }

  public Long getSumAndReset() {
    return value.getAndSet(0);
  }
}
