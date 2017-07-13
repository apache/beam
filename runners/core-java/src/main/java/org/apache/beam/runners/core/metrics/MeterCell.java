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

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.metrics.Meter;
import org.apache.beam.sdk.metrics.MetricName;

/**
 * The MeterCell wraps a dropwizard Meter.
 *
 * <p>This class generally shouldn't be used directly. The only exception is within a runner where
 * a meter is being reported for a specific step (rather than the meter in the current context).
 * In that case retrieving the underlying cell and reporting directly to it avoids a step of
 * indirection.
 */
@Experimental(Kind.METRICS)
public class MeterCell implements Meter, MetricCell<MeterData> {

  private final DirtyState dirty = new DirtyState();
  private transient com.codahale.metrics.Meter value = new com.codahale.metrics.Meter();
  private final MetricName name;

  public MeterCell(MetricName name) {
    this.name = name;
  }

  /**
   * Increment the meter by the given amount.
   *
   * @param n value to increment by.
   */
  @Override
  public void mark(long n) {
    value.mark(n);
    dirty.afterModification();
  }

  public void mark() {
    mark(1);
  }

  @Override
  public DirtyState getDirty() {
    return dirty;
  }

  @Override
  public MeterData getCumulative() {
    return MeterData.create(
        value.getCount(),
        value.getOneMinuteRate(),
        value.getFiveMinuteRate(),
        value.getFifteenMinuteRate(),
        value.getMeanRate());
  }

  @Override
  public MetricName getName() {
    return name;
  }
}
