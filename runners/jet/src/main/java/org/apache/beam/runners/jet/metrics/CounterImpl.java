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
package org.apache.beam.runners.jet.metrics;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricName;

/** Implementation of {@link Counter}. */
public class CounterImpl extends AbstractMetric<Long> implements Counter {

  private long count = 0L;

  CounterImpl(MetricName metricName) {
    super(metricName);
  }

  @Override
  Long getValue() {
    return count;
  }

  @Override
  public void inc() {
    count++;
  }

  @Override
  public void inc(long n) {
    count += n;
  }

  @Override
  public void dec() {
    count--;
  }

  @Override
  public void dec(long n) {
    count -= n;
  }
}
