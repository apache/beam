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

package org.apache.beam.integration.nexmark;

import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Max.MaxLongFn;
import org.apache.beam.sdk.transforms.Min.MinLongFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum.SumLongFn;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;

/**
 * A monitor of elements with support for later retrieving their aggregators.
 *
 * @param <T> Type of element we are monitoring.
 */
public class Monitor<T extends KnownSize> implements Serializable {
  private class MonitorDoFn extends DoFn<T, T> {
    public final Aggregator<Long, Long> elementCounter =
        createAggregator(counterNamePrefix + "_elements", new SumLongFn());
    public final Aggregator<Long, Long> bytesCounter =
        createAggregator(counterNamePrefix + "_bytes", new SumLongFn());
    public final Aggregator<Long, Long> startTime =
        createAggregator(counterNamePrefix + "_startTime", new MinLongFn());
    public final Aggregator<Long, Long> endTime =
        createAggregator(counterNamePrefix + "_endTime", new MaxLongFn());
    public final Aggregator<Long, Long> startTimestamp =
        createAggregator("startTimestamp", new MinLongFn());
    public final Aggregator<Long, Long> endTimestamp =
        createAggregator("endTimestamp", new MaxLongFn());

    @Override
    public void processElement(ProcessContext c) {
      elementCounter.addValue(1L);
      bytesCounter.addValue(c.element().sizeInBytes());
      long now = System.currentTimeMillis();
      startTime.addValue(now);
      endTime.addValue(now);
      startTimestamp.addValue(c.timestamp().getMillis());
      endTimestamp.addValue(c.timestamp().getMillis());
      c.output(c.element());
    }
  }

  final MonitorDoFn doFn;
  final PTransform<PCollection<? extends T>, PCollection<T>> transform;
  private String counterNamePrefix;

  public Monitor(String name, String counterNamePrefix) {
    this.counterNamePrefix = counterNamePrefix;
    doFn = new MonitorDoFn();
    transform = ParDo.named(name + ".Monitor").of(doFn);
  }

  public PTransform<PCollection<? extends T>, PCollection<T>> getTransform() {
    return transform;
  }

  public Aggregator<Long, Long> getElementCounter() {
    return doFn.elementCounter;
  }

  public Aggregator<Long, Long> getBytesCounter() {
    return doFn.bytesCounter;
  }

  public Aggregator<Long, Long> getStartTime() {
    return doFn.startTime;
  }

  public Aggregator<Long, Long> getEndTime() {
    return doFn.endTime;
  }

  public Aggregator<Long, Long> getStartTimestamp() {
    return doFn.startTimestamp;
  }

  public Aggregator<Long, Long> getEndTimestamp() {
    return doFn.endTimestamp;
  }
}
