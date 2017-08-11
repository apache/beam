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

import java.io.Serializable;

import org.apache.beam.integration.nexmark.model.KnownSize;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * A monitor of elements with support for later retrieving their metrics.
 *
 * @param <T> Type of element we are monitoring.
 */
public class Monitor<T extends KnownSize> implements Serializable {
  private class MonitorDoFn extends DoFn<T, T> {
    final Counter elementCounter =
      Metrics.counter(name , prefix + ".elements");
    final Counter bytesCounter =
      Metrics.counter(name , prefix + ".bytes");
    final Distribution startTime =
      Metrics.distribution(name , prefix + ".startTime");
    final Distribution endTime =
      Metrics.distribution(name , prefix + ".endTime");
    final Distribution startTimestamp =
      Metrics.distribution(name , prefix + ".startTimestamp");
    final Distribution endTimestamp =
      Metrics.distribution(name , prefix + ".endTimestamp");

    @ProcessElement
    public void processElement(ProcessContext c) {
      elementCounter.inc();
      bytesCounter.inc(c.element().sizeInBytes());
      long now = System.currentTimeMillis();
      startTime.update(now);
      endTime.update(now);
      startTimestamp.update(c.timestamp().getMillis());
      endTimestamp.update(c.timestamp().getMillis());
      c.output(c.element());
    }
  }

  public final String name;
  public final String prefix;
  final MonitorDoFn doFn;
  final PTransform<PCollection<? extends T>, PCollection<T>> transform;

  public Monitor(String name, String prefix) {
    this.name = name;
    this.prefix = prefix;
    doFn = new MonitorDoFn();
    transform = ParDo.of(doFn);
  }

  public PTransform<PCollection<? extends T>, PCollection<T>> getTransform() {
    return transform;
  }
}
