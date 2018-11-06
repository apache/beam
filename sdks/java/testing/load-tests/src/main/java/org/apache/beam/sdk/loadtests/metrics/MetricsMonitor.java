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
package org.apache.beam.sdk.loadtests.metrics;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/** Monitors various metrics from within a pipeline. */
public class MetricsMonitor extends DoFn<KV<byte[], byte[]>, KV<byte[], byte[]>> {

  private Distribution timeDistribution;

  private Counter totalBytes;

  public MetricsMonitor(String namespace) {
    this.timeDistribution = Metrics.distribution(namespace, "runtime");
    this.totalBytes = Metrics.counter(namespace, "totalBytes.count");
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    timeDistribution.update(System.currentTimeMillis());
    totalBytes.inc(c.element().getKey().length + c.element().getValue().length);
    c.output(c.element());
  }
}
