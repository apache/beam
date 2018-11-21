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

import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * Monitor that records processing time distribution in the pipeline.
 *
 * <p>To use: apply a monitor directly after each source and sink transform. This will capture a
 * distribution of element processing timestamps, which can be collected and queried using {@link
 * MetricsReader}.
 */
public class TimeMonitor<K, V> extends DoFn<KV<K, V>, KV<K, V>> {

  private Distribution timeDistribution;

  public TimeMonitor(String namespace, String name) {
    this.timeDistribution = Metrics.distribution(namespace, name);
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    timeDistribution.update(System.currentTimeMillis());
    c.output(c.element());
  }
}
