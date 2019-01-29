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
package org.apache.beam.fn.harness.data;

import java.io.Closeable;
import java.util.HashMap;
import org.apache.beam.runners.core.metrics.LabeledMetrics;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.runners.core.metrics.SimpleMonitoringInfoBuilder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * A wrapping {@code FnDataReceiver<WindowedValue<T>>} which counts the number of elements consumed
 * by the original {@code FnDataReceiver<WindowedValue<T>>}.
 *
 * @param <T> - The receiving type of the PTransform.
 */
public class ElementCountFnDataReceiver<T> implements FnDataReceiver<WindowedValue<T>> {

  private FnDataReceiver<WindowedValue<T>> original;
  private Counter counter;
  private MetricsContainer unboundMetricContainer;

  public ElementCountFnDataReceiver(
      FnDataReceiver<WindowedValue<T>> original, String pCollection,
      MetricsContainerStepMap metricsContainerStepMap) {
    this.original = original;
    HashMap<String, String> labels = new HashMap<String, String>();
    labels.put(SimpleMonitoringInfoBuilder.PCOLLECTION_LABEL, pCollection);
    MonitoringInfoMetricName metricName =
        MonitoringInfoMetricName.named(SimpleMonitoringInfoBuilder.ELEMENT_COUNT_URN, labels);
    this.counter = LabeledMetrics.counter(metricName);
    // Place it in a metric container which is not bound to the step name.
    // Though, this is not likely to happen for ElementCount because the producing pTransform for
    // the pCollection always invokes the consumer.
    this.unboundMetricContainer = metricsContainerStepMap.getUnboundContainer();
  }

  @Override
  public void accept(WindowedValue<T> input) throws Exception {
    try (Closeable close = MetricsEnvironment.scopedMetricsContainer(this.unboundMetricContainer)) {
      // Increment the counter for each window the element occurs in.
      this.counter.inc(input.getWindows().size());
      this.original.accept(input);
    }
  }
}
