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
import java.util.Random;
import org.apache.beam.runners.core.metrics.LabeledMetrics;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants.Labels;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.PCollection;

/**
 * A wrapping {@code FnDataReceiver<WindowedValue<T>>} which counts the number of elements consumed
 * by the original {@code FnDataReceiver<WindowedValue<T>>}.
 *
 * @param <T> - The receiving type of the PTransform.
 */
public class ElementCountFnDataReceiver<T> implements FnDataReceiver<WindowedValue<T>> {

  private FnDataReceiver<WindowedValue<T>> original;
  private Counter elementCounter;
  private final Distribution sampledByteSizeDistribution;
  private final MetricsContainer unboundMetricContainer;

  private ElementByteSizeObserver observer;
  private static final int SAMPLING_TOKEN_UPPER_BOUND = 1000000;
  private int samplingToken = 0;
  private static final int SAMPLING_CUTOFF = 10;
  private Random randomGenerator = new Random();
  org.apache.beam.sdk.coders.Coder<T> elementCoder;

  public ElementCountFnDataReceiver(
      FnDataReceiver<WindowedValue<T>> original,
      String pCollection,
      MetricsContainerStepMap metricContainerRegistry,
      PCollection<?> pColl) {
    this.original = original;
    HashMap<String, String> labels = new HashMap<String, String>();
    labels.put(Labels.PCOLLECTION, pCollection);
    MonitoringInfoMetricName elementCountMetricName =
        MonitoringInfoMetricName.named(MonitoringInfoConstants.Urns.ELEMENT_COUNT, labels);
    this.elementCounter = LabeledMetrics.counter(elementCountMetricName);

    MonitoringInfoMetricName sampledByteSizeMetricName =
        MonitoringInfoMetricName.named(MonitoringInfoConstants.Urns.SAMPLED_BYTE_SIZE, labels);
    this.sampledByteSizeDistribution = LabeledMetrics.distribution(sampledByteSizeMetricName);
    // Collect the metric in a metric container which is not bound to the step name.
    // This is required to count elements from impulse steps, which will produce elements outside
    // of a pTransform context.
    this.unboundMetricContainer = metricContainerRegistry.getUnboundContainer();

    this.observer =
        new ElementByteSizeObserver() {
          @Override
          protected void reportElementSize(long elementByteSize) {
            sampledByteSizeDistribution.update(elementByteSize);
          }
        };
    this.elementCoder = (org.apache.beam.sdk.coders.Coder<T>) pColl.getCoder();
  }

  @Override
  public void accept(WindowedValue<T> input) throws Exception {
    try (Closeable close = MetricsEnvironment.scopedMetricsContainer(this.unboundMetricContainer)) {
      // Increment the counter for each window the element occurs in.
      this.elementCounter.inc(input.getWindows().size());

      boolean sample =
          this.elementCoder.isRegisterByteSizeObserverCheap(input.getValue())
              || shouldSampleElement();
      if (sample) {
        this.elementCoder.registerByteSizeObserver(input.getValue(), this.observer);
      }

      this.original.accept(input);

      // Calling advance triggers the call to reportElementSize immediately.
      // So it will be triggered.
      if (sample) {
        this.observer.advance();
      }
    }
  }

  protected boolean shouldSampleElement() {
    // Sampling probability decreases as the element count is increasing.
    // We unconditionally sample the first samplingCutoff elements. For the
    // next samplingCutoff elements, the sampling probability drops from 100%
    // to 50%. The probability of sampling the Nth element is:
    // min(1, samplingCutoff / N), with an additional lower bound of
    // samplingCutoff / samplingTokenUpperBound. This algorithm may be refined
    // later.
    samplingToken = Math.min(samplingToken + 1, SAMPLING_TOKEN_UPPER_BOUND);
    int randomInt = randomGenerator.nextInt(samplingToken);
    return randomInt < SAMPLING_CUTOFF;
  }
}
