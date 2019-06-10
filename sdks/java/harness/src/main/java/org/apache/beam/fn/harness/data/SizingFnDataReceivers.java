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
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.beam.runners.core.metrics.LabeledMetrics;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.MetricsContainer;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SizingFnDataReceivers {

  public static <T> FnDataReceiver<WindowedValue<T>> forPCollection(
          MetricsContainer metricsContainer, String pCollectionId, Coder<T> elementCoder, FnDataReceiver<WindowedValue<T>> delegate) {

    // Collect the metric in a metric container which is not bound to the step name.
    // This is required to count elements from impulse steps, which will produce elements outside
    // of a pTransform context.
    return new SamplingSizingFnDataReceiver<>(
            metricsContainer, pCollectionId, delegate, elementCoder, ThreadLocalRandom.current());
  }

  private static class SamplingSizingFnDataReceiver<T> extends ElementByteSizeObserver implements FnDataReceiver<WindowedValue<T>> {
    // Lowest sampling probability: 0.001%.
    private static final int SAMPLING_TOKEN_UPPER_BOUND = 1000000;
    private static final int SAMPLING_CUTOFF = 10;
    private final Counter elementCounter;
    private final Distribution sampledByteSizeDistribution;
    private final MetricsContainer metricsContainer;
    private int samplingToken = 0;

    private final FnDataReceiver<WindowedValue<T>> delegate;
    private final Coder<T> elementCoder;
    private final Random randomGenerator;

    private SamplingSizingFnDataReceiver(
            MetricsContainer metricsContainer,
            String pCollectionId,
            FnDataReceiver<WindowedValue<T>> delegate,
            Coder<T> elementCoder,
            Random randomGenerator) {
      this.delegate = delegate;
      this.elementCoder = elementCoder;
      this.randomGenerator = randomGenerator;

      Map<String, String> labels = ImmutableMap.of(MonitoringInfoConstants.Labels.PCOLLECTION, pCollectionId);
      MonitoringInfoMetricName elementCountMetricName =
              MonitoringInfoMetricName.named(MonitoringInfoConstants.Urns.ELEMENT_COUNT, labels);
      this.elementCounter = LabeledMetrics.counter(elementCountMetricName);

      MonitoringInfoMetricName sampledByteSizeMetricName =
              MonitoringInfoMetricName.named(MonitoringInfoConstants.Urns.SAMPLED_BYTE_SIZE, labels);
      this.sampledByteSizeDistribution = LabeledMetrics.distribution(sampledByteSizeMetricName);
      this.metricsContainer = metricsContainer;
    }

    @Override
    public void accept(WindowedValue<T> input) throws Exception {
      try (Closeable close = MetricsEnvironment.scopedMetricsContainer(this.metricsContainer)) {
        this.elementCounter.inc(input.getWindows().size());
        boolean shouldSample = shouldSample(input);
        if (shouldSample) {
          elementCoder.registerByteSizeObserver(input.getValue(), this);
        }
        delegate.accept(input);
        if (shouldSample) {
          advance();
        }
      }
    }

    private boolean shouldSample(WindowedValue<T> input) {
      if (elementCoder.isRegisterByteSizeObserverCheap(input.getValue())) {
        return true;
      }
      // Sampling probability decreases as the element count is increasing.
      // We unconditionally sample the first samplingCutoff elements. For the
      // next samplingCutoff elements, the sampling probability drops from 100%
      // to 50%. The probability of sampling the Nth element is:
      // min(1, samplingCutoff / N), with an additional lower bound of
      // samplingCutoff / samplingTokenUpperBound. This algorithm may be refined
      // later.
      samplingToken = Math.min(samplingToken + 1, SAMPLING_TOKEN_UPPER_BOUND);
      return (randomGenerator.nextInt(samplingToken) < SAMPLING_CUTOFF);
    }

    @Override
    protected void reportElementSize(long elementByteSize) {
      sampledByteSizeDistribution.update(elementByteSize);
    }

    @Override
    public void setLazy() {
      throw new UnsupportedOperationException("TODO: Support lazy observation of element sizes.");
    }
  }
}
