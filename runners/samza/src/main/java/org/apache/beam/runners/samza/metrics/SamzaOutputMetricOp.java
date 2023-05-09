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
package org.apache.beam.runners.samza.metrics;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.samza.runtime.KeyedTimerData;
import org.apache.beam.runners.samza.runtime.Op;
import org.apache.beam.runners.samza.runtime.OpEmitter;
import org.apache.beam.runners.samza.util.PipelineJsonRenderer;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.samza.config.Config;
import org.apache.samza.context.Context;
import org.apache.samza.operators.Scheduler;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SamzaOutputMetricOp is a metric Op that emits & maintains default transform metrics for output
 * PCollection to the transform. It emits the output throughput and maintains avg arrival time for
 * output PCollection per watermark.
 *
 * <p>Assumes that {@code SamzaOutputMetricOp#processWatermark(Instant, OpEmitter)} is exclusive of
 * {@code SamzaOutputMetricOp#processElement(Instant, OpEmitter)}. Specifically, the
 * processWatermark method assumes that no calls to processElement will be made during its
 * execution, and vice versa.
 *
 * @param <T> The type of the elements in the output PCollection.
 */
class SamzaOutputMetricOp<T> implements Op<T, T, Void> {
  // Unique name of the PTransform this MetricOp is associated with
  protected final String transformFullName;
  protected final SamzaTransformMetricRegistry samzaTransformMetricRegistry;
  // Name or identifier of the PCollection which PTransform is processing
  protected final String pValue;
  // Counters for output throughput
  private final AtomicLong count;
  private final AtomicReference<BigInteger> sumOfTimestamps;
  // List of input PValue(s) for all PCollections processing the PTransform
  protected transient List<String> transformInputs;
  // List of output PValue(s) for all PCollections processing the PTransform
  protected transient List<String> transformOutputs;
  // Name of the task, for logging purpose
  protected transient String task;

  private static final Logger LOG = LoggerFactory.getLogger(SamzaOutputMetricOp.class);

  // Some fields are initialized in open() method, which is called after the constructor.
  @SuppressWarnings("initialization.fields.uninitialized")
  public SamzaOutputMetricOp(
      @NonNull String pValue,
      @NonNull String transformFullName,
      @NonNull SamzaTransformMetricRegistry samzaTransformMetricRegistry) {
    this.transformFullName = transformFullName;
    this.samzaTransformMetricRegistry = samzaTransformMetricRegistry;
    this.pValue = pValue;
    this.count = new AtomicLong(0L);
    this.sumOfTimestamps = new AtomicReference<>(BigInteger.ZERO);
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void open(
      Config config,
      Context context,
      Scheduler<KeyedTimerData<Void>> timerRegistry,
      OpEmitter<T> emitter) {
    final Map.Entry<List<String>, List<String>> transformInputOutput =
        PipelineJsonRenderer.getTransformIOMap(config).get(transformFullName);
    this.transformInputs =
        transformInputOutput != null ? transformInputOutput.getKey() : new ArrayList();
    this.transformOutputs =
        transformInputOutput != null ? transformInputOutput.getValue() : new ArrayList();
    // for logging / debugging purposes
    this.task = context.getTaskContext().getTaskModel().getTaskName().getTaskName();
    // Register the transform with SamzaTransformMetricRegistry
    samzaTransformMetricRegistry.register(transformFullName, pValue, context);
  }

  @Override
  public void processElement(WindowedValue<T> inputElement, OpEmitter<T> emitter) {
    // update counters for timestamps
    count.incrementAndGet();
    sumOfTimestamps.updateAndGet(sum -> sum.add(BigInteger.valueOf(System.nanoTime())));
    samzaTransformMetricRegistry
        .getTransformMetrics()
        .getTransformOutputThroughput(transformFullName)
        .inc();
    emitter.emitElement(inputElement);
  }

  @Override
  @SuppressWarnings({"CompareToZero"})
  public void processWatermark(Instant watermark, OpEmitter<T> emitter) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Processing Output Watermark for Transform: {} Count: {} SumOfTimestamps: {} for Watermark: {} for Task: {}",
          transformFullName,
          count.get(),
          sumOfTimestamps.get().longValue(),
          watermark.getMillis(),
          task);
    }

    // if there is no input data then counters will be zero and only watermark will progress
    if (count.get() > 0) {
      // if BigInt.longValue is out of range for long then only the low-order 64 bits are retained
      long avg = Math.floorDiv(sumOfTimestamps.get().longValue(), count.get());
      // Update MetricOp Registry with avg arrival for the pValue
      samzaTransformMetricRegistry.updateArrivalTimeMap(
          transformFullName, pValue, watermark.getMillis(), avg);
      // compute & emit the latency metric
      samzaTransformMetricRegistry.emitLatencyMetric(
          transformFullName, transformInputs, transformOutputs, watermark.getMillis(), task);
    }

    // update output watermark progress metric
    samzaTransformMetricRegistry
        .getTransformMetrics()
        .getTransformWatermarkProgress(transformFullName)
        .set(watermark.getMillis());

    // reset all counters
    count.set(0L);
    this.sumOfTimestamps.set(BigInteger.ZERO);
    emitter.emitWatermark(watermark);
  }

  @VisibleForTesting
  void init(List<String> transformInputs, List<String> transformOutputs) {
    this.transformInputs = transformInputs;
    this.transformOutputs = transformOutputs;
  }
}
