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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.runners.samza.runtime.OpEmitter;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SamzaInputMetricOp is a {@link SamzaMetricOp} that emits & maintains default transform metrics
 * for input PCollection to the transform. It emits the input throughput and maintains avg arrival
 * time for input PCollection per watermark.
 *
 * <p>Assumes that {@code SamzaInputMetricOp#processWatermark(Instant, OpEmitter)} is exclusive of
 * {@code SamzaInputMetricOp#processElement(Instant, OpEmitter)}. Specifically, the processWatermark
 * method assumes that no calls to processElement will be made during its execution, and vice versa.
 *
 * @param <T> The type of the elements in the input PCollection.
 */
public class SamzaInputMetricOp<T> extends SamzaMetricOp<T> {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaInputMetricOp.class);
  // Counters to maintain avg arrival time per watermark for input PCollection.
  private final AtomicLong count;
  private AtomicReference<BigInteger> sumOfTimestamps;

  public SamzaInputMetricOp(
      String pValue,
      String transformFullName,
      SamzaTransformMetricRegistry samzaTransformMetricRegistry) {
    super(pValue, transformFullName, samzaTransformMetricRegistry);
    this.count = new AtomicLong(0L);
    this.sumOfTimestamps = new AtomicReference<>(BigInteger.ZERO);
  }

  @Override
  public void processElement(WindowedValue<T> inputElement, OpEmitter<T> emitter) {
    count.incrementAndGet();
    sumOfTimestamps.updateAndGet(sum -> sum.add(BigInteger.valueOf(System.nanoTime())));
    samzaTransformMetricRegistry
        .getTransformMetrics()
        .getTransformInputThroughput(transformFullName)
        .inc();
    emitter.emitElement(inputElement);
  }

  @Override
  public void processWatermark(Instant watermark, OpEmitter<T> emitter) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(
          "Processing Input Watermark for Transform: {} Count: {} SumOfTimestamps: {} for Watermark: {} for Task: {}",
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
      samzaTransformMetricRegistry.updateArrivalTimeMap(
          transformFullName, pValue, watermark.getMillis(), avg);
    }
    // reset all counters
    count.set(0L);
    this.sumOfTimestamps = new AtomicReference<>(BigInteger.ZERO);
    super.processWatermark(watermark, emitter);
  }
}
