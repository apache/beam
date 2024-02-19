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

import org.apache.beam.runners.samza.runtime.Op;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Factory class to create {@link Op} for default transform metric computation.
 *
 * <p>Each metric Op computes and emits default throughput, latency & watermark progress metric per
 * transform for Beam Samza Runner. A metric Op can be either attached to Input PCollection or
 * Output PCollection of a PTransform.
 *
 * <p>Each concrete metric OP is responsible for following metrics computation: 1. Throughput: Emit
 * the number of elements processed in the PCollection 2. Watermark Progress: Emit the output
 * watermark progress of the PCollection 3. Latency: Maintain the avg arrival time per watermark
 * across elements it processes, compute & emit the latency
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness"
}) // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
public class SamzaMetricOpFactory {
  public enum OpType {
    INPUT,
    OUTPUT
  }

  /**
   * Create a {@link Op} for default transform metric computation.
   *
   * @param urn URN of the PCollection metric Op is processing
   * @param pValue name of the PCollection metric Op is processing
   * @param transformName name of the PTransform for which metric Op is created
   * @param opType type of the metric
   * @param samzaTransformMetricRegistry metric registry
   * @param <T> type of the message
   * @return a {@link Op} for default transform metric computation
   */
  public static @NonNull <T> Op<T, T, Void> createMetricOp(
      @NonNull String urn,
      @NonNull String pValue,
      @NonNull String transformName,
      @NonNull OpType opType,
      @NonNull SamzaTransformMetricRegistry samzaTransformMetricRegistry) {
    if (isDataShuffleTransform(urn)) {
      return new SamzaGBKMetricOp<>(pValue, transformName, opType, samzaTransformMetricRegistry);
    }
    return new SamzaMetricOp<>(pValue, transformName, opType, samzaTransformMetricRegistry);
  }

  private static boolean isDataShuffleTransform(String urn) {
    return urn.equals(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN)
        || urn.equals(PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN);
  }
}
