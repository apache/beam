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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.samza.runtime.KeyedTimerData;
import org.apache.beam.runners.samza.runtime.Op;
import org.apache.beam.runners.samza.runtime.OpEmitter;
import org.apache.beam.runners.samza.util.PipelineJsonRenderer;
import org.apache.samza.config.Config;
import org.apache.samza.context.Context;
import org.apache.samza.operators.Scheduler;

/**
 * MetricOp for default throughput, latency & watermark progress metric per transform for Beam Samza
 * Runner. A MetricOp can be either attached to Input PCollection or Output PCollection of a
 * PTransform.
 *
 * <p>A MetricOp is created per primitive PTransform per PCollection its across its inputs &
 * outputs. 1. An independent MetricOp is created and attached to each input PCollection to the
 * PTransform. 2. An independent MetricOp is created and attached to each input PCollection to the
 * PTransform.
 *
 * <p>Each concrete MetricOp is responsible for following metrics computation: 1. Throughput: Emit
 * the number of elements processed in the PCollection 2. Watermark Progress: Emit the watermark
 * progress of the PCollection 3. Latency: Maintain the avg arrival time per watermark across
 * elements it processes, compute & emit the latency
 *
 * @param <T> type of the message
 */
public abstract class SamzaMetricOp<T> implements Op<T, T, Void> {
  // Unique name of the PTransform this MetricOp is associated with
  protected final String transformFullName;
  protected final SamzaTransformMetricRegistry samzaTransformMetricRegistry;
  // Name or identifier of the PCollection which PTransform is processing
  protected final String pValue;
  // List of input PValue(s) for all PCollections processing the PTransform
  protected transient List<String> transformInputs;
  // List of output PValue(s) for all PCollections processing the PTransform
  protected transient List<String> transformOutputs;
  // Name of the task, for logging purpose
  protected transient String task;

  // Some fields are initialized in open() method, which is called after the constructor.
  @SuppressWarnings("initialization.fields.uninitialized")
  public SamzaMetricOp(
      String pValue,
      String transformFullName,
      SamzaTransformMetricRegistry samzaTransformMetricRegistry) {
    this.transformFullName = transformFullName;
    this.samzaTransformMetricRegistry = samzaTransformMetricRegistry;
    this.pValue = pValue;
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
}
