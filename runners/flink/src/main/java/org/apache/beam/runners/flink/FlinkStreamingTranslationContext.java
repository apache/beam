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
package org.apache.beam.runners.flink;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Helper for keeping track of which {@link DataStream DataStreams} map to which {@link PTransform
 * PTransforms}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class FlinkStreamingTranslationContext {

  private final StreamExecutionEnvironment env;
  private final PipelineOptions options;
  private final boolean isStreaming;

  /**
   * Keeps a mapping between the output value of the PTransform and the Flink Operator that produced
   * it, after the translation of the correspondinf PTransform to its Flink equivalent.
   */
  private final Map<PValue, DataStream<?>> dataStreams = new HashMap<>();

  private final Map<PValue, PTransform<?, ?>> producers = new HashMap<>();

  private AppliedPTransform<?, ?, ?> currentTransform;

  public FlinkStreamingTranslationContext(
      StreamExecutionEnvironment env, PipelineOptions options, boolean isStreaming) {
    this.env = checkNotNull(env);
    this.options = checkNotNull(options);
    this.isStreaming = isStreaming;
  }

  public StreamExecutionEnvironment getExecutionEnvironment() {
    return env;
  }

  public PipelineOptions getPipelineOptions() {
    return options;
  }

  public boolean isStreaming() {
    return isStreaming;
  }

  @SuppressWarnings("unchecked")
  public <T> DataStream<T> getInputDataStream(PValue value) {
    return (DataStream<T>) dataStreams.get(value);
  }

  public void setOutputDataStream(PValue value, DataStream<?> set) {
    final PTransform<?, ?> previousProducer = producers.put(value, currentTransform.getTransform());
    Preconditions.checkArgument(
        previousProducer == null, "PValue can only have a single producer.");
    if (!dataStreams.containsKey(value)) {
      dataStreams.put(value, set);
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  <T extends PValue> PTransform<?, T> getProducer(T value) {
    return (PTransform) producers.get(value);
  }

  /**
   * Sets the AppliedPTransform which carries input/output.
   *
   * @param currentTransform
   */
  public void setCurrentTransform(AppliedPTransform<?, ?, ?> currentTransform) {
    this.currentTransform = currentTransform;
  }

  public <T> Coder<WindowedValue<T>> getWindowedInputCoder(PCollection<T> collection) {
    final Coder<T> valueCoder = collection.getCoder();
    return WindowedValue.getFullCoder(
        valueCoder, collection.getWindowingStrategy().getWindowFn().windowCoder());
  }

  public <T> Coder<T> getInputCoder(PCollection<T> collection) {
    return collection.getCoder();
  }

  public Map<TupleTag<?>, Coder<?>> getOutputCoders() {
    return currentTransform.getOutputs().entrySet().stream()
        .filter(e -> e.getValue() instanceof PCollection)
        .collect(Collectors.toMap(e -> e.getKey(), e -> ((PCollection) e.getValue()).getCoder()));
  }

  @SuppressWarnings("unchecked")
  public <T> TypeInformation<WindowedValue<T>> getTypeInfo(PCollection<T> collection) {
    return new CoderTypeInformation<>(getWindowedInputCoder(collection), options);
  }

  public AppliedPTransform<?, ?, ?> getCurrentTransform() {
    return currentTransform;
  }

  @SuppressWarnings("unchecked")
  public <T extends PValue> T getInput(PTransform<T, ?> transform) {
    return (T) Iterables.getOnlyElement(TransformInputs.nonAdditionalInputs(currentTransform));
  }

  public <T extends PInput> Map<TupleTag<?>, PCollection<?>> getInputs(PTransform<T, ?> transform) {
    return currentTransform.getInputs();
  }

  @SuppressWarnings("unchecked")
  public <T extends PValue> T getOutput(PTransform<?, T> transform) {
    return (T) Iterables.getOnlyElement(currentTransform.getOutputs().values());
  }

  public <OutputT extends POutput> Map<TupleTag<?>, PCollection<?>> getOutputs(
      PTransform<?, OutputT> transform) {
    return currentTransform.getOutputs();
  }
}
