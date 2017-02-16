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
package org.apache.beam.runners.flink.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Iterables;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Helper for keeping track of which {@link DataStream DataStreams} map
 * to which {@link PTransform PTransforms}.
 */
public class FlinkStreamingTranslationContext {

  private final StreamExecutionEnvironment env;
  private final PipelineOptions options;

  /**
   * Keeps a mapping between the output value of the PTransform (in Dataflow) and the
   * Flink Operator that produced it, after the translation of the correspondinf PTransform
   * to its Flink equivalent.
   * */
  private final Map<PValue, DataStream<?>> dataStreams;

  private AppliedPTransform<?, ?, ?> currentTransform;

  public FlinkStreamingTranslationContext(StreamExecutionEnvironment env, PipelineOptions options) {
    this.env = checkNotNull(env);
    this.options = checkNotNull(options);
    this.dataStreams = new HashMap<>();
  }

  public StreamExecutionEnvironment getExecutionEnvironment() {
    return env;
  }

  public PipelineOptions getPipelineOptions() {
    return options;
  }

  @SuppressWarnings("unchecked")
  public <T> DataStream<T> getInputDataStream(PValue value) {
    return (DataStream<T>) dataStreams.get(value);
  }

  public void setOutputDataStream(PValue value, DataStream<?> set) {
    if (!dataStreams.containsKey(value)) {
      dataStreams.put(value, set);
    }
  }

  /**
   * Sets the AppliedPTransform which carries input/output.
   * @param currentTransform
   */
  public void setCurrentTransform(AppliedPTransform<?, ?, ?> currentTransform) {
    this.currentTransform = currentTransform;
  }

  public <T> Coder<WindowedValue<T>> getCoder(PCollection<T> collection) {
    Coder<T> valueCoder = collection.getCoder();

    return WindowedValue.getFullCoder(
        valueCoder,
        collection.getWindowingStrategy().getWindowFn().windowCoder());
  }

  @SuppressWarnings("unchecked")
  public <T> TypeInformation<WindowedValue<T>> getTypeInfo(PCollection<T> collection) {
    Coder<T> valueCoder = collection.getCoder();
    WindowedValue.FullWindowedValueCoder<T> windowedValueCoder =
        WindowedValue.getFullCoder(
            valueCoder,
            collection.getWindowingStrategy().getWindowFn().windowCoder());

    return new CoderTypeInformation<>(windowedValueCoder);
  }


  @SuppressWarnings("unchecked")
  public <T extends PValue> T getInput(PTransform<T, ?> transform) {
    return (T) Iterables.getOnlyElement(currentTransform.getInputs()).getValue();
  }

  public <T extends PInput> List<TaggedPValue> getInputs(PTransform<T, ?> transform) {
    return currentTransform.getInputs();
  }

  @SuppressWarnings("unchecked")
  public <T extends PValue> T getOutput(PTransform<?, T> transform) {
    return (T) Iterables.getOnlyElement(currentTransform.getOutputs()).getValue();
  }

  public <OutputT extends POutput> List<TaggedPValue> getOutputs(PTransform<?, OutputT> transform) {
    return currentTransform.getOutputs();
  }

}
