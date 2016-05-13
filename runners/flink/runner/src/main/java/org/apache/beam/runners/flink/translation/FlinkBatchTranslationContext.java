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

import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper for {@link FlinkBatchPipelineTranslator} and translators in
 * {@link FlinkBatchTransformTranslators}.
 */
public class FlinkBatchTranslationContext {
  
  private final Map<PValue, DataSet<?>> dataSets;
  private final Map<PCollectionView<?>, DataSet<?>> broadcastDataSets;

  /**
   * For keeping track about which DataSets don't have a successor. We
   * need to terminate these with a discarding sink because the Beam
   * model allows dangling operations.
   */
  private final Map<PValue, DataSet<?>> danglingDataSets;

  private final ExecutionEnvironment env;
  private final PipelineOptions options;

  private AppliedPTransform<?, ?, ?> currentTransform;
  
  // ------------------------------------------------------------------------
  
  public FlinkBatchTranslationContext(ExecutionEnvironment env, PipelineOptions options) {
    this.env = env;
    this.options = options;
    this.dataSets = new HashMap<>();
    this.broadcastDataSets = new HashMap<>();

    this.danglingDataSets = new HashMap<>();
  }
  
  // ------------------------------------------------------------------------

  public Map<PValue, DataSet<?>> getDanglingDataSets() {
    return danglingDataSets;
  }

  public ExecutionEnvironment getExecutionEnvironment() {
    return env;
  }

  public PipelineOptions getPipelineOptions() {
    return options;
  }
  
  @SuppressWarnings("unchecked")
  public <T> DataSet<WindowedValue<T>> getInputDataSet(PValue value) {
    // assume that the DataSet is used as an input if retrieved here
    danglingDataSets.remove(value);
    return (DataSet<WindowedValue<T>>) dataSets.get(value);
  }

  public <T> void setOutputDataSet(PValue value, DataSet<WindowedValue<T>> set) {
    if (!dataSets.containsKey(value)) {
      dataSets.put(value, set);
      danglingDataSets.put(value, set);
    }
  }

  /**
   * Sets the AppliedPTransform which carries input/output.
   * @param currentTransform
   */
  public void setCurrentTransform(AppliedPTransform<?, ?, ?> currentTransform) {
    this.currentTransform = currentTransform;
  }

  @SuppressWarnings("unchecked")
  public <T> DataSet<T> getSideInputDataSet(PCollectionView<?> value) {
    return (DataSet<T>) broadcastDataSets.get(value);
  }

  public <ViewT, ElemT> void setSideInputDataSet(
      PCollectionView<ViewT> value,
      DataSet<WindowedValue<ElemT>> set) {
    if (!broadcastDataSets.containsKey(value)) {
      broadcastDataSets.put(value, set);
    }
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
  <T extends PInput> T getInput(PTransform<T, ?> transform) {
    return (T) currentTransform.getInput();
  }

  @SuppressWarnings("unchecked")
  <T extends POutput> T getOutput(PTransform<?, T> transform) {
    return (T) currentTransform.getOutput();
  }
}
