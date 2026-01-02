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

import java.util.Map;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.utils.CountingPipelineVisitor;
import org.apache.beam.runners.flink.translation.utils.LookupPipelineVisitor;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Helper for {@link FlinkBatchPipelineTranslator} and translators in {@link
 * FlinkBatchTransformTranslators}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class FlinkBatchTranslationContext {
  private final PipelineOptions options;

  private AppliedPTransform<?, ?, ?> currentTransform;

  private final CountingPipelineVisitor countingPipelineVisitor = new CountingPipelineVisitor();
  private final LookupPipelineVisitor lookupPipelineVisitor = new LookupPipelineVisitor();

  // ------------------------------------------------------------------------

  FlinkBatchTranslationContext(PipelineOptions options) {
    this.options = options;
  }

  void init(Pipeline pipeline) {
    pipeline.traverseTopologically(countingPipelineVisitor);
    pipeline.traverseTopologically(lookupPipelineVisitor);
  }

  public PipelineOptions getPipelineOptions() {
    return options;
  }

  /**
   * Sets the AppliedPTransform which carries input/output.
   *
   * @param currentTransform Current transformation.
   */
  void setCurrentTransform(AppliedPTransform<?, ?, ?> currentTransform) {
    this.currentTransform = currentTransform;
  }

  AppliedPTransform<?, ?, ?> getCurrentTransform() {
    return currentTransform;
  }

  Map<TupleTag<?>, Coder<?>> getOutputCoders(PTransform<?, ?> transform) {
    return lookupPipelineVisitor.getOutputCoders(transform);
  }

  <T> TypeInformation<WindowedValue<T>> getTypeInfo(PCollection<T> collection) {
    return getTypeInfo(collection.getCoder(), collection.getWindowingStrategy());
  }

  <T> TypeInformation<WindowedValue<T>> getTypeInfo(
      Coder<T> coder, WindowingStrategy<?, ?> windowingStrategy) {
    WindowedValues.FullWindowedValueCoder<T> windowedValueCoder =
        WindowedValues.getFullCoder(coder, windowingStrategy.getWindowFn().windowCoder());

    return new CoderTypeInformation<>(windowedValueCoder, options);
  }

  Map<TupleTag<?>, PCollection<?>> getInputs(PTransform<?, ?> transform) {
    return lookupPipelineVisitor.getInputs(transform);
  }

  <T extends PValue> T getInput(PTransform<T, ?> transform) {
    return lookupPipelineVisitor.getInput(transform);
  }

  Map<TupleTag<?>, PCollection<?>> getOutputs(PTransform<?, ?> transform) {
    return lookupPipelineVisitor.getOutputs(transform);
  }

  <T extends PValue> T getOutput(PTransform<?, T> transform) {
    return lookupPipelineVisitor.getOutput(transform);
  }

  /** {@link CountingPipelineVisitor#getNumConsumers(PValue)}. */
  int getNumConsumers(PValue value) {
    return countingPipelineVisitor.getNumConsumers(value);
  }
}
