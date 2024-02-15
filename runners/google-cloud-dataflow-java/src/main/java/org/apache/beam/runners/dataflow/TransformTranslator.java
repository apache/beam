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
package org.apache.beam.runners.dataflow;

import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.OutputReference;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.SdkComponents;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A {@link TransformTranslator} knows how to translate a particular subclass of {@link PTransform}
 * for the Cloud Dataflow service. It does so by mutating the {@link TranslationContext}.
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public interface TransformTranslator<TransformT extends PTransform> {
  void translate(TransformT transform, TranslationContext context);

  /**
   * The interface provided to registered callbacks for interacting with the {@link DataflowRunner},
   * including reading and writing the values of {@link PCollection}s and side inputs.
   */
  interface TranslationContext {
    default boolean isStreamingEngine() {
      List<String> experiments = getPipelineOptions().getExperiments();
      return experiments != null
          && experiments.contains(GcpOptions.STREAMING_ENGINE_EXPERIMENT)
          && experiments.contains(GcpOptions.WINDMILL_SERVICE_EXPERIMENT);
    }

    /** Returns the configured pipeline options. */
    DataflowPipelineOptions getPipelineOptions();

    /** Returns the input of the currently being translated transform. */
    <InputT extends PInput> Map<TupleTag<?>, PCollection<?>> getInputs(
        PTransform<InputT, ?> transform);

    <InputT extends PValue> InputT getInput(PTransform<InputT, ?> transform);

    /** Returns the output of the currently being translated transform. */
    <OutputT extends POutput> Map<TupleTag<?>, PCollection<?>> getOutputs(
        PTransform<?, OutputT> transform);

    <OutputT extends PValue> OutputT getOutput(PTransform<?, OutputT> transform);

    /** Returns the full name of the currently being translated transform. */
    String getFullName(PTransform<?, ?> transform);

    /**
     * Adds a step to the Dataflow workflow for the given transform, with the given Dataflow step
     * type.
     */
    StepTranslationContext addStep(PTransform<?, ?> transform, String type);

    /** Encode a PValue reference as an output reference. */
    OutputReference asOutputReference(PValue value, AppliedPTransform<?, ?, ?> producer);

    SdkComponents getSdkComponents();

    AppliedPTransform<?, ?, ?> getCurrentTransform();

    /** Get the {@link AppliedPTransform} that produced the provided {@link PValue}. */
    AppliedPTransform<?, ?, ?> getProducer(PValue value);

    /**
     * Gets the parent composite transform to the current transform, if one exists. Otherwise
     * returns one null.
     */
    AppliedPTransform<?, ?, ?> getCurrentParent();
  }

  /** The interface for a {@link TransformTranslator} to build a Dataflow step. */
  interface StepTranslationContext {
    /** Sets the encoding for this Dataflow step. */
    void addEncodingInput(Coder<?> value);

    /** Adds an input with the given name and value to this Dataflow step. */
    void addInput(String name, Boolean value);

    /** Adds an input with the given name and value to this Dataflow step. */
    void addInput(String name, String value);

    /** Adds an input with the given name and value to this Dataflow step. */
    void addInput(String name, Long value);

    /**
     * Adds an input with the given name to this Dataflow step, coming from the specified input
     * PValue.
     *
     * <p>The input {@link PValue} must have already been produced by a step earlier in this {@link
     * Pipeline}. If the input value has not yet been produced yet (by a call to either {@link
     * StepTranslationContext#addOutput} or {@link
     * StepTranslationContext#addCollectionToSingletonOutput}) this method will throw an exception.
     */
    void addInput(String name, PInput value);

    /** Adds an input that is a dictionary of strings to objects. */
    void addInput(String name, Map<String, Object> elements);

    /** Adds an input that is a list of objects. */
    void addInput(String name, List<? extends Map<String, Object>> elements);

    /**
     * Adds a primitive output to this Dataflow step with the given name as the local output name,
     * producing the specified output {@code PValue}, including its {@code Coder} if a {@code
     * TypedPValue}. If the {@code PValue} is a {@code PCollection}, wraps its coder inside a {@code
     * WindowedValueCoder}.
     */
    void addOutput(String name, PCollection<?> value);

    /**
     * Adds an output to this {@code CollectionToSingleton} Dataflow step, consuming the specified
     * input {@code PValue} and producing the specified output {@code PValue}. This step requires
     * special treatment for its output encoding. Returns a pipeline level unique id.
     */
    void addCollectionToSingletonOutput(
        PCollection<?> inputValue, String outputName, PCollectionView<?> outputValue);
  }
}
