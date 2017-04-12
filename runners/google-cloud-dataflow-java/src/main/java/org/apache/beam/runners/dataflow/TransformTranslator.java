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

import com.google.api.services.dataflow.model.Step;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.OutputReference;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A {@link TransformTranslator} knows how to translate a particular subclass of {@link PTransform}
 * for the Cloud Dataflow service. It does so by mutating the {@link TranslationContext}.
 */
interface TransformTranslator<TransformT extends PTransform> {
  void translate(TransformT transform, TranslationContext context);

  /**
   * The interface provided to registered callbacks for interacting with the {@link DataflowRunner},
   * including reading and writing the values of {@link PCollection}s and side inputs.
   */
  interface TranslationContext {
    /** Returns the configured pipeline options. */
    DataflowPipelineOptions getPipelineOptions();

    /** Returns the input of the currently being translated transform. */
    <InputT extends PInput> Map<TupleTag<?>, PValue> getInputs(PTransform<InputT, ?> transform);

    <InputT extends PValue> InputT getInput(PTransform<InputT, ?> transform);

    /** Returns the output of the currently being translated transform. */
    <OutputT extends POutput> Map<TupleTag<?>, PValue> getOutputs(PTransform<?, OutputT> transform);

    <OutputT extends PValue> OutputT getOutput(PTransform<?, OutputT> transform);

    /** Returns the full name of the currently being translated transform. */
    String getFullName(PTransform<?, ?> transform);

    /**
     * Adds a step to the Dataflow workflow for the given transform, with the given Dataflow step
     * type.
     */
    StepTranslationContext addStep(PTransform<?, ?> transform, String type);

    /**
     * Adds a pre-defined step to the Dataflow workflow. The given PTransform should be consistent
     * with the Step, in terms of input, output and coder types.
     *
     * <p>This is a low-level operation, when using this method it is up to the caller to ensure
     * that names do not collide.
     */
    Step addStep(PTransform<?, ? extends PValue> transform, Step step);
    /** Encode a PValue reference as an output reference. */
    OutputReference asOutputReference(PValue value, AppliedPTransform<?, ?, ?> producer);

    /**
     * Get the {@link AppliedPTransform} that produced the provided {@link PValue}.
     */
    AppliedPTransform<?, ?, ?> getProducer(PValue value);
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
     * Pipeline}. If the input value has not yet been produced yet (either by a call to {@link
     * StepTranslationContext#addOutput(PValue)} or within a call to {@link
     * TranslationContext#addStep(PTransform, Step)}), this method will throw an exception.
     */
    void addInput(String name, PInput value);

    /** Adds an input that is a dictionary of strings to objects. */
    void addInput(String name, Map<String, Object> elements);

    /** Adds an input that is a list of objects. */
    void addInput(String name, List<? extends Map<String, Object>> elements);

    /**
     * Adds an output to this Dataflow step, producing the specified output {@code PValue},
     * including its {@code Coder} if a {@code TypedPValue}. If the {@code PValue} is a {@code
     * PCollection}, wraps its coder inside a {@code WindowedValueCoder}. Returns a pipeline level
     * unique id.
     */
    long addOutput(PValue value);

    /**
     * Adds an output to this {@code CollectionToSingleton} Dataflow step, consuming the specified
     * input {@code PValue} and producing the specified output {@code PValue}. This step requires
     * special treatment for its output encoding. Returns a pipeline level unique id.
     */
    long addCollectionToSingletonOutput(PValue inputValue, PValue outputValue);
  }
}
