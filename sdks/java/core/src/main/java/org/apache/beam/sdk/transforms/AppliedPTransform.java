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
package org.apache.beam.sdk.transforms;

import com.google.auto.value.AutoValue;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Represents the application of a {@link PTransform} to a specific input to produce
 * a specific output.
 *
 * <p>For internal use.
 *
 * <p>Inputs and outputs are stored in their expanded forms, as the condensed form of a composite
 * {@link PInput} or {@link POutput} is a language-specific concept, and {@link AppliedPTransform}
 * represents a possibly cross-language transform for which no appropriate composite type exists
 * in the Java SDK.
 *
 * @param <InputT>     transform input type
 * @param <OutputT>    transform output type
 * @param <TransformT> transform type
 */
@AutoValue
public abstract class AppliedPTransform<
    InputT extends PInput, OutputT extends POutput,
    TransformT extends PTransform<? super InputT, OutputT>> {
  // To prevent extension outside of this package.
  AppliedPTransform() {}

  public static <
          InputT extends PInput,
          OutputT extends POutput,
          TransformT extends PTransform<? super InputT, OutputT>>
      AppliedPTransform<InputT, OutputT, TransformT> of(
          String fullName,
          Map<TupleTag<?>, PValue> input,
          Map<TupleTag<?>, PValue> output,
          TransformT transform,
          Pipeline p) {
    return new AutoValue_AppliedPTransform<InputT, OutputT, TransformT>(
        fullName, input, output, transform, p);
  }

  public abstract String getFullName();

  public abstract Map<TupleTag<?>, PValue> getInputs();

  public abstract Map<TupleTag<?>, PValue> getOutputs();

  public abstract TransformT getTransform();

  public abstract Pipeline getPipeline();
}
