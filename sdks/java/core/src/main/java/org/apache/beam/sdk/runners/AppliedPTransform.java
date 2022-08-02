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
package org.apache.beam.sdk.runners;

import com.google.auto.value.AutoValue;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValues;
import org.apache.beam.sdk.values.TupleTag;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>Inputs and outputs are stored in their expanded forms, as the condensed form of a composite
 * {@link PInput} or {@link POutput} is a language-specific concept, and {@link AppliedPTransform}
 * represents a possibly cross-language transform for which no appropriate composite type exists in
 * the Java SDK.
 *
 * @param <InputT> transform input type
 * @param <OutputT> transform output type
 * @param <TransformT> transform type
 */
@Internal
@AutoValue
public abstract class AppliedPTransform<
    InputT extends PInput,
    OutputT extends POutput,
    TransformT extends PTransform<? super InputT, OutputT>> {
  // To prevent extension outside of this package.
  AppliedPTransform() {}

  public static <
          InputT extends PInput,
          OutputT extends POutput,
          TransformT extends PTransform<? super InputT, OutputT>>
      AppliedPTransform<InputT, OutputT, TransformT> of(
          String fullName,
          Map<TupleTag<?>, PCollection<?>> input,
          Map<TupleTag<?>, PCollection<?>> output,
          TransformT transform,
          ResourceHints resourceHints,
          Pipeline p) {
    return new AutoValue_AppliedPTransform<>(fullName, input, output, transform, resourceHints, p);
  }

  public abstract String getFullName();

  public abstract Map<TupleTag<?>, PCollection<?>> getInputs();

  public abstract Map<TupleTag<?>, PCollection<?>> getOutputs();

  public abstract TransformT getTransform();

  public abstract ResourceHints getResourceHints();

  public abstract Pipeline getPipeline();

  /** @return map of {@link TupleTag TupleTags} which are not side inputs. */
  public Map<TupleTag<?>, PCollection<?>> getMainInputs() {
    Map<TupleTag<?>, PCollection<?>> sideInputs =
        PValues.fullyExpand(getTransform().getAdditionalInputs());
    return getInputs().entrySet().stream()
        .filter(e -> !sideInputs.containsKey(e.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}
