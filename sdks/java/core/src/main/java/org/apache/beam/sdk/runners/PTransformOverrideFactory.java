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
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * <b><i>For internal use only; no backwards-compatibility guarantees.</i></b>
 *
 * <p>Produces {@link PipelineRunner}-specific overrides of {@link PTransform PTransforms}, and
 * provides mappings between original and replacement outputs.
 */
@Internal
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public interface PTransformOverrideFactory<
    InputT extends PInput,
    OutputT extends POutput,
    TransformT extends PTransform<? super InputT, OutputT>> {
  /**
   * Returns a {@link PTransform} that produces equivalent output to the provided {@link
   * AppliedPTransform transform}.
   */
  PTransformReplacement<InputT, OutputT> getReplacementTransform(
      AppliedPTransform<InputT, OutputT, TransformT> transform);

  /**
   * Returns a {@link Map} from the expanded values in {@code newOutput} to the values produced by
   * the original transform.
   */
  Map<PCollection<?>, ReplacementOutput> mapOutputs(
      Map<TupleTag<?>, PCollection<?>> outputs, OutputT newOutput);

  /**
   * A {@link PTransform} that replaces an {@link AppliedPTransform}, and the input required to do
   * so. The input must be constructed from the expanded form, as the transform may not have
   * originally been applied within this process or from within a Java SDK.
   */
  @AutoValue
  abstract class PTransformReplacement<InputT extends PInput, OutputT extends POutput> {
    public static <InputT extends PInput, OutputT extends POutput>
        PTransformReplacement<InputT, OutputT> of(
            InputT input, PTransform<InputT, OutputT> transform) {
      return new AutoValue_PTransformOverrideFactory_PTransformReplacement(input, transform);
    }

    public abstract InputT getInput();

    public abstract PTransform<InputT, OutputT> getTransform();
  }

  /** A mapping between original {@link TaggedPValue} outputs and their replacements. */
  @AutoValue
  abstract class ReplacementOutput {
    public static ReplacementOutput of(TaggedPValue original, TaggedPValue replacement) {
      return new AutoValue_PTransformOverrideFactory_ReplacementOutput(original, replacement);
    }

    public abstract TaggedPValue getOriginal();

    public abstract TaggedPValue getReplacement();
  }
}
