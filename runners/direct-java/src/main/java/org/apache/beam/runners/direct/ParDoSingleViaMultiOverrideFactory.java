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
package org.apache.beam.runners.direct;

import org.apache.beam.runners.core.construction.SingleInputOutputOverrideFactory;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.Bound;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * A {@link PTransformOverrideFactory} that overrides single-output {@link ParDo} to implement
 * it in terms of multi-output {@link ParDo}.
 */
class ParDoSingleViaMultiOverrideFactory<InputT, OutputT>
    extends SingleInputOutputOverrideFactory<
            PCollection<? extends InputT>, PCollection<OutputT>, Bound<InputT, OutputT>> {
  @Override
  public PTransform<PCollection<? extends InputT>, PCollection<OutputT>> getReplacementTransform(
      Bound<InputT, OutputT> transform) {
    return new ParDoSingleViaMulti<>(transform);
  }

  static class ParDoSingleViaMulti<InputT, OutputT>
      extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>> {
    private static final String MAIN_OUTPUT_TAG = "main";

    private final ParDo.Bound<InputT, OutputT> underlyingParDo;

    public ParDoSingleViaMulti(ParDo.Bound<InputT, OutputT> underlyingParDo) {
      this.underlyingParDo = underlyingParDo;
    }

    @Override
    public PCollection<OutputT> expand(PCollection<? extends InputT> input) {

      // Output tags for ParDo need only be unique up to applied transform
      TupleTag<OutputT> mainOutputTag = new TupleTag<OutputT>(MAIN_OUTPUT_TAG);

      PCollectionTuple outputs =
          input.apply(
              ParDo.of(underlyingParDo.getFn())
                  .withSideInputs(underlyingParDo.getSideInputs())
                  .withOutputTags(mainOutputTag, TupleTagList.empty()));
      PCollection<OutputT> output = outputs.get(mainOutputTag);

      output.setTypeDescriptor(underlyingParDo.getFn().getOutputTypeDescriptor());
      return output;
    }
  }
}
