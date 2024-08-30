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

import java.util.Map;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.DoFn.RequiresStableInput;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

/** Transform overrides for supporting {@link RequiresStableInput} in the Dataflow runner. */
class RequiresStableInputParDoOverrides {

  /**
   * Returns a {@link PTransformOverrideFactory} that inserts a {@link Reshuffle.ViaRandomKey}
   * before a {@link ParDo.SingleOutput} that uses the {@link RequiresStableInput} annotation.
   */
  static <InputT, OutputT>
      PTransformOverrideFactory<
              PCollection<InputT>, PCollection<OutputT>, ParDo.SingleOutput<InputT, OutputT>>
          singleOutputOverrideFactory() {
    return new SingleOutputOverrideFactory<>();
  }

  /**
   * Returns a {@link PTransformOverrideFactory} that inserts a {@link Reshuffle.ViaRandomKey}
   * before a {@link ParDo.MultiOutput} that uses the {@link RequiresStableInput} annotation.
   */
  static <InputT, OutputT>
      PTransformOverrideFactory<
              PCollection<InputT>, PCollectionTuple, ParDo.MultiOutput<InputT, OutputT>>
          multiOutputOverrideFactory() {
    return new MultiOutputOverrideFactory<>();
  }

  private static class SingleOutputOverrideFactory<InputT, OutputT>
      implements PTransformOverrideFactory<
          PCollection<InputT>, PCollection<OutputT>, ParDo.SingleOutput<InputT, OutputT>> {

    @Override
    public PTransformReplacement<PCollection<InputT>, PCollection<OutputT>> getReplacementTransform(
        AppliedPTransform<
                PCollection<InputT>, PCollection<OutputT>, ParDo.SingleOutput<InputT, OutputT>>
            appliedTransform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(appliedTransform),
          new PTransform<PCollection<InputT>, PCollection<OutputT>>() {
            @Override
            public PCollection<OutputT> expand(PCollection<InputT> input) {
              return input
                  .apply("Materialize input", Reshuffle.viaRandomKey())
                  .apply("ParDo with stable input", appliedTransform.getTransform());
            }
          });
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollection<OutputT> newOutput) {
      return ReplacementOutputs.singleton(outputs, newOutput);
    }
  }

  private static class MultiOutputOverrideFactory<InputT, OutputT>
      implements PTransformOverrideFactory<
          PCollection<InputT>, PCollectionTuple, ParDo.MultiOutput<InputT, OutputT>> {

    @Override
    public PTransformReplacement<PCollection<InputT>, PCollectionTuple> getReplacementTransform(
        AppliedPTransform<PCollection<InputT>, PCollectionTuple, ParDo.MultiOutput<InputT, OutputT>>
            appliedTransform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(appliedTransform),
          new PTransform<PCollection<InputT>, PCollectionTuple>() {
            @Override
            public PCollectionTuple expand(PCollection<InputT> input) {
              return input
                  .apply("Materialize input", Reshuffle.viaRandomKey())
                  .apply("ParDo with stable input", appliedTransform.getTransform());
            }
          });
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollectionTuple newOutput) {
      return ReplacementOutputs.tagged(outputs, newOutput);
    }
  }
}
