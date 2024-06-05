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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.construction.ForwardingPTransform;
import org.apache.beam.sdk.util.construction.PTransformReplacements;
import org.apache.beam.sdk.util.construction.ReplacementOutputs;
import org.apache.beam.sdk.util.construction.SplittableParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/** Transform overrides for supporting {@link SplittableParDo} in the Dataflow runner. */
class SplittableParDoOverrides {
  static class ParDoSingleViaMulti<InputT, OutputT>
      extends ForwardingPTransform<PCollection<? extends InputT>, PCollection<OutputT>> {
    private final ParDo.SingleOutput<InputT, OutputT> original;

    public ParDoSingleViaMulti(
        DataflowRunner ignored, ParDo.SingleOutput<InputT, OutputT> original) {
      this.original = original;
    }

    @Override
    protected PTransform<PCollection<? extends InputT>, PCollection<OutputT>> delegate() {
      return original;
    }

    @Override
    public PCollection<OutputT> expand(PCollection<? extends InputT> input) {
      TupleTag<OutputT> mainOutput = new TupleTag<>();
      return input.apply(original.withOutputTags(mainOutput, TupleTagList.empty())).get(mainOutput);
    }
  }

  static class SplittableParDoOverrideFactory<InputT, OutputT, RestrictionT>
      implements PTransformOverrideFactory<
          PCollection<InputT>, PCollectionTuple, ParDo.MultiOutput<InputT, OutputT>> {
    @Override
    public PTransformReplacement<PCollection<InputT>, PCollectionTuple> getReplacementTransform(
        AppliedPTransform<PCollection<InputT>, PCollectionTuple, ParDo.MultiOutput<InputT, OutputT>>
            appliedTransform) {
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(appliedTransform),
          SplittableParDo.forAppliedParDo(appliedTransform));
    }

    @Override
    public Map<PCollection<?>, ReplacementOutput> mapOutputs(
        Map<TupleTag<?>, PCollection<?>> outputs, PCollectionTuple newOutput) {
      return ReplacementOutputs.tagged(outputs, newOutput);
    }
  }
}
