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
package org.apache.beam.runners.core.construction.graph;

import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.resume;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.stop;
import static org.junit.Assert.assertEquals;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SplittableParDoExpanderTest {

  @UnboundedPerElement
  static class PairStringWithIndexToLengthBase extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public ProcessContinuation process(
        ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
      for (long i = tracker.currentRestriction().getFrom(), numIterations = 0;
          tracker.tryClaim(i);
          ++i, ++numIterations) {
        c.output(KV.of(c.element(), (int) i));
        if (numIterations % 3 == 0) {
          return resume();
        }
      }
      return stop();
    }

    @GetInitialRestriction
    public OffsetRange getInitialRange(@Element String element) {
      return new OffsetRange(0, element.length());
    }

    @SplitRestriction
    public void splitRange(
        @Element String element,
        @Restriction OffsetRange range,
        OutputReceiver<OffsetRange> receiver) {
      receiver.output(new OffsetRange(range.getFrom(), (range.getFrom() + range.getTo()) / 2));
      receiver.output(new OffsetRange((range.getFrom() + range.getTo()) / 2, range.getTo()));
    }
  }

  @Test
  public void testSizedReplacement() {
    Pipeline p = Pipeline.create();
    p.apply(Create.of("1", "2", "3"))
        .apply("TestSDF", ParDo.of(new PairStringWithIndexToLengthBase()));

    RunnerApi.Pipeline proto = PipelineTranslation.toProto(p);
    String transformName =
        Iterables.getOnlyElement(
            Maps.filterValues(
                    proto.getComponents().getTransformsMap(),
                    (RunnerApi.PTransform transform) ->
                        transform
                            .getUniqueName()
                            .contains(PairStringWithIndexToLengthBase.class.getSimpleName()))
                .keySet());

    RunnerApi.Pipeline updatedProto =
        ProtoOverrides.updateTransform(
            PTransformTranslation.PAR_DO_TRANSFORM_URN,
            proto,
            SplittableParDoExpander.createSizedReplacement());
    RunnerApi.PTransform newComposite =
        updatedProto.getComponents().getTransformsOrThrow(transformName);
    assertEquals(FunctionSpec.getDefaultInstance(), newComposite.getSpec());
    assertEquals(3, newComposite.getSubtransformsCount());
    assertEquals(
        PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN,
        updatedProto
            .getComponents()
            .getTransformsOrThrow(newComposite.getSubtransforms(0))
            .getSpec()
            .getUrn());
    assertEquals(
        PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN,
        updatedProto
            .getComponents()
            .getTransformsOrThrow(newComposite.getSubtransforms(1))
            .getSpec()
            .getUrn());
    assertEquals(
        PTransformTranslation.SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN,
        updatedProto
            .getComponents()
            .getTransformsOrThrow(newComposite.getSubtransforms(2))
            .getSpec()
            .getUrn());
  }

  @Test
  public void testTruncateReplacement() {
    Pipeline p = Pipeline.create();
    p.apply(Create.of("1", "2", "3"))
        .apply("TestSDF", ParDo.of(new PairStringWithIndexToLengthBase()));

    RunnerApi.Pipeline proto = PipelineTranslation.toProto(p);
    String transformName =
        Iterables.getOnlyElement(
            Maps.filterValues(
                    proto.getComponents().getTransformsMap(),
                    (RunnerApi.PTransform transform) ->
                        transform
                            .getUniqueName()
                            .contains(PairStringWithIndexToLengthBase.class.getSimpleName()))
                .keySet());

    RunnerApi.Pipeline updatedProto =
        ProtoOverrides.updateTransform(
            PTransformTranslation.PAR_DO_TRANSFORM_URN,
            proto,
            SplittableParDoExpander.createTruncateReplacement());
    RunnerApi.PTransform newComposite =
        updatedProto.getComponents().getTransformsOrThrow(transformName);
    assertEquals(FunctionSpec.getDefaultInstance(), newComposite.getSpec());
    assertEquals(4, newComposite.getSubtransformsCount());
    assertEquals(
        PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN,
        updatedProto
            .getComponents()
            .getTransformsOrThrow(newComposite.getSubtransforms(0))
            .getSpec()
            .getUrn());
    assertEquals(
        PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN,
        updatedProto
            .getComponents()
            .getTransformsOrThrow(newComposite.getSubtransforms(1))
            .getSpec()
            .getUrn());
    assertEquals(
        PTransformTranslation.SPLITTABLE_TRUNCATE_SIZED_RESTRICTION_URN,
        updatedProto
            .getComponents()
            .getTransformsOrThrow(newComposite.getSubtransforms(2))
            .getSpec()
            .getUrn());
    assertEquals(
        PTransformTranslation.SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN,
        updatedProto
            .getComponents()
            .getTransformsOrThrow(newComposite.getSubtransforms(3))
            .getSpec()
            .getUrn());
  }
}
