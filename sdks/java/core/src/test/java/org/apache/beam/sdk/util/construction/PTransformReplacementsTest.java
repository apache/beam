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
package org.apache.beam.sdk.util.construction;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Collections;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValues;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PTransformReplacements}. */
@RunWith(JUnit4.class)
public class PTransformReplacementsTest {
  @Rule public TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
  @Rule public ExpectedException thrown = ExpectedException.none();
  private PCollection<Long> mainInput = pipeline.apply(GenerateSequence.from(0));
  private PCollectionView<String> sideInput =
      pipeline.apply(Create.of("foo")).apply(View.asSingleton());

  private PCollection<Long> output = mainInput.apply(ParDo.of(new TestDoFn()));

  @Test
  public void getMainInputSingleOutputSingleInput() {
    AppliedPTransform<PCollection<Long>, ?, ?> application =
        AppliedPTransform.of(
            "application",
            Collections.singletonMap(new TupleTag<Long>(), mainInput),
            Collections.singletonMap(new TupleTag<Long>(), output),
            ParDo.of(new TestDoFn()),
            ResourceHints.create(),
            pipeline);
    PCollection<Long> input = PTransformReplacements.getSingletonMainInput(application);
    assertThat(input, equalTo(mainInput));
  }

  @Test
  public void getMainInputSingleOutputSideInputs() {
    AppliedPTransform<PCollection<Long>, ?, ?> application =
        AppliedPTransform.of(
            "application",
            ImmutableMap.<TupleTag<?>, PCollection<?>>builder()
                .put(new TupleTag<Long>(), mainInput)
                .put(sideInput.getTagInternal(), sideInput.getPCollection())
                .build(),
            Collections.singletonMap(new TupleTag<Long>(), output),
            ParDo.of(new TestDoFn()).withSideInputs(sideInput),
            ResourceHints.create(),
            pipeline);
    PCollection<Long> input = PTransformReplacements.getSingletonMainInput(application);
    assertThat(input, equalTo(mainInput));
  }

  @Test
  public void getMainInputExtraMainInputsThrows() {
    PCollection<Long> notInParDo = pipeline.apply("otherPCollection", Create.of(1L, 2L, 3L));
    ImmutableMap<TupleTag<?>, PCollection<?>> inputs =
        ImmutableMap.<TupleTag<?>, PCollection<?>>builder()
            .putAll(PValues.expandInput(mainInput))
            // Not represnted as an input
            .put(new TupleTag<Long>(), notInParDo)
            .put(sideInput.getTagInternal(), sideInput.getPCollection())
            .build();
    AppliedPTransform<PCollection<Long>, ?, ?> application =
        AppliedPTransform.of(
            "application",
            inputs,
            Collections.singletonMap(new TupleTag<Long>(), output),
            ParDo.of(new TestDoFn()).withSideInputs(sideInput),
            ResourceHints.create(),
            pipeline);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("multiple inputs");
    thrown.expectMessage("not additional inputs");
    thrown.expectMessage(mainInput.toString());
    thrown.expectMessage(notInParDo.toString());
    PTransformReplacements.getSingletonMainInput(application);
  }

  @Test
  public void getMainInputNoMainInputsThrows() {
    ImmutableMap<TupleTag<?>, PCollection<?>> inputs =
        ImmutableMap.<TupleTag<?>, PCollection<?>>builder()
            .put(sideInput.getTagInternal(), sideInput.getPCollection())
            .build();
    AppliedPTransform<PCollection<Long>, ?, ?> application =
        AppliedPTransform.of(
            "application",
            inputs,
            Collections.singletonMap(new TupleTag<Long>(), output),
            ParDo.of(new TestDoFn()).withSideInputs(sideInput),
            ResourceHints.create(),
            pipeline);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("No main input");
    PTransformReplacements.getSingletonMainInput(application);
  }

  private static class TestDoFn extends DoFn<Long, Long> {
    @ProcessElement
    public void process(ProcessContext context) {}
  }
}
