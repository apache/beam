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
package org.apache.beam.runners.core.construction;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link PTransformTranslation}. */
@RunWith(Parameterized.class)
public class PTransformTranslationTest {

  @Parameters(name = "{index}: {0}")
  public static Iterable<ToAndFromProtoSpec> data() {
    // This pipeline exists for construction, not to run any test.
    // TODO: Leaf node with understood payload - i.e. validate payloads
    ToAndFromProtoSpec readLeaf = ToAndFromProtoSpec.leaf(read(TestPipeline.create()));

    ToAndFromProtoSpec readMultipleInAndOut =
        ToAndFromProtoSpec.leaf(multiMultiParDo(TestPipeline.create()));

    TestPipeline compositeReadPipeline = TestPipeline.create();
    ToAndFromProtoSpec compositeRead =
        ToAndFromProtoSpec.composite(
            generateSequence(compositeReadPipeline),
            ToAndFromProtoSpec.leaf(read(compositeReadPipeline)));

    ToAndFromProtoSpec rawLeafNullSpec =
        ToAndFromProtoSpec.leaf(rawPTransformWithNullSpec(TestPipeline.create()));

    return ImmutableList.<ToAndFromProtoSpec>builder()
        .add(readLeaf)
        .add(readMultipleInAndOut)
        .add(compositeRead)
        .add(rawLeafNullSpec)
        // TODO: Composite with multiple children
        // TODO: Composite with a composite child
        .build();
  }

  @AutoValue
  abstract static class ToAndFromProtoSpec {
    public static ToAndFromProtoSpec leaf(AppliedPTransform<?, ?, ?> transform) {
      return new AutoValue_PTransformTranslationTest_ToAndFromProtoSpec(
          transform, Collections.emptyList());
    }

    public static ToAndFromProtoSpec composite(
        AppliedPTransform<?, ?, ?> topLevel, ToAndFromProtoSpec spec, ToAndFromProtoSpec... specs) {
      List<ToAndFromProtoSpec> childSpecs = new ArrayList<>();
      childSpecs.add(spec);
      childSpecs.addAll(Arrays.asList(specs));
      return new AutoValue_PTransformTranslationTest_ToAndFromProtoSpec(topLevel, childSpecs);
    }

    abstract AppliedPTransform<?, ?, ?> getTransform();

    abstract Collection<ToAndFromProtoSpec> getChildren();
  }

  @Parameter(0)
  public ToAndFromProtoSpec spec;

  @Test
  public void toAndFromProto() throws IOException {
    SdkComponents components = SdkComponents.create(spec.getTransform().getPipeline().getOptions());
    RunnerApi.PTransform converted = convert(spec, components);
    Components protoComponents = components.toComponents();

    // Sanity checks
    assertThat(converted.getInputsCount(), equalTo(spec.getTransform().getInputs().size()));
    assertThat(converted.getOutputsCount(), equalTo(spec.getTransform().getOutputs().size()));
    assertThat(converted.getSubtransformsCount(), equalTo(spec.getChildren().size()));

    assertThat(converted.getUniqueName(), equalTo(spec.getTransform().getFullName()));
    for (PValue inputValue : spec.getTransform().getInputs().values()) {
      PCollection<?> inputPc = (PCollection<?>) inputValue;
      protoComponents.getPcollectionsOrThrow(components.registerPCollection(inputPc));
    }
    for (PValue outputValue : spec.getTransform().getOutputs().values()) {
      PCollection<?> outputPc = (PCollection<?>) outputValue;
      protoComponents.getPcollectionsOrThrow(components.registerPCollection(outputPc));
    }
  }

  private RunnerApi.PTransform convert(ToAndFromProtoSpec spec, SdkComponents components)
      throws IOException {
    List<AppliedPTransform<?, ?, ?>> childTransforms = new ArrayList<>();
    for (ToAndFromProtoSpec child : spec.getChildren()) {
      childTransforms.add(child.getTransform());
      System.out.println("Converting child " + child);
      convert(child, components);
      // Sanity call
      components.getExistingPTransformId(child.getTransform());
    }
    RunnerApi.PTransform convert =
        PTransformTranslation.toProto(spec.getTransform(), childTransforms, components);
    // Make sure the converted transform is registered. Convert it independently, but if this is a
    // child spec, the child must be in the components.
    components.registerPTransform(spec.getTransform(), childTransforms);
    return convert;
  }

  private static class TestDoFn extends DoFn<Long, KV<Long, String>> {
    // Exists to stop the ParDo application from throwing
    @ProcessElement
    public void process(ProcessContext context) {}
  }

  private static AppliedPTransform<?, ?, ?> generateSequence(Pipeline pipeline) {
    GenerateSequence sequence = GenerateSequence.from(0);
    PCollection<Long> pcollection = pipeline.apply(sequence);
    return AppliedPTransform.of(
        "Count", pipeline.begin().expand(), pcollection.expand(), sequence, pipeline);
  }

  private static AppliedPTransform<?, ?, ?> read(Pipeline pipeline) {
    Read.Unbounded<Long> transform = Read.from(CountingSource.unbounded());
    PCollection<Long> pcollection = pipeline.apply(transform);
    return AppliedPTransform.of(
        "ReadTheCount", pipeline.begin().expand(), pcollection.expand(), transform, pipeline);
  }

  private static AppliedPTransform<?, ?, ?> rawPTransformWithNullSpec(Pipeline pipeline) {
    PTransformTranslation.RawPTransform<PBegin, PDone> rawPTransform =
        new PTransformTranslation.RawPTransform<PBegin, PDone>() {
          @Override
          public String getUrn() {
            return "fake/urn";
          }

          @Override
          public RunnerApi.@Nullable FunctionSpec getSpec() {
            return null;
          }
        };
    return AppliedPTransform.<PBegin, PDone, PTransform<PBegin, PDone>>of(
        "RawPTransformWithNoSpec",
        pipeline.begin().expand(),
        PDone.in(pipeline).expand(),
        rawPTransform,
        pipeline);
  }

  private static AppliedPTransform<?, ?, ?> multiMultiParDo(Pipeline pipeline) {
    PCollectionView<String> view = pipeline.apply(Create.of("foo")).apply(View.asSingleton());
    PCollection<Long> input = pipeline.apply(GenerateSequence.from(0));
    ParDo.MultiOutput<Long, KV<Long, String>> parDo =
        ParDo.of(new TestDoFn())
            .withSideInputs(view)
            .withOutputTags(
                new TupleTag<KV<Long, String>>() {},
                TupleTagList.of(new TupleTag<KV<String, Long>>() {}));
    PCollectionTuple output = input.apply(parDo);

    Map<TupleTag<?>, PValue> inputs = new HashMap<>();
    inputs.putAll(parDo.getAdditionalInputs());
    inputs.putAll(input.expand());

    return AppliedPTransform
        .<PCollection<Long>, PCollectionTuple, ParDo.MultiOutput<Long, KV<Long, String>>>of(
            "MultiParDoInAndOut", inputs, output.expand(), parDo, pipeline);
  }
}
