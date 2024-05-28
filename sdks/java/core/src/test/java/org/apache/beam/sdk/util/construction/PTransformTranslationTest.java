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

import static org.apache.beam.model.pipeline.v1.ExternalTransforms.Annotations.Enum.CONFIG_ROW_KEY;
import static org.apache.beam.model.pipeline.v1.ExternalTransforms.Annotations.Enum.CONFIG_ROW_SCHEMA_KEY;
import static org.apache.beam.model.pipeline.v1.ExternalTransforms.Annotations.Enum.MANAGED_UNDERLYING_TRANSFORM_URN_KEY;
import static org.apache.beam.model.pipeline.v1.ExternalTransforms.Annotations.Enum.SCHEMATRANSFORM_URN_KEY;
import static org.apache.beam.sdk.util.construction.PTransformTranslation.MANAGED_TRANSFORM_URN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

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
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.managed.ManagedSchemaTransformProvider;
import org.apache.beam.sdk.providers.GenerateSequenceSchemaTransformProvider;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.utils.YamlUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.PValues;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
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
  public static Iterable<ToAndFromProtoSpec> data() throws CoderException {
    // This pipeline exists for construction, not to run any test.
    // TODO: Leaf node with understood payload - i.e. validate payloads
    ToAndFromProtoSpec readLeaf = ToAndFromProtoSpec.leaf(read(TestPipeline.create()));

    ToAndFromProtoSpec readMultipleInAndOut =
        ToAndFromProtoSpec.leaf(multiMultiParDo(TestPipeline.create()));

    TestPipeline compositeReadPipeline = TestPipeline.create();
    ToAndFromProtoSpec compositeRead =
        ToAndFromProtoSpec.composite(
            generateSequence(compositeReadPipeline),
            ToAndFromProtoSpec.leaf(read(compositeReadPipeline)),
            Collections.emptyMap());

    TestPipeline compositeManagedReadPipeline = TestPipeline.create();
    ToAndFromProtoSpec compositeManagedRead =
        managedGenerateSequenceSpec(compositeManagedReadPipeline);

    ToAndFromProtoSpec rawLeafNullSpec =
        ToAndFromProtoSpec.leaf(rawPTransformWithNullSpec(TestPipeline.create()));

    return ImmutableList.<ToAndFromProtoSpec>builder()
        .add(readLeaf)
        .add(readMultipleInAndOut)
        .add(compositeRead)
        .add(compositeManagedRead)
        .add(rawLeafNullSpec)
        // TODO: Composite with multiple children
        // TODO: Composite with a composite child
        .build();
  }

  @AutoValue
  abstract static class ToAndFromProtoSpec {
    public static ToAndFromProtoSpec leaf(AppliedPTransform<?, ?, ?> transform) {
      return new AutoValue_PTransformTranslationTest_ToAndFromProtoSpec(
          transform, Collections.emptyList(), Collections.emptyMap());
    }

    public static ToAndFromProtoSpec composite(
        AppliedPTransform<?, ?, ?> topLevel,
        ToAndFromProtoSpec spec,
        Map<String, ByteString> annotations,
        ToAndFromProtoSpec... specs) {
      List<ToAndFromProtoSpec> childSpecs = new ArrayList<>();
      childSpecs.add(spec);
      childSpecs.addAll(Arrays.asList(specs));
      return new AutoValue_PTransformTranslationTest_ToAndFromProtoSpec(
          topLevel, childSpecs, annotations);
    }

    abstract AppliedPTransform<?, ?, ?> getTransform();

    abstract Collection<ToAndFromProtoSpec> getChildren();

    abstract Map<String, ByteString> getAnnotations();
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
    assertThat(converted.getAnnotationsMap(), equalTo(spec.getAnnotations()));

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
        "Count",
        PValues.expandInput(pipeline.begin()),
        PValues.expandOutput(pcollection),
        sequence,
        ResourceHints.create(),
        pipeline);
  }

  private static ToAndFromProtoSpec managedGenerateSequenceSpec(Pipeline pipeline)
      throws CoderException {
    GenerateSequenceSchemaTransformProvider generateProvider =
        new GenerateSequenceSchemaTransformProvider();
    ManagedSchemaTransformProvider managedProvider =
        new ManagedSchemaTransformProvider(Arrays.asList(generateProvider.identifier()));
    Map<String, Object> config =
        ImmutableMap.<String, Object>builder().put("start", 0L).put("end", 10L).build();
    Row managedConfigRow =
        Row.withSchema(managedProvider.configurationSchema())
            .withFieldValue("transform_identifier", generateProvider.identifier())
            .withFieldValue("config", YamlUtils.yamlStringFromMap(config))
            .build();
    SchemaTransform generateSequenceST = managedProvider.from(managedConfigRow);
    PCollection<Row> pcollection =
        PCollectionRowTuple.empty(pipeline)
            .apply(generateSequenceST)
            .get(GenerateSequenceSchemaTransformProvider.OUTPUT_ROWS_TAG);

    AppliedPTransform<?, ?, ?> managedGenerateSequence =
        AppliedPTransform.of(
            "Managed Count",
            PValues.expandInput(pipeline.begin()),
            PValues.expandOutput(pcollection),
            generateSequenceST,
            ResourceHints.create(),
            pipeline);

    return ToAndFromProtoSpec.composite(
        managedGenerateSequence,
        ToAndFromProtoSpec.leaf(read(pipeline)),
        ImmutableMap.<String, ByteString>builder()
            .put(
                BeamUrns.getConstant(SCHEMATRANSFORM_URN_KEY),
                ByteString.copyFromUtf8(MANAGED_TRANSFORM_URN))
            .put(
                BeamUrns.getConstant(MANAGED_UNDERLYING_TRANSFORM_URN_KEY),
                ByteString.copyFromUtf8(generateProvider.identifier()))
            .put(
                BeamUrns.getConstant(CONFIG_ROW_KEY),
                ByteString.copyFrom(
                    CoderUtils.encodeToByteArray(
                        RowCoder.of(managedConfigRow.getSchema()), managedConfigRow)))
            .put(
                BeamUrns.getConstant(CONFIG_ROW_SCHEMA_KEY),
                ByteString.copyFrom(
                    SchemaTranslation.schemaToProto(managedConfigRow.getSchema(), true)
                        .toByteArray()))
            .build());
  }

  private static AppliedPTransform<?, ?, ?> read(Pipeline pipeline) {
    Read.Unbounded<Long> transform = Read.from(CountingSource.unbounded());
    PCollection<Long> pcollection = pipeline.apply(transform);
    return AppliedPTransform.of(
        "ReadTheCount",
        PValues.expandInput(pipeline.begin()),
        PValues.expandOutput(pcollection),
        transform,
        ResourceHints.create(),
        pipeline);
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
        PValues.expandInput(pipeline.begin()),
        PValues.expandOutput(PDone.in(pipeline)),
        rawPTransform,
        ResourceHints.create(),
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

    Map<TupleTag<?>, PCollection<?>> inputs = new HashMap<>();
    inputs.putAll(PValues.fullyExpand(parDo.getAdditionalInputs()));
    inputs.putAll(PValues.expandInput(input));

    return AppliedPTransform
        .<PCollection<Long>, PCollectionTuple, ParDo.MultiOutput<Long, KV<Long, String>>>of(
            "MultiParDoInAndOut",
            inputs,
            PValues.expandOutput(output),
            parDo,
            ResourceHints.create(),
            pipeline);
  }
}
