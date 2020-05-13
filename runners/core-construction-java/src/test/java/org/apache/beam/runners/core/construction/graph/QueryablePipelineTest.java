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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables.getOnlyElement;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.FunctionSpec;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.SideInput;
import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link QueryablePipeline}. */
@RunWith(JUnit4.class)
public class QueryablePipelineTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  /**
   * Constructing a {@link QueryablePipeline} with components that reference absent {@link
   * RunnerApi.PCollection PCollections} should fail.
   */
  @Test
  public void fromEmptyComponents() {
    // Not that it's hugely useful, but it shouldn't throw.
    QueryablePipeline p = QueryablePipeline.forPrimitivesIn(Components.getDefaultInstance());
    assertThat(p.getRootTransforms(), emptyIterable());
  }

  @Test
  public void fromComponentsWithMalformedComponents() {
    Components components =
        Components.newBuilder()
            .putTransforms(
                "root",
                PTransform.newBuilder()
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.IMPULSE_TRANSFORM_URN)
                            .build())
                    .putOutputs("output", "output.out")
                    .build())
            .build();

    thrown.expect(IllegalArgumentException.class);
    QueryablePipeline.forPrimitivesIn(components).getComponents();
  }

  @Test
  public void forTransformsWithMalformedGraph() {
    Components components =
        Components.newBuilder()
            .putTransforms(
                "root", PTransform.newBuilder().putOutputs("output", "output.out").build())
            .putPcollections(
                "output.out",
                RunnerApi.PCollection.newBuilder().setUniqueName("output.out").build())
            .putTransforms(
                "consumer", PTransform.newBuilder().putInputs("input", "output.out").build())
            .build();

    thrown.expect(IllegalArgumentException.class);
    // Consumer consumes a PCollection which isn't produced.
    QueryablePipeline.forTransforms(ImmutableSet.of("consumer"), components);
  }

  @Test
  public void forTransformsWithSubgraph() {
    Components components =
        Components.newBuilder()
            .putTransforms(
                "root", PTransform.newBuilder().putOutputs("output", "output.out").build())
            .putPcollections(
                "output.out",
                RunnerApi.PCollection.newBuilder().setUniqueName("output.out").build())
            .putTransforms(
                "consumer", PTransform.newBuilder().putInputs("input", "output.out").build())
            .putTransforms(
                "ignored", PTransform.newBuilder().putInputs("input", "output.out").build())
            .build();

    QueryablePipeline pipeline =
        QueryablePipeline.forTransforms(ImmutableSet.of("root", "consumer"), components);

    assertThat(
        pipeline.getRootTransforms(),
        contains(PipelineNode.pTransform("root", components.getTransformsOrThrow("root"))));

    Set<PTransformNode> consumers =
        pipeline.getPerElementConsumers(
            PipelineNode.pCollection(
                "output.out", components.getPcollectionsOrThrow("output.out")));

    assertThat(
        consumers,
        contains(PipelineNode.pTransform("consumer", components.getTransformsOrThrow("consumer"))));
  }

  @Test
  public void rootTransforms() {
    Pipeline p = Pipeline.create();
    p.apply("UnboundedRead", Read.from(CountingSource.unbounded()))
        .apply(Window.into(FixedWindows.of(Duration.millis(5L))))
        .apply(Count.perElement());
    p.apply("BoundedRead", Read.from(CountingSource.upTo(100L)));

    Components components = PipelineTranslation.toProto(p).getComponents();
    QueryablePipeline qp = QueryablePipeline.forPrimitivesIn(components);

    assertThat(qp.getRootTransforms(), hasSize(2));
    for (PTransformNode rootTransform : qp.getRootTransforms()) {
      assertThat(
          "Root transforms should have no inputs",
          rootTransform.getTransform().getInputsCount(),
          equalTo(0));
      assertThat(
          "Only added source reads to the pipeline",
          rootTransform.getTransform().getSpec().getUrn(),
          equalTo(PTransformTranslation.READ_TRANSFORM_URN));
    }
  }

  /**
   * Tests that inputs that are only side inputs are not returned from {@link
   * QueryablePipeline#getPerElementConsumers(PCollectionNode)} and are returned from {@link
   * QueryablePipeline#getSideInputs(PTransformNode)}.
   */
  @Test
  public void transformWithSideAndMainInputs() {
    Pipeline p = Pipeline.create();
    PCollection<Long> longs = p.apply("BoundedRead", Read.from(CountingSource.upTo(100L)));
    PCollectionView<String> view =
        p.apply("Create", Create.of("foo")).apply("View", View.asSingleton());
    longs.apply(
        "par_do",
        ParDo.of(new TestFn())
            .withSideInputs(view)
            .withOutputTags(new TupleTag<>(), TupleTagList.empty()));

    Components components = PipelineTranslation.toProto(p).getComponents();
    QueryablePipeline qp = QueryablePipeline.forPrimitivesIn(components);

    String mainInputName =
        getOnlyElement(
            PipelineNode.pTransform("BoundedRead", components.getTransformsOrThrow("BoundedRead"))
                .getTransform()
                .getOutputsMap()
                .values());
    PCollectionNode mainInput =
        PipelineNode.pCollection(mainInputName, components.getPcollectionsOrThrow(mainInputName));
    PTransform parDoTransform = components.getTransformsOrThrow("par_do");
    String sideInputLocalName =
        getOnlyElement(
            parDoTransform.getInputsMap().entrySet().stream()
                .filter(entry -> !entry.getValue().equals(mainInputName))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet()));
    String sideInputCollectionId = parDoTransform.getInputsOrThrow(sideInputLocalName);
    PCollectionNode sideInput =
        PipelineNode.pCollection(
            sideInputCollectionId, components.getPcollectionsOrThrow(sideInputCollectionId));
    PTransformNode parDoNode =
        PipelineNode.pTransform("par_do", components.getTransformsOrThrow("par_do"));
    SideInputReference sideInputRef =
        SideInputReference.of(parDoNode, sideInputLocalName, sideInput);

    assertThat(qp.getSideInputs(parDoNode), contains(sideInputRef));
    assertThat(qp.getPerElementConsumers(mainInput), contains(parDoNode));
    assertThat(qp.getPerElementConsumers(sideInput), not(contains(parDoNode)));
  }

  /**
   * Tests that inputs that are both side inputs and main inputs are returned from {@link
   * QueryablePipeline#getPerElementConsumers(PCollectionNode)} and {@link
   * QueryablePipeline#getSideInputs(PTransformNode)}.
   */
  @Test
  public void transformWithSameSideAndMainInput() {
    Components components =
        Components.newBuilder()
            .putPcollections("read_pc", RunnerApi.PCollection.getDefaultInstance())
            .putPcollections("pardo_out", RunnerApi.PCollection.getDefaultInstance())
            .putTransforms(
                "root",
                PTransform.newBuilder()
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.IMPULSE_TRANSFORM_URN)
                            .build())
                    .putOutputs("out", "read_pc")
                    .build())
            .putTransforms(
                "multiConsumer",
                PTransform.newBuilder()
                    .putInputs("main_in", "read_pc")
                    .putInputs("side_in", "read_pc")
                    .putOutputs("out", "pardo_out")
                    .setSpec(
                        FunctionSpec.newBuilder()
                            .setUrn(PTransformTranslation.PAR_DO_TRANSFORM_URN)
                            .setPayload(
                                ParDoPayload.newBuilder()
                                    .putSideInputs("side_in", SideInput.getDefaultInstance())
                                    .build()
                                    .toByteString())
                            .build())
                    .build())
            .build();

    QueryablePipeline qp = QueryablePipeline.forPrimitivesIn(components);
    PCollectionNode multiInputPc =
        PipelineNode.pCollection("read_pc", components.getPcollectionsOrThrow("read_pc"));
    PTransformNode multiConsumerPT =
        PipelineNode.pTransform("multiConsumer", components.getTransformsOrThrow("multiConsumer"));
    SideInputReference sideInputRef =
        SideInputReference.of(multiConsumerPT, "side_in", multiInputPc);
    assertThat(qp.getPerElementConsumers(multiInputPc), contains(multiConsumerPT));
    assertThat(qp.getSideInputs(multiConsumerPT), contains(sideInputRef));
  }

  /**
   * Tests that {@link QueryablePipeline#getPerElementConsumers(PCollectionNode)} returns a
   * transform that consumes the node more than once.
   */
  @Test
  public void perElementConsumersWithConsumingMultipleTimes() {
    Pipeline p = Pipeline.create();
    PCollection<Long> longs = p.apply("BoundedRead", Read.from(CountingSource.upTo(100L)));
    PCollectionList.of(longs).and(longs).and(longs).apply("flatten", Flatten.pCollections());

    Components components = PipelineTranslation.toProto(p).getComponents();
    // This breaks if the way that IDs are assigned to PTransforms changes in PipelineTranslation
    String readOutput =
        getOnlyElement(components.getTransformsOrThrow("BoundedRead").getOutputsMap().values());
    QueryablePipeline qp = QueryablePipeline.forPrimitivesIn(components);
    Set<PTransformNode> consumers =
        qp.getPerElementConsumers(
            PipelineNode.pCollection(readOutput, components.getPcollectionsOrThrow(readOutput)));

    assertThat(consumers.size(), equalTo(1));
    assertThat(
        getOnlyElement(consumers).getTransform().getSpec().getUrn(),
        equalTo(PTransformTranslation.FLATTEN_TRANSFORM_URN));
  }

  @Test
  public void getProducer() {
    Pipeline p = Pipeline.create();
    PCollection<Long> longs = p.apply("BoundedRead", Read.from(CountingSource.upTo(100L)));
    PCollectionList.of(longs).and(longs).and(longs).apply("flatten", Flatten.pCollections());

    Components components = PipelineTranslation.toProto(p).getComponents();
    QueryablePipeline qp = QueryablePipeline.forPrimitivesIn(components);

    String longsOutputName =
        getOnlyElement(
            PipelineNode.pTransform("BoundedRead", components.getTransformsOrThrow("BoundedRead"))
                .getTransform()
                .getOutputsMap()
                .values());
    PTransformNode longsProducer =
        PipelineNode.pTransform("BoundedRead", components.getTransformsOrThrow("BoundedRead"));
    PCollectionNode longsOutput =
        PipelineNode.pCollection(
            longsOutputName, components.getPcollectionsOrThrow(longsOutputName));
    String flattenOutputName =
        getOnlyElement(
            PipelineNode.pTransform("flatten", components.getTransformsOrThrow("flatten"))
                .getTransform()
                .getOutputsMap()
                .values());
    PTransformNode flattenProducer =
        PipelineNode.pTransform("flatten", components.getTransformsOrThrow("flatten"));
    PCollectionNode flattenOutput =
        PipelineNode.pCollection(
            flattenOutputName, components.getPcollectionsOrThrow(flattenOutputName));

    assertThat(qp.getProducer(longsOutput), equalTo(longsProducer));
    assertThat(qp.getProducer(flattenOutput), equalTo(flattenProducer));
  }

  @Test
  public void getEnvironmentWithEnvironment() {
    Pipeline p = Pipeline.create();
    PCollection<Long> longs = p.apply("BoundedRead", Read.from(CountingSource.upTo(100L)));
    longs.apply(WithKeys.of("a")).apply("groupByKey", GroupByKey.create());

    Components components = PipelineTranslation.toProto(p).getComponents();
    QueryablePipeline qp = QueryablePipeline.forPrimitivesIn(components);

    PTransformNode environmentalRead =
        PipelineNode.pTransform("BoundedRead", components.getTransformsOrThrow("BoundedRead"));
    PTransformNode nonEnvironmentalTransform =
        PipelineNode.pTransform("groupByKey", components.getTransformsOrThrow("groupByKey"));

    assertThat(qp.getEnvironment(environmentalRead).isPresent(), is(true));
    assertThat(
        qp.getEnvironment(environmentalRead).get().getUrn(),
        equalTo(Environments.JAVA_SDK_HARNESS_ENVIRONMENT.getUrn()));
    assertThat(
        qp.getEnvironment(environmentalRead).get().getPayload(),
        equalTo(Environments.JAVA_SDK_HARNESS_ENVIRONMENT.getPayload()));
    assertThat(qp.getEnvironment(nonEnvironmentalTransform).isPresent(), is(false));
  }

  private static class TestFn extends DoFn<Long, Long> {
    @ProcessElement
    public void process(ProcessContext ctxt) {}
  }

  @Test
  public void retainOnlyPrimitivesWithOnlyPrimitivesUnchanged() {
    Pipeline p = Pipeline.create();
    p.apply("Read", Read.from(CountingSource.unbounded()))
        .apply(
            "multi-do",
            ParDo.of(new TestFn()).withOutputTags(new TupleTag<>(), TupleTagList.empty()));

    Components originalComponents = PipelineTranslation.toProto(p).getComponents();
    Collection<String> primitiveComponents =
        QueryablePipeline.getPrimitiveTransformIds(originalComponents);

    assertThat(primitiveComponents, equalTo(originalComponents.getTransformsMap().keySet()));
  }

  @Test
  public void retainOnlyPrimitivesComposites() {
    Pipeline p = Pipeline.create();
    p.apply(
        new org.apache.beam.sdk.transforms.PTransform<PBegin, PCollection<Long>>() {
          @Override
          public PCollection<Long> expand(PBegin input) {
            return input
                .apply(GenerateSequence.from(2L))
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(5L))))
                .apply(MapElements.into(TypeDescriptors.longs()).via(l -> l + 1));
          }
        });

    Components originalComponents = PipelineTranslation.toProto(p).getComponents();
    Collection<String> primitiveComponents =
        QueryablePipeline.getPrimitiveTransformIds(originalComponents);

    // Read, Window.Assign, ParDo. This will need to be updated if the expansions change.
    assertThat(primitiveComponents, hasSize(3));
    for (String transformId : primitiveComponents) {
      assertThat(originalComponents.getTransformsMap(), hasKey(transformId));
    }
  }

  /** This method doesn't do any pruning for reachability, but this may not require a test. */
  @Test
  public void retainOnlyPrimitivesIgnoresUnreachableNodes() {
    Pipeline p = Pipeline.create();
    p.apply(
        new org.apache.beam.sdk.transforms.PTransform<PBegin, PCollection<Long>>() {
          @Override
          public PCollection<Long> expand(PBegin input) {
            return input
                .apply(GenerateSequence.from(2L))
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(5L))))
                .apply(MapElements.into(TypeDescriptors.longs()).via(l -> l + 1));
          }
        });

    Components augmentedComponents =
        PipelineTranslation.toProto(p)
            .getComponents()
            .toBuilder()
            .putCoders("extra-coder", RunnerApi.Coder.getDefaultInstance())
            .putWindowingStrategies(
                "extra-windowing-strategy", RunnerApi.WindowingStrategy.getDefaultInstance())
            .putEnvironments("extra-env", RunnerApi.Environment.getDefaultInstance())
            .putPcollections("extra-pc", RunnerApi.PCollection.getDefaultInstance())
            .build();
    Collection<String> primitiveComponents =
        QueryablePipeline.getPrimitiveTransformIds(augmentedComponents);
  }
}
