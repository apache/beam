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
import static org.hamcrest.Matchers.hasItem;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.CombinePayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.construction.CoderTranslation.TranslationContext;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link PipelineTranslation}. */
@RunWith(Parameterized.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class PipelineTranslationTest {
  @Parameter(0)
  public Pipeline pipeline;

  @Parameters(name = "{index}")
  public static Iterable<Pipeline> testPipelines() {
    Pipeline trivialPipeline = Pipeline.create();
    trivialPipeline.apply(Create.of(1, 2, 3));

    Pipeline sideInputPipeline = Pipeline.create();
    final PCollectionView<String> singletonView =
        sideInputPipeline.apply(Create.of("foo")).apply(View.asSingleton());
    sideInputPipeline
        .apply(Create.of("main input"))
        .apply(
            ParDo.of(
                    new DoFn<String, String>() {
                      @ProcessElement
                      public void process(ProcessContext c) {
                        // actually never executed and no effect on translation
                        c.sideInput(singletonView);
                      }
                    })
                .withSideInputs(singletonView));

    Pipeline complexPipeline = Pipeline.create();
    BigEndianLongCoder customCoder = BigEndianLongCoder.of();
    PCollection<Long> elems = complexPipeline.apply(GenerateSequence.from(0L).to(207L));
    PCollection<Long> counted = elems.apply(Count.globally()).setCoder(customCoder);
    PCollection<Long> windowed =
        counted.apply(
            Window.<Long>into(FixedWindows.of(Duration.standardMinutes(7)))
                .triggering(
                    AfterWatermark.pastEndOfWindow()
                        .withLateFirings(AfterPane.elementCountAtLeast(19)))
                .accumulatingFiredPanes()
                .withAllowedLateness(Duration.standardMinutes(3L)));
    windowed.getWindowingStrategy();
    PCollection<KV<String, Long>> keyed = windowed.apply(WithKeys.of("foo"));
    keyed.apply(GroupByKey.create());
    return ImmutableList.of(trivialPipeline, sideInputPipeline, complexPipeline);
  }

  @Test
  public void testProtoDirectly() {
    final RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, false);
    pipeline.traverseTopologically(
        new PipelineProtoVerificationVisitor(pipelineProto, pipeline.getCoderRegistry(), false));
  }

  @Test
  public void testProtoDirectlyWithViewTransform() {
    final RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, true);
    pipeline.traverseTopologically(
        new PipelineProtoVerificationVisitor(pipelineProto, pipeline.getCoderRegistry(), true));
  }

  private static class PipelineProtoVerificationVisitor extends PipelineVisitor.Defaults {

    private final RunnerApi.Pipeline pipelineProto;
    private final CoderRegistry coderRegistry;
    private boolean useDeprecatedViewTransforms;
    Set<Node> transforms;
    Set<PCollection<?>> pcollections;
    Set<Coder<?>> coders;
    Set<WindowingStrategy<?, ?>> windowingStrategies;
    int missingViewTransforms = 0;

    public PipelineProtoVerificationVisitor(
        RunnerApi.Pipeline pipelineProto,
        CoderRegistry coderRegistry,
        boolean useDeprecatedViewTransforms) {
      this.pipelineProto = pipelineProto;
      this.coderRegistry = coderRegistry;
      this.useDeprecatedViewTransforms = useDeprecatedViewTransforms;
      transforms = new HashSet<>();
      pcollections = new HashSet<>();
      coders = new HashSet<>();
      windowingStrategies = new HashSet<>();
    }

    @Override
    public void leaveCompositeTransform(Node node) {
      if (node.isRootNode()) {
        assertThat(
            "Unexpected number of PTransforms",
            pipelineProto.getComponents().getTransformsCount(),
            equalTo(transforms.size() - missingViewTransforms));
        assertThat(
            "Unexpected number of PCollections",
            pipelineProto.getComponents().getPcollectionsCount(),
            equalTo(pcollections.size() - missingViewTransforms));
        assertThat(
            "Unexpected number of Coders",
            pipelineProto.getComponents().getCodersCount(),
            equalTo(coders.size()));
        assertThat(
            "Unexpected number of Windowing Strategies",
            pipelineProto.getComponents().getWindowingStrategiesCount(),
            equalTo(windowingStrategies.size()));
      } else {
        transforms.add(node);
        if (PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN.equals(
            PTransformTranslation.urnForTransformOrNull(node.getTransform()))) {
          // Combine translation introduces a coder that is not assigned to any PCollection
          // in the default expansion, and must be explicitly added here.
          try {
            addCoders(getAccumulatorCoder(node.toAppliedPTransform(getPipeline())));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    @Override
    public void visitPrimitiveTransform(Node node) {
      transforms.add(node);
      if (!useDeprecatedViewTransforms
          && PTransformTranslation.CREATE_VIEW_TRANSFORM_URN.equals(
              PTransformTranslation.urnForTransformOrNull(node.getTransform()))) {
        missingViewTransforms += 1;
      }
      if (PTransformTranslation.PAR_DO_TRANSFORM_URN.equals(
          PTransformTranslation.urnForTransformOrNull(node.getTransform()))) {
        final DoFn<?, ?> doFn;
        if (node.getTransform() instanceof ParDo.SingleOutput) {
          doFn = ((ParDo.SingleOutput) node.getTransform()).getFn();
        } else if (node.getTransform() instanceof ParDo.MultiOutput) {
          doFn = ((ParDo.MultiOutput) node.getTransform()).getFn();
        } else {
          throw new IllegalStateException(
              "Unexpected type of ParDo " + node.getTransform().getClass());
        }
        final DoFnSignature signature = DoFnSignatures.getSignature(doFn.getClass());
        if (signature.processElement().isSplittable()) {
          DoFnInvoker<?, ?> doFnInvoker = DoFnInvokers.invokerFor(doFn);
          final Coder<?> restrictionAndWatermarkStateCoder =
              KvCoder.of(
                  doFnInvoker.invokeGetRestrictionCoder(coderRegistry),
                  doFnInvoker.invokeGetWatermarkEstimatorStateCoder(coderRegistry));
          addCoders(restrictionAndWatermarkStateCoder);
        }
      }
    }

    @Override
    public void visitValue(PValue value, Node producer) {
      if (value instanceof PCollection) {
        PCollection pc = (PCollection) value;
        pcollections.add(pc);
        addCoders(pc.getCoder());
        windowingStrategies.add(pc.getWindowingStrategy());
        addCoders(pc.getWindowingStrategy().getWindowFn().windowCoder());
      }
    }

    private void addCoders(Coder<?> coder) {
      coders.add(coder);
      if (CoderTranslation.getKnownCoderUrns().containsKey(coder.getClass())) {
        for (Coder<?> component : ((StructuredCoder<?>) coder).getComponents()) {
          addCoders(component);
        }
      }
    }
  }

  private static Coder<?> getAccumulatorCoder(AppliedPTransform<?, ?, ?> transform)
      throws IOException {
    SdkComponents sdkComponents = SdkComponents.create(transform.getPipeline().getOptions());
    String id =
        getCombinePayload(transform, sdkComponents)
            .map(CombinePayload::getAccumulatorCoderId)
            .orElseThrow(() -> new IOException("Transform does not contain an AccumulatorCoder"));
    Components components = sdkComponents.toComponents();
    return CoderTranslation.fromProto(
        components.getCodersOrThrow(id),
        RehydratedComponents.forComponents(components),
        TranslationContext.DEFAULT);
  }

  private static Optional<CombinePayload> getCombinePayload(
      AppliedPTransform<?, ?, ?> transform, SdkComponents components) throws IOException {
    RunnerApi.PTransform proto =
        PTransformTranslation.toProto(transform, Collections.emptyList(), components);

    // Even if the proto has no spec, calling getSpec still returns a blank spec, which we want to
    // avoid. It should be clear to the caller whether or not there was a spec in the transform.
    if (proto.hasSpec()) {
      return Optional.of(CombinePayload.parseFrom(proto.getSpec().getPayload()));
    } else {
      return Optional.empty();
    }
  }

  // Static, out-of-line for serialization.
  private static class DoFnRequiringStableInput extends DoFn<Integer, String> {
    @RequiresStableInput
    @ProcessElement
    public void process(ProcessContext c) {
      // actually never executed and no effect on translation
    }
  }

  @Test
  public void testRequirements() {
    Pipeline pipeline = Pipeline.create();
    pipeline.apply(Create.of(1, 2, 3)).apply(ParDo.of(new DoFnRequiringStableInput()));
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, false);
    assertThat(
        pipelineProto.getRequirementsList(), hasItem(ParDoTranslation.REQUIRES_STABLE_INPUT_URN));
  }

  private RunnerApi.PTransform getLeafTransform(RunnerApi.Pipeline pipelineProto, String prefix) {
    for (RunnerApi.PTransform transform :
        pipelineProto.getComponents().getTransformsMap().values()) {
      if (transform.getUniqueName().startsWith(prefix) && transform.getSubtransformsCount() == 0) {
        return transform;
      }
    }
    throw new java.lang.IllegalArgumentException(prefix);
  }

  private static class IdentityDoFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(@Element T input, OutputReceiver<T> out) {
      out.output(input);
    }
  }

  @Test
  public void testResourceHints() {
    Pipeline pipeline = Pipeline.create();
    PCollection<byte[]> root = pipeline.apply(Impulse.create());
    ParDo.SingleOutput<byte[], byte[]> transform = ParDo.of(new IdentityDoFn<byte[]>());
    root.apply("Big", transform.setResourceHints(ResourceHints.create().withMinRam("640KB")));
    root.apply("Small", transform.setResourceHints(ResourceHints.create().withMinRam(1)));
    root.apply(
        "AnotherBig", transform.setResourceHints(ResourceHints.create().withMinRam("640KB")));
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(pipeline, false);
    assertThat(
        pipelineProto
            .getComponents()
            .getEnvironmentsMap()
            .get(getLeafTransform(pipelineProto, "Big").getEnvironmentId())
            .getResourceHintsMap(),
        org.hamcrest.Matchers.hasEntry(
            "beam:resources:min_ram_bytes:v1", ByteString.copyFromUtf8("640000")));
    assertThat(
        pipelineProto
            .getComponents()
            .getEnvironmentsMap()
            .get(getLeafTransform(pipelineProto, "Small").getEnvironmentId())
            .getResourceHintsMap(),
        org.hamcrest.Matchers.hasEntry(
            "beam:resources:min_ram_bytes:v1", ByteString.copyFromUtf8("1")));
    // Ensure we re-use environments.
    assertThat(
        getLeafTransform(pipelineProto, "Big").getEnvironmentId(),
        equalTo(getLeafTransform(pipelineProto, "AnotherBig").getEnvironmentId()));
  }
}
