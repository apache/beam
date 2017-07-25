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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.protobuf.Any;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.PTransformTranslation.RawPTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/** Utilities for going to/from Runner API pipelines. */
public class PipelineTranslation {

  public static RunnerApi.Pipeline toProto(final Pipeline pipeline) {
    final SdkComponents components = SdkComponents.create();
    final Collection<String> rootIds = new HashSet<>();
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          private final ListMultimap<Node, AppliedPTransform<?, ?, ?>> children =
              ArrayListMultimap.create();

          @Override
          public void leaveCompositeTransform(Node node) {
            if (node.isRootNode()) {
              for (AppliedPTransform<?, ?, ?> pipelineRoot : children.get(node)) {
                rootIds.add(components.getExistingPTransformId(pipelineRoot));
              }
            } else {
              // TODO: Include DisplayData in the proto
              children.put(node.getEnclosingNode(), node.toAppliedPTransform(pipeline));
              try {
                components.registerPTransform(
                    node.toAppliedPTransform(pipeline), children.get(node));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          }

          @Override
          public void visitPrimitiveTransform(Node node) {
            // TODO: Include DisplayData in the proto
            children.put(node.getEnclosingNode(), node.toAppliedPTransform(pipeline));
            try {
              components.registerPTransform(
                  node.toAppliedPTransform(pipeline),
                  Collections.<AppliedPTransform<?, ?, ?>>emptyList());
            } catch (IOException e) {
              throw new IllegalStateException(e);
            }
          }
        });
    return RunnerApi.Pipeline.newBuilder()
        .setComponents(components.toComponents())
        .addAllRootTransformIds(rootIds)
        .build();
  }

  private static DisplayData evaluateDisplayData(HasDisplayData component) {
    return DisplayData.from(component);
  }

  public static Pipeline fromProto(final RunnerApi.Pipeline pipelineProto)
      throws IOException {
    TransformHierarchy transforms = new TransformHierarchy();
    Pipeline pipeline = Pipeline.forTransformHierarchy(transforms, PipelineOptionsFactory.create());

    // Keeping the PCollections straight is a semantic necessity, but being careful not to explode
    // the number of coders and windowing strategies is also nice, and helps testing.
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(pipelineProto.getComponents()).withPipeline(pipeline);

    for (String rootId : pipelineProto.getRootTransformIdsList()) {
      addRehydratedTransform(
          transforms,
          pipelineProto.getComponents().getTransformsOrThrow(rootId),
          pipeline,
          pipelineProto.getComponents().getTransformsMap(),
          rehydratedComponents);
    }

    return pipeline;
  }

  private static void addRehydratedTransform(
      TransformHierarchy transforms,
      RunnerApi.PTransform transformProto,
      Pipeline pipeline,
      Map<String, RunnerApi.PTransform> transformProtos,
      RehydratedComponents rehydratedComponents)
      throws IOException {

    Map<TupleTag<?>, PValue> rehydratedInputs = new HashMap<>();
    for (Map.Entry<String, String> inputEntry : transformProto.getInputsMap().entrySet()) {
      rehydratedInputs.put(
          new TupleTag<>(inputEntry.getKey()),
          rehydratedComponents.getPCollection(inputEntry.getValue()));
    }

    Map<TupleTag<?>, PValue> rehydratedOutputs = new HashMap<>();
    for (Map.Entry<String, String> outputEntry : transformProto.getOutputsMap().entrySet()) {
      rehydratedOutputs.put(
          new TupleTag<>(outputEntry.getKey()),
          rehydratedComponents.getPCollection(outputEntry.getValue()));
    }

    RunnerApi.FunctionSpec transformSpec = transformProto.getSpec();

    // By default, no "additional" inputs, since that is an SDK-specific thing.
    // Only ParDo really separates main from side inputs
    Map<TupleTag<?>, PValue> additionalInputs = Collections.emptyMap();

    // TODO: ParDoTranslator should own it - https://issues.apache.org/jira/browse/BEAM-2674
    if (transformSpec.getUrn().equals(PTransformTranslation.PAR_DO_TRANSFORM_URN)) {
      RunnerApi.ParDoPayload payload =
          transformSpec.getParameter().unpack(RunnerApi.ParDoPayload.class);

      List<PCollectionView<?>> views = new ArrayList<>();
      for (Map.Entry<String, RunnerApi.SideInput> sideInputEntry :
          payload.getSideInputsMap().entrySet()) {
        String localName = sideInputEntry.getKey();
        RunnerApi.SideInput sideInput = sideInputEntry.getValue();
        PCollection<?> pCollection =
            (PCollection<?>) checkNotNull(rehydratedInputs.get(new TupleTag<>(localName)));
        views.add(
            ParDoTranslation.viewFromProto(
                sideInputEntry.getValue(),
                sideInputEntry.getKey(),
                pCollection,
                transformProto,
                rehydratedComponents));
      }
      additionalInputs = PCollectionViews.toAdditionalInputs(views);
    }

    // TODO: CombineTranslator should own it - https://issues.apache.org/jira/browse/BEAM-2674
    List<Coder<?>> additionalCoders = Collections.emptyList();
    if (transformSpec.getUrn().equals(PTransformTranslation.COMBINE_TRANSFORM_URN)) {
      RunnerApi.CombinePayload payload =
          transformSpec.getParameter().unpack(RunnerApi.CombinePayload.class);
      additionalCoders =
          (List)
              Collections.singletonList(
                  rehydratedComponents.getCoder(payload.getAccumulatorCoderId()));
    }

    RehydratedPTransform transform =
        RehydratedPTransform.of(
            transformSpec.getUrn(),
            transformSpec.getParameter(),
            additionalInputs,
            additionalCoders);

    if (isPrimitive(transformProto)) {
      transforms.addFinalizedPrimitiveNode(
          transformProto.getUniqueName(), rehydratedInputs, transform, rehydratedOutputs);
    } else {
      transforms.pushFinalizedNode(
          transformProto.getUniqueName(), rehydratedInputs, transform, rehydratedOutputs);

      for (String childTransformId : transformProto.getSubtransformsList()) {
        addRehydratedTransform(
            transforms,
            transformProtos.get(childTransformId),
            pipeline,
            transformProtos,
            rehydratedComponents);
      }

      transforms.popNode();
    }
  }

  // A primitive transform is one with outputs that are not in its input and also
  // not produced by a subtransform.
  private static boolean isPrimitive(RunnerApi.PTransform transformProto) {
    return transformProto.getSubtransformsCount() == 0
        && !transformProto
        .getInputsMap()
        .values()
        .containsAll(transformProto.getOutputsMap().values());
  }

  @AutoValue
  abstract static class RehydratedPTransform extends RawPTransform<PInput, POutput> {

    @Nullable
    public abstract String getUrn();

    @Nullable
    public abstract Any getPayload();

    @Override
    public abstract Map<TupleTag<?>, PValue> getAdditionalInputs();

    public abstract List<Coder<?>> getCoders();

    public static RehydratedPTransform of(
        String urn,
        Any payload,
        Map<TupleTag<?>, PValue> additionalInputs,
        List<Coder<?>> additionalCoders) {
      return new AutoValue_PipelineTranslation_RehydratedPTransform(
          urn, payload, additionalInputs, additionalCoders);
    }

    @Override
    public POutput expand(PInput input) {
      throw new IllegalStateException(
          String.format(
              "%s should never be asked to expand;"
                  + " it is the result of deserializing an already-constructed Pipeline",
              getClass().getSimpleName()));
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("urn", getUrn())
          .add("payload", getPayload())
          .toString();
    }

    @Override
    public void registerComponents(SdkComponents components) {
      for (Coder<?> coder : getCoders()) {
        try {
          components.registerCoder(coder);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
