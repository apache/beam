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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.PTransformTranslation.RawPTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/** Utilities for going to/from Runner API pipelines. */
public class PipelineTranslation {

  public static RunnerApi.Pipeline toProto(Pipeline pipeline) {
    return toProto(pipeline, SdkComponents.create());
  }

  public static RunnerApi.Pipeline toProto(
      final Pipeline pipeline, final SdkComponents components) {
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
                  node.toAppliedPTransform(pipeline), Collections.emptyList());
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

  public static Pipeline fromProto(final RunnerApi.Pipeline pipelineProto) throws IOException {
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

    RawPTransform<?, ?> transform =
        PTransformTranslation.rehydrate(transformProto, rehydratedComponents);

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

  private static Map<TupleTag<?>, PValue> sideInputMapToAdditionalInputs(
      RunnerApi.PTransform transformProto,
      RehydratedComponents rehydratedComponents,
      Map<TupleTag<?>, PValue> rehydratedInputs,
      Map<String, RunnerApi.SideInput> sideInputsMap)
      throws IOException {
    List<PCollectionView<?>> views = new ArrayList<>();
    for (Map.Entry<String, RunnerApi.SideInput> sideInputEntry : sideInputsMap.entrySet()) {
      String localName = sideInputEntry.getKey();
      RunnerApi.SideInput sideInput = sideInputEntry.getValue();
      PCollection<?> pCollection =
          (PCollection<?>) checkNotNull(rehydratedInputs.get(new TupleTag<>(localName)));
      views.add(
          PCollectionViewTranslation.viewFromProto(
              sideInput, localName, pCollection, transformProto, rehydratedComponents));
    }
    return PCollectionViews.toAdditionalInputs(views);
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
}
