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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.PipelineValidator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ListMultimap;

/** Utilities for going to/from Runner API pipelines. */
public class PipelineTranslation {

  public static RunnerApi.Pipeline toProto(Pipeline pipeline) {
    return toProto(pipeline, SdkComponents.create(pipeline.getOptions()));
  }

  public static RunnerApi.Pipeline toProto(Pipeline pipeline, boolean useDeprecatedViewTransforms) {
    return toProto(
        pipeline, SdkComponents.create(pipeline.getOptions()), useDeprecatedViewTransforms);
  }

  public static RunnerApi.Pipeline toProto(Pipeline pipeline, SdkComponents components) {
    return toProto(pipeline, components, false);
  }

  public static RunnerApi.Pipeline toProto(
      final Pipeline pipeline,
      final SdkComponents components,
      boolean useDeprecatedViewTransforms) {
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
    RunnerApi.Pipeline res =
        RunnerApi.Pipeline.newBuilder()
            .setComponents(components.toComponents())
            .addAllRequirements(components.requirements())
            .addAllRootTransformIds(rootIds)
            .build();
    if (!useDeprecatedViewTransforms) {
      // TODO(JIRA-5649): Don't even emit these transforms in the generated protos.
      res = elideDeprecatedViews(res);
    }
    // Validate that translation didn't produce an invalid pipeline.
    PipelineValidator.validate(res);
    return res;
  }

  private static RunnerApi.Pipeline elideDeprecatedViews(RunnerApi.Pipeline pipeline) {
    // Record data on CreateView operations.
    Set<String> viewTransforms = new HashSet<>();
    Map<String, String> viewOutputsToInputs = new HashMap<>();
    pipeline
        .getComponents()
        .getTransformsMap()
        .forEach(
            (transformId, transform) -> {
              if (transform
                  .getSpec()
                  .getUrn()
                  .equals(PTransformTranslation.CREATE_VIEW_TRANSFORM_URN)) {
                viewTransforms.add(transformId);
                viewOutputsToInputs.put(
                    Iterables.getOnlyElement(transform.getOutputsMap().values()),
                    Iterables.getOnlyElement(transform.getInputsMap().values()));
              }
            });
    // Fix up view references.
    Map<String, RunnerApi.PTransform> newTransforms = new HashMap<>();
    pipeline
        .getComponents()
        .getTransformsMap()
        .forEach(
            (transformId, transform) -> {
              RunnerApi.PTransform.Builder transformBuilder = transform.toBuilder();
              transform
                  .getInputsMap()
                  .forEach(
                      (key, value) -> {
                        if (viewOutputsToInputs.containsKey(value)) {
                          transformBuilder.putInputs(key, viewOutputsToInputs.get(value));
                        }
                      });
              transform
                  .getOutputsMap()
                  .forEach(
                      (key, value) -> {
                        if (viewOutputsToInputs.containsKey(value)) {
                          transformBuilder.putOutputs(key, viewOutputsToInputs.get(value));
                        }
                      });
              // Unfortunately transformBuilder.getSubtransformsList().removeAll(viewTransforms)
              // throws UnsupportedOperationException.
              transformBuilder.clearSubtransforms();
              transformBuilder.addAllSubtransforms(
                  transform.getSubtransformsList().stream()
                      .filter(id -> !viewTransforms.contains(id))
                      .collect(Collectors.toList()));
              newTransforms.put(transformId, transformBuilder.build());
            });

    RunnerApi.Pipeline.Builder newPipeline = pipeline.toBuilder();
    // Replace transforms.
    newPipeline.getComponentsBuilder().putAllTransforms(newTransforms);
    // Remove CreateView operation components.
    viewTransforms.forEach(newPipeline.getComponentsBuilder()::removeTransforms);
    viewOutputsToInputs.keySet().forEach(newPipeline.getComponentsBuilder()::removePcollections);
    newPipeline.clearRootTransformIds();
    newPipeline.addAllRootTransformIds(
        pipeline.getRootTransformIdsList().stream()
            .filter(id -> !viewTransforms.contains(id))
            .collect(Collectors.toList()));
    return newPipeline.build();
  }
}
