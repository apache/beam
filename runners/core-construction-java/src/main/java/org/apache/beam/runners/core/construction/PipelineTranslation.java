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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.graph.PipelineValidator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;

/** Utilities for going to/from Runner API pipelines. */
public class PipelineTranslation {

  public static RunnerApi.Pipeline toProto(Pipeline pipeline) {
    return toProto(pipeline, SdkComponents.create(pipeline.getOptions()));
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
    RunnerApi.Pipeline res =
        RunnerApi.Pipeline.newBuilder()
            .setComponents(components.toComponents())
            .addAllRootTransformIds(rootIds)
            .build();
    // Validate that translation didn't produce an invalid pipeline.
    PipelineValidator.validate(res);
    return res;
  }
}
