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
package org.apache.beam.sdk.util.construction.graph;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.ComponentsOrBuilder;
import org.apache.beam.model.pipeline.v1.RunnerApi.MessageWithComponents;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A way to apply a Proto-based {@link PTransformOverride}.
 *
 * <p>This should generally be used by runners to replace transforms within graphs. SDK construction
 * code should rely on the more powerful and native {@link
 * org.apache.beam.sdk.Pipeline#replaceAll(List)}.
 */
public class ProtoOverrides {
  /**
   * Update all composites present in the {@code originalPipeline} with an URN equal to the provided
   * {@code urn} using the provided {@link TransformReplacement}.
   */
  public static Pipeline updateTransform(
      String urn, Pipeline originalPipeline, TransformReplacement compositeBuilder) {
    Components.Builder resultComponents = originalPipeline.getComponents().toBuilder();
    for (Map.Entry<String, PTransform> pt :
        originalPipeline.getComponents().getTransformsMap().entrySet()) {
      if (pt.getValue().getSpec() != null && urn.equals(pt.getValue().getSpec().getUrn())) {
        MessageWithComponents updated =
            compositeBuilder.getReplacement(pt.getKey(), originalPipeline.getComponents());
        if (updated == null) {
          continue;
        }

        checkArgument(
            updated.getPtransform().getOutputsMap().equals(pt.getValue().getOutputsMap()),
            "A %s must produce all of the outputs of the original %s",
            TransformReplacement.class.getSimpleName(),
            PTransform.class.getSimpleName());
        removeSubtransforms(pt.getValue(), resultComponents);
        resultComponents
            .mergeFrom(updated.getComponents())
            .putTransforms(pt.getKey(), updated.getPtransform());
      }
    }
    return originalPipeline.toBuilder().setComponents(resultComponents).build();
  }

  /**
   * Remove all sub-transforms of the provided transform recursively. A {@link PTransform} can be
   * the sub-transform of only one enclosing transform.
   */
  private static void removeSubtransforms(PTransform pt, Components.Builder target) {
    for (String subtransformId : pt.getSubtransformsList()) {
      PTransform subtransform = target.getTransformsOrThrow(subtransformId);
      removeSubtransforms(subtransform, target);
      target.removeTransforms(subtransformId);
      // TODO: remove PCollections not produced by 'pt' here.
    }
  }

  /**
   * A Function that takes a transform and the existing components and returns the new composite
   * PTransform and additional components.
   */
  @FunctionalInterface
  public interface TransformReplacement {
    /**
     * Returns the updated composite structure for the provided {@link PTransform}.
     *
     * <p>If the return is null, then no replacement is performed, otherwise the returned {@link
     * MessageWithComponents} must contain a single {@link PTransform}. The result {@link
     * Components} will be merged into the existing components, and the result {@link PTransform}
     * will be set as a replacement of the original {@link PTransform}. Notably, this does not
     * require that the {@code existingComponents} are present in the returned {@link
     * MessageWithComponents}.
     *
     * <p>Introduced components must not collide with any components in the existing components.
     */
    @Nullable
    MessageWithComponents getReplacement(
        String transformId, ComponentsOrBuilder existingComponents);
  }
}
