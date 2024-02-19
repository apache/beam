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

import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TrivialNativeTransformExpander is used to replace transforms with known URNs with their native
 * equivalent.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class TrivialNativeTransformExpander {
  private static final Logger LOG = LoggerFactory.getLogger(TrivialNativeTransformExpander.class);

  /**
   * Replaces transforms with the known URN with a native equivalent stripping the environment and
   * removing any sub-transforms from the returned pipeline.
   *
   * @param pipeline the pipeline to be trimmed
   * @param knownUrns set of URNs for the runner's native transforms
   * @return the trimmed pipeline
   */
  public static Pipeline forKnownUrns(Pipeline pipeline, Set<String> knownUrns) {
    return makeKnownUrnsPrimitives(pipeline, knownUrns);
  }

  private static RunnerApi.Pipeline makeKnownUrnsPrimitives(
      RunnerApi.Pipeline pipeline, Set<String> knownUrns) {
    RunnerApi.Pipeline.Builder trimmedPipeline = pipeline.toBuilder();
    for (String ptransformId : pipeline.getComponents().getTransformsMap().keySet()) {
      // Skip over previously removed transforms from the original pipeline since we iterate
      // over all transforms from the original pipeline and not the trimmed down version.
      RunnerApi.PTransform currentTransform =
          trimmedPipeline.getComponents().getTransformsOrDefault(ptransformId, null);
      if (currentTransform != null && knownUrns.contains(currentTransform.getSpec().getUrn())) {
        LOG.debug(
            "Removing descendants and environment of known native PTransform {}", ptransformId);
        removeDescendants(trimmedPipeline, ptransformId);
        trimmedPipeline
            .getComponentsBuilder()
            .putTransforms(
                ptransformId,
                currentTransform.toBuilder().clearSubtransforms().clearEnvironmentId().build());
      }
    }
    return trimmedPipeline.build();
  }

  private static void removeDescendants(RunnerApi.Pipeline.Builder pipeline, String parentId) {
    RunnerApi.PTransform parentProto = pipeline.getComponents().getTransformsOrThrow(parentId);
    for (String childId : parentProto.getSubtransformsList()) {
      removeDescendants(pipeline, childId);
      pipeline.getComponentsBuilder().removeTransforms(childId);
    }
  }
}
