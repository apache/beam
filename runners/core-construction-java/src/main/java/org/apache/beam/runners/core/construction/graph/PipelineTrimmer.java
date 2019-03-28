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

import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO(BEAM-6327): Remove the need for this.

/** PipelineTrimmer removes subcomponents of native transforms that shouldn't be fused. */
public class PipelineTrimmer {
  private static final Logger LOG = LoggerFactory.getLogger(PipelineTrimmer.class);

  /**
   * Remove subcomponents of native transforms that shouldn't be fused.
   *
   * @param pipeline the pipeline to be trimmed
   * @param knownUrns set of URNs for the runner's native transforms
   * @return the trimmed pipeline
   */
  public static Pipeline trim(Pipeline pipeline, Set<String> knownUrns) {
    return makeKnownUrnsPrimitives(pipeline, knownUrns);
  }

  private static RunnerApi.Pipeline makeKnownUrnsPrimitives(
      RunnerApi.Pipeline pipeline, Set<String> knownUrns) {
    RunnerApi.Pipeline.Builder trimmedPipeline = pipeline.toBuilder();
    for (String ptransformId : pipeline.getComponents().getTransformsMap().keySet()) {
      if (knownUrns.contains(
          pipeline.getComponents().getTransformsOrThrow(ptransformId).getSpec().getUrn())) {
        LOG.debug("Removing descendants of known PTransform {}" + ptransformId);
        removeDescendants(trimmedPipeline, ptransformId);
      }
    }
    return trimmedPipeline.build();
  }

  private static void removeDescendants(RunnerApi.Pipeline.Builder pipeline, String parentId) {
    RunnerApi.PTransform parentProto =
        pipeline.getComponents().getTransformsOrDefault(parentId, null);
    if (parentProto != null) {
      for (String childId : parentProto.getSubtransformsList()) {
        removeDescendants(pipeline, childId);
        pipeline.getComponentsBuilder().removeTransforms(childId);
      }
      pipeline
          .getComponentsBuilder()
          .putTransforms(parentId, parentProto.toBuilder().clearSubtransforms().build());
    }
  }
}
