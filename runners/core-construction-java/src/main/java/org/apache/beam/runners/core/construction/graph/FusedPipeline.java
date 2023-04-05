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

import com.google.auto.value.AutoValue;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.SyntheticComponents;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;

/** A {@link Pipeline} which has been separated into collections of executable components. */
@AutoValue
public abstract class FusedPipeline {
  static FusedPipeline of(
      Components components,
      Set<ExecutableStage> environmentalStages,
      Set<PTransformNode> runnerStages,
      Set<String> requirements) {
    return new AutoValue_FusedPipeline(components, environmentalStages, runnerStages, requirements);
  }

  abstract Components getComponents();

  /** The {@link ExecutableStage executable stages} that are executed by SDK harnesses. */
  public abstract Set<ExecutableStage> getFusedStages();

  /** The {@link PTransform PTransforms} that a runner is responsible for executing. */
  public abstract Set<PTransformNode> getRunnerExecutedTransforms();

  /** The {@link PTransform PTransforms} that a runner is responsible for executing. */
  public abstract Set<String> getRequirements();

  /**
   * Returns the {@link RunnerApi.Pipeline} representation of this {@link FusedPipeline}.
   *
   * <p>The {@link Components} of the returned pipeline will contain all of the {@link PTransform
   * PTransforms} present in the original Pipeline that this {@link FusedPipeline} was created from,
   * plus all of the {@link ExecutableStage ExecutableStages} contained within this {@link
   * FusedPipeline}. The {@link Pipeline#getRootTransformIdsList()} will contain all of the runner
   * executed transforms and all of the {@link ExecutableStage execuable stages} contained within
   * the Pipeline.
   */
  public RunnerApi.Pipeline toPipeline() {
    Map<String, PTransform> executableStageTransforms = getEnvironmentExecutedTransforms();
    Set<String> executableTransformIds =
        Sets.union(
            executableStageTransforms.keySet(),
            getRunnerExecutedTransforms().stream()
                .map(PTransformNode::getId)
                .collect(Collectors.toSet()));

    // Augment the initial transforms with all of the executable transforms.
    Components fusedComponents =
        getComponents().toBuilder().putAllTransforms(executableStageTransforms).build();
    List<String> rootTransformIds =
        StreamSupport.stream(
                QueryablePipeline.forTransforms(executableTransformIds, fusedComponents)
                    .getTopologicallyOrderedTransforms()
                    .spliterator(),
                false)
            .map(PTransformNode::getId)
            .collect(Collectors.toList());
    Pipeline res =
        Pipeline.newBuilder()
            .setComponents(fusedComponents)
            .addAllRootTransformIds(rootTransformIds)
            .addAllRequirements(getRequirements())
            .build();
    // Validate that fusion didn't produce a malformed pipeline.
    PipelineValidator.validate(res);
    return res;
  }

  /**
   * Return a map of IDs to {@link PTransform} which are executed by an SDK Harness.
   *
   * <p>The transforms that are present in the returned map are the {@link RunnerApi.PTransform}
   * versions of the {@link ExecutableStage ExecutableStages} returned in {@link #getFusedStages()}.
   * The IDs of the returned transforms will not collide with any transform ID present in {@link
   * #getComponents()}.
   */
  private Map<String, PTransform> getEnvironmentExecutedTransforms() {
    Map<String, PTransform> topLevelTransforms = new HashMap<>();
    for (ExecutableStage stage : getFusedStages()) {
      String baseName =
          String.format(
              "%s/%s",
              stage.getInputPCollection().getPCollection().getUniqueName(),
              stage.getEnvironment().getUrn());
      Set<String> usedNames =
          Sets.union(topLevelTransforms.keySet(), getComponents().getTransformsMap().keySet());
      String uniqueId = SyntheticComponents.uniqueId(baseName, usedNames::contains);
      topLevelTransforms.put(uniqueId, stage.toPTransform(uniqueId));
    }
    return topLevelTransforms;
  }
}
