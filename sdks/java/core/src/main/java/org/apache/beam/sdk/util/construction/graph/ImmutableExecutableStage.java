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

import com.google.auto.value.AutoValue;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.ExecutableStagePayload.WireCoderSetting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

/** An {@link ExecutableStage} which is constructed with all of its initial state. */
@AutoValue
public abstract class ImmutableExecutableStage implements ExecutableStage {
  public static ImmutableExecutableStage ofFullComponents(
      Components components,
      Environment environment,
      PipelineNode.PCollectionNode input,
      Collection<SideInputReference> sideInputs,
      Collection<UserStateReference> userStates,
      Collection<TimerReference> timers,
      Collection<PipelineNode.PTransformNode> transforms,
      Collection<PipelineNode.PCollectionNode> outputs,
      Collection<WireCoderSetting> wireCoderSettings) {
    Components prunedComponents =
        components
            .toBuilder()
            .clearTransforms()
            .putAllTransforms(
                transforms.stream()
                    .collect(
                        Collectors.toMap(
                            PipelineNode.PTransformNode::getId,
                            PipelineNode.PTransformNode::getTransform)))
            .build();
    return of(
        prunedComponents,
        environment,
        input,
        sideInputs,
        userStates,
        timers,
        transforms,
        outputs,
        wireCoderSettings);
  }

  public static ImmutableExecutableStage of(
      Components components,
      Environment environment,
      PipelineNode.PCollectionNode input,
      Collection<SideInputReference> sideInputs,
      Collection<UserStateReference> userStates,
      Collection<TimerReference> timers,
      Collection<PipelineNode.PTransformNode> transforms,
      Collection<PipelineNode.PCollectionNode> outputs,
      Collection<WireCoderSetting> wireCoderSettings) {
    return new AutoValue_ImmutableExecutableStage(
        components,
        environment,
        input,
        ImmutableSet.copyOf(sideInputs),
        ImmutableSet.copyOf(userStates),
        ImmutableSet.copyOf(timers),
        ImmutableSet.copyOf(transforms),
        ImmutableSet.copyOf(outputs),
        wireCoderSettings);
  }

  @Override
  public abstract Components getComponents();

  // Redefine the methods to have a known order.
  @Override
  public abstract Environment getEnvironment();

  @Override
  public abstract PipelineNode.PCollectionNode getInputPCollection();

  @Override
  public abstract Collection<SideInputReference> getSideInputs();

  @Override
  public abstract Collection<UserStateReference> getUserStates();

  @Override
  public abstract Collection<TimerReference> getTimers();

  @Override
  public abstract Collection<PipelineNode.PTransformNode> getTransforms();

  @Override
  public abstract Collection<PipelineNode.PCollectionNode> getOutputPCollections();

  @Override
  public abstract Collection<WireCoderSetting> getWireCoderSettings();
}
