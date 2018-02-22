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
import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;

/** An {@link ExecutableStage} which is constructed with all of its initial state. */
@AutoValue
abstract class ImmutableExecutableStage implements ExecutableStage {
  static ImmutableExecutableStage of(
      Environment environment,
      PCollectionNode input,
      Collection<PTransformNode> transforms,
      Collection<PCollectionNode> outputs) {
    return new AutoValue_ImmutableExecutableStage(
        environment, input, ImmutableSet.copyOf(transforms), ImmutableSet.copyOf(outputs));
  }

  // Redefine the methods to have a known order.
  @Override
  public abstract Environment getEnvironment();

  @Override
  public abstract PCollectionNode getInputPCollection();

  @Override
  public abstract Collection<PTransformNode> getTransforms();

  @Override
  public abstract Collection<PCollectionNode> getOutputPCollections();
}
