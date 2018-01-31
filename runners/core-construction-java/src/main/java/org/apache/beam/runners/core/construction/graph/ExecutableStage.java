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

import java.util.Collection;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;

/**
 * A combination of PTransforms that can be executed within a single SDK harness.
 *
 * <p>Contains only the nodes that specify the processing to perform within the SDK harness, and
 * does not contain any runner-executed nodes.
 *
 * <p>Within a single {@link Pipeline}, {@link PTransform PTransforms} and {@link PCollection
 * PCollections} are permitted to appear in multiple executable stages. However, paths from a root
 * {@link PTransform} to any other {@link PTransform} within that set of stages must be unique.
 */
public interface ExecutableStage {
  /**
   * The URN identifying an {@link ExecutableStage} that has been converted to a {@link PTransform}.
   */
  String URN = "beam:runner:executable_stage:v1";

  /**
   * Returns the {@link Environment} this stage executes in.
   *
   * <p>An {@link ExecutableStage} consists of {@link PTransform PTransforms} which can all be
   * executed within a single {@link Environment}. The assumption made here is that
   * runner-implemented transforms will be associated with these subgraphs by the overall graph
   * topology, which will be handled by runners by performing already-required element routing and
   * runner-side processing.
   */
  Environment getEnvironment();

  /**
   * Returns the root {@link PCollectionNode} of this {@link ExecutableStage}. This
   * {@link ExecutableStage} executes by reading elements from a Remote gRPC Read Node.
   */
  PCollectionNode getInputPCollection();

  /**
   * Returns the leaf {@link PCollectionNode PCollections} of this {@link ExecutableStage}.
   *
   * <p>All of these {@link PCollectionNode PCollections} are consumed by a {@link PTransformNode
   * PTransform} which is not contained within this executable stage, and must be materialized at
   * execution time by a Remote gRPC Write Transform.
   */
  Collection<PCollectionNode> getOutputPCollections();

  Collection<PTransformNode> getTransforms();

  /**
   * Returns a composite {@link PTransform} which contains all of the {@link PTransform PTransforms}
   * fused into this {@link ExecutableStage} as {@link PTransform#getSubtransformsList()
   * subtransforms} .
   *
   * <p>The input {@link PCollection} for the returned {@link PTransform} will be the consumed
   * {@link PCollectionNode} returned by {@link #getInputPCollection()} and the output {@link
   * PCollection PCollections} will be the {@link PCollectionNode PCollections} returned by {@link
   * #getOutputPCollections()}.
   */
  PTransform toPTransform();
}
