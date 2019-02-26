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
package org.apache.beam.runners.direct.portable;

import java.util.Collection;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;

/**
 * Provides {@link CommittedBundle bundles} that will be provided to the {@link PTransform
 * PTransforms} that are at the root of a {@link Pipeline}.
 */
interface RootInputProvider<ShardT> {
  /**
   * Get the initial inputs for the {@link PTransformNode}. The {@link PTransformNode} will be
   * provided with these {@link CommittedBundle bundles} as input when the {@link Pipeline} runs.
   *
   * <p>For source transforms, these should be sufficient that, when provided to the evaluators
   * produced by {@link TransformEvaluatorFactory#forApplication(PTransformNode, CommittedBundle)},
   * all of the elements contained in the source are eventually produced.
   *
   * @param transform the {@link PTransformNode} to get initial inputs for.
   * @param targetParallelism the target amount of parallelism to obtain from the source. Must be
   *     greater than or equal to 1.
   */
  Collection<CommittedBundle<ShardT>> getInitialInputs(
      PTransformNode transform, int targetParallelism) throws Exception;
}
