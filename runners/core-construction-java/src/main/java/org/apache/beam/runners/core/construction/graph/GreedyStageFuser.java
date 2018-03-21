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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayDeque;
import java.util.LinkedHashSet;
import java.util.Queue;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory class which produces an {@link ExecutableStage} by attempting to fuse all available
 * {@link PCollectionNode PCollections} when it is constructed.
 *
 * <p>A {@link PCollectionNode} is fused into a stage if all of its consumers can be fused into the
 * stage. A consumer can be fused into a stage if it is executed within the environment of that
 * {@link ExecutableStage}, and receives only per-element inputs. PTransforms which consume side
 * inputs are always at the root of a stage.
 *
 * <p>A {@link PCollectionNode} with consumers that execute in an environment other than a stage is
 * materialized, and its consumers execute in independent stages.
 */
public class GreedyStageFuser {
  // TODO: Provide a way to merge in a compatible subgraph (e.g. one where all of the siblings
  // consume a PCollection materialized by this subgraph and can be fused into it).
  private static final Logger LOG = LoggerFactory.getLogger(GreedyStageFuser.class);

  private GreedyStageFuser() {
    // A Factory class, do not instantiate
  }

  /**
   * Returns an {@link ExecutableStage} where the initial {@link PTransformNode PTransform} is a
   * Remote gRPC Port Read, reading elements from the materialized {@link PCollectionNode
   * PCollection}.
   *
   * @param initialNodes the initial set of sibling transforms to fuse into this node. All of the
   *     transforms must consume the {@code inputPCollection} on a per-element basis, and must all
   *     be mutually compatible.
   */
  public static ExecutableStage forGrpcPortRead(
      QueryablePipeline pipeline,
      PCollectionNode inputPCollection,
      Set<PTransformNode> initialNodes) {
    checkArgument(
        !initialNodes.isEmpty(),
        "%s must contain at least one %s.",
        GreedyStageFuser.class.getSimpleName(),
        PTransformNode.class.getSimpleName());
    // Choose the environment from an arbitrary node. The initial nodes may not be empty for this
    // subgraph to make any sense, there has to be at least one processor node
    // (otherwise the stage is gRPC Read -> gRPC Write, which doesn't do anything).
    Environment environment = getStageEnvironment(pipeline, initialNodes);

    ImmutableSet.Builder<PTransformNode> fusedTransforms = ImmutableSet.builder();
    fusedTransforms.addAll(initialNodes);

    Set<PCollectionNode> sideInputs = new LinkedHashSet<>();
    Set<PCollectionNode> fusedCollections = new LinkedHashSet<>();
    Set<PCollectionNode> materializedPCollections = new LinkedHashSet<>();

    Queue<PCollectionNode> fusionCandidates = new ArrayDeque<>();
    for (PTransformNode initialConsumer : initialNodes) {
      fusionCandidates.addAll(pipeline.getOutputPCollections(initialConsumer));
      sideInputs.addAll(pipeline.getSideInputs(initialConsumer));
    }
    while (!fusionCandidates.isEmpty()) {
      PCollectionNode candidate = fusionCandidates.poll();
      if (fusedCollections.contains(candidate) || materializedPCollections.contains(candidate)) {
        // This should generally mean we get to a Flatten via multiple paths through the graph and
        // we've already determined what to do with the output.
        LOG.debug(
            "Skipping fusion candidate {} because it is {} in this {}",
            candidate,
            fusedCollections.contains(candidate) ? "fused" : "materialized",
            ExecutableStage.class.getSimpleName());
        continue;
      }
      PCollectionFusibility fusibility =
          canFuse(pipeline, candidate, environment, fusedCollections);
      switch (fusibility) {
        case MATERIALIZE:
          materializedPCollections.add(candidate);
          break;
        case FUSE:
          // All of the consumers of the candidate PCollection can be fused into this stage. Do so.
          fusedCollections.add(candidate);
          fusedTransforms.addAll(pipeline.getPerElementConsumers(candidate));
          for (PTransformNode consumer : pipeline.getPerElementConsumers(candidate)) {
            // The outputs of every transform fused into this stage must be either materialized or
            // themselves fused away, so add them to the set of candidates.
            fusionCandidates.addAll(pipeline.getOutputPCollections(consumer));
            sideInputs.addAll(pipeline.getSideInputs(consumer));
          }
          break;
        default:
          throw new IllegalStateException(
              String.format(
                  "Unknown type of %s %s",
                  PCollectionFusibility.class.getSimpleName(), fusibility));
      }
    }

    return ImmutableExecutableStage.of(
        environment,
        inputPCollection,
        sideInputs,
        fusedTransforms.build(),
        materializedPCollections);
  }

  private static Environment getStageEnvironment(
      QueryablePipeline pipeline, Set<PTransformNode> initialNodes) {
    Supplier<IllegalArgumentException> missingEnv =
        () ->
            new IllegalArgumentException(
                String.format(
                    "%s must be populated on all %s in a %s",
                    Environment.class.getSimpleName(),
                    PTransformNode.class.getSimpleName(),
                    GreedyStageFuser.class.getSimpleName()));
    Environment env =
        pipeline.getEnvironment(initialNodes.iterator().next()).orElseThrow(missingEnv);
    initialNodes.forEach(
        transformNode ->
            checkArgument(
                env.equals(pipeline.getEnvironment(transformNode).orElseThrow(missingEnv)),
                "All %s in a %s must be the same. Got %s and %s",
                Environment.class.getSimpleName(),
                ExecutableStage.class.getSimpleName(),
                env,
                pipeline.getEnvironment(transformNode).get() /* will throw above if absent. */));
    return env;
  }

  private static PCollectionFusibility canFuse(
      QueryablePipeline pipeline,
      PCollectionNode candidate,
      Environment environment,
      Set<PCollectionNode> fusedPCollections) {
    for (PTransformNode node : pipeline.getPerElementConsumers(candidate)) {
      if (!(GreedyPCollectionFusers.canFuse(node, environment, fusedPCollections, pipeline))) {
        // Some of the consumers can't be fused into this subgraph, so the PCollection has to be
        // materialized.
        // TODO: Potentially, some of the consumers can be fused back into this stage later
        // complicate the process, but at a high level, if a downstream stage can be fused into all
        // of the stages that produce a PCollection it can be fused into all of those stages.
        return PCollectionFusibility.MATERIALIZE;
      }
    }
    return PCollectionFusibility.FUSE;
  }

  private enum PCollectionFusibility {
    MATERIALIZE,
    FUSE,
  }
}
