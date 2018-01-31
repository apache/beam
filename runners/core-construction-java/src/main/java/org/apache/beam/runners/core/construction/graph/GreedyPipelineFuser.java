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
import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Fuses a {@link Pipeline} into some set of single-environment executable transforms. */
public class GreedyPipelineFuser {
  private static final Logger LOG = LoggerFactory.getLogger(GreedyPipelineFuser.class);

  private final QueryablePipeline pipeline;
  private final Map<CollectionConsumer, ExecutableStage> consumedCollectionsAndTransforms =
      new HashMap<>();
  private final Set<PTransformNode> unfusedTransforms = new HashSet<>();
  private final Set<ExecutableStage> stages = new HashSet<>();

  private GreedyPipelineFuser(Pipeline p) {
    this.pipeline = QueryablePipeline.fromComponents(p.getComponents());
    Collection<Set<CollectionConsumer>> rootStages = new ArrayList<>();
    for (PTransformNode pTransformNode : pipeline.getRootTransforms()) {
      // This will usually be a single node, either downstream of an Impulse or a Read node
      Collection<Set<CollectionConsumer>> rootNodeStageRoots = getRootEnvTransforms(pTransformNode);
      rootStages.addAll(rootNodeStageRoots);
    }
    fusePipeline(rootStages);
  }

  public static FusedPipeline fuse(Pipeline p) {
    GreedyPipelineFuser fuser = new GreedyPipelineFuser(p);
    return FusedPipeline.of(fuser.stages, fuser.unfusedTransforms);
  }

  private void fusePipeline(Collection<Set<CollectionConsumer>> initialConsumers) {
    Queue<Set<CollectionConsumer>> siblingSets = new ArrayDeque<>();
    siblingSets.addAll(initialConsumers);
    while (!siblingSets.isEmpty()) {
      // Only introduce new PCollection consumers. Not performing this introduces potential
      // duplicate paths through the pipeline.
      Set<CollectionConsumer> sibs = siblingSets.poll();
      Set<CollectionConsumer> siblingSet =
          sibs.stream()
              .filter(
                  collectionConsumer ->
                      !consumedCollectionsAndTransforms.containsKey(collectionConsumer))
              .collect(Collectors.toSet());
      checkState(
          siblingSet.equals(sibs) || siblingSet.isEmpty(),
          "Inconsistent collection of siblings reported for a %s. Initial attempt missed %s",
          PCollectionNode.class.getSimpleName(),
          Sets.difference(sibs, siblingSet));
      if (siblingSet.isEmpty()) {
        LOG.debug("Filtered out duplicate stage root {}", sibs);
        continue;
      }
      ExecutableStage stage = fuseSiblings(siblingSet);
      for (CollectionConsumer sibling : siblingSet) {
        ExecutableStage oldStage = consumedCollectionsAndTransforms.put(sibling, stage);
        // This should never happen; we filter out all of the existing consumers
        checkState(
            oldStage == null,
            "Multiple %s registered for %s %s: %s and %s",
            ExecutableStage.class.getSimpleName(),
            CollectionConsumer.class.getSimpleName(),
            sibling,
            stage,
            oldStage);
      }
      stages.add(stage);
      for (PCollectionNode materializedOutput : stage.getOutputPCollections()) {
        Set<CollectionConsumer> materializedConsumers =
            getDownstreamInEnvConsumers(materializedOutput);
        Collection<Set<CollectionConsumer>> siblings = groupSiblings(materializedConsumers);
        siblingSets.addAll(siblings);
      }
    }
    // TODO: Stages can be fused with each other, if doing so does not introduce duplicate paths
    // for an element to take through the Pipeline. Compatible siblings can generally be fused,
    // as can compatible producers/consumers if a PCollection is only materialized once.
  }

  private Collection<Set<CollectionConsumer>> getRootEnvTransforms(PTransformNode rootNode) {
    checkArgument(
        rootNode.getTransform().getInputsCount() == 0,
        "%s is not at the root of the graph (consumes %s)",
        PTransformNode.class.getSimpleName(),
        rootNode.getTransform().getInputsMap());
    if (pipeline.getEnvironment(rootNode).isPresent()) {
      throw new IllegalArgumentException(
          String.format(
              "%s requires all root nodes to be runner-implemented %s primitives",
              GreedyPipelineFuser.class.getSimpleName(),
              PTransformTranslation.IMPULSE_TRANSFORM_URN));
    } else {
      unfusedTransforms.add(rootNode);
      Set<CollectionConsumer> environmentNodes = new HashSet<>();
      // Walk down until the first environments are found, and fuse them as appropriate.
      for (PCollectionNode output : pipeline.getOutputPCollections(rootNode)) {
        environmentNodes.addAll(getDownstreamInEnvConsumers(output));
      }
      return groupSiblings(environmentNodes);
    }
  }

  private Set<CollectionConsumer> getDownstreamInEnvConsumers(PCollectionNode inputPCollection) {
    Set<CollectionConsumer> downstreamConsumers = new HashSet<>();
    for (PTransformNode consumer : pipeline.getPerElementConsumers(inputPCollection)) {
      if (pipeline.getEnvironment(consumer).isPresent()) {
        downstreamConsumers.add(CollectionConsumer.of(inputPCollection, consumer));
      } else {
        LOG.debug(
            "Adding {} {} to the set of runner-executed transforms",
            PTransformNode.class.getSimpleName(),
            consumer.getId());
        unfusedTransforms.add(consumer);
        for (PCollectionNode output : pipeline.getOutputPCollections(consumer)) {
          downstreamConsumers.addAll(getDownstreamInEnvConsumers(output));
        }
      }
    }
    return downstreamConsumers;
  }

  // Aww.
  private Collection<Set<CollectionConsumer>> groupSiblings(Set<CollectionConsumer> newConsumers) {
    // This is just a way to make sure we don't go over nodes that are not fusible by construction
    Multimap<PCollectionNode, Set<CollectionConsumer>> compatibleConsumers =
        ArrayListMultimap.create();
    // This is O(N**2) with the number of siblings we consider, which is generally the number of
    // parallel consumers of a PCollection. This usually is unlikely to be high,
    // but it is pretty significant slowdown. TODO: Easy updates, like add environments here?
    for (CollectionConsumer newConsumer : newConsumers) {
      boolean foundSet = false;
      for (Set<CollectionConsumer> existingConsumers :
          compatibleConsumers.get(newConsumer.consumedCollection())) {
        if (existingConsumers
            .stream()
            .allMatch(
                // The two consume the same PCollection and can exist in the same stage.
                collectionConsumer ->
                    GreedyPCollectionFusers.isCompatible(
                        collectionConsumer.consumingTransform(),
                        newConsumer.consumingTransform(),
                        pipeline))) {
          existingConsumers.add(newConsumer);
          foundSet = true;
          break;
        }
      }
      if (!foundSet) {
        Set<CollectionConsumer> newConsumerSet = new HashSet<>();
        newConsumerSet.add(newConsumer);
        compatibleConsumers.put(newConsumer.consumedCollection(), newConsumerSet);
      }
    }
    return compatibleConsumers.values();
  }

  private ExecutableStage fuseSiblings(Set<CollectionConsumer> mutuallyCompatible) {
    PCollectionNode rootCollection = mutuallyCompatible.iterator().next().consumedCollection();
    return GreedilyFusedExecutableStage.forGrpcPortRead(
        pipeline,
        rootCollection,
        mutuallyCompatible
            .stream()
            .map(CollectionConsumer::consumingTransform)
            .collect(Collectors.toSet()));
  }

  @AutoValue
  abstract static class CollectionConsumer {
    static CollectionConsumer of(PCollectionNode collection, PTransformNode consumer) {
      return new AutoValue_GreedyPipelineFuser_CollectionConsumer(collection, consumer);
    }

    abstract PCollectionNode consumedCollection();

    abstract PTransformNode consumingTransform();
  }

  public static Collection<RunnerApi.PTransform> createFusedStages(Pipeline p) {
    new GreedyPipelineFuser(p);
    throw new UnsupportedOperationException();
  }
}
