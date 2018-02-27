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
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Fuses a {@link Pipeline} into some set of single-environment executable transforms. */
// The use of NavigableSets everywhere provides consistent ordering but may be overkill for this
// cause.
public class GreedyPipelineFuser {
  private static final Logger LOG = LoggerFactory.getLogger(GreedyPipelineFuser.class);

  private final QueryablePipeline pipeline;
  private final Map<CollectionConsumer, ExecutableStage> consumedCollectionsAndTransforms =
      new HashMap<>();
  private final Set<PTransformNode> unfusedTransforms = new LinkedHashSet<>();
  private final Set<ExecutableStage> stages = new LinkedHashSet<>();

  private GreedyPipelineFuser(Pipeline p) {
    this.pipeline = QueryablePipeline.forPrimitivesIn(p.getComponents());
    NavigableSet<CollectionConsumer> rootConsumers = new TreeSet<>();
    for (PTransformNode pTransformNode : pipeline.getRootTransforms()) {
      // This will usually be a single node, the downstream of an Impulse, but may be of any size
      rootConsumers.addAll(getRootEnvTransforms(pTransformNode));
    }
    fusePipeline(groupSiblings(rootConsumers));
  }

  /**
   * Fuses a {@link Pipeline} into a collection of {@link ExecutableStage}s.
   *
   * <p>This fuser expects each ExecutableStage to have exactly one input. This means that pipelines
   * must be rooted at Impulse, or other runner-executed primitive transforms, instead of primitive
   * Read nodes. The utilities in
   * {@link org.apache.beam.runners.core.construction.JavaReadViaImpulse} can be used to translate
   * non-compliant pipelines.
   */
  public static FusedPipeline fuse(Pipeline p) {
    GreedyPipelineFuser fuser = new GreedyPipelineFuser(p);
    return FusedPipeline.of(fuser.stages, fuser.unfusedTransforms);
  }

  /**
   * Fuses a {@link Pipeline} into a collection of {@link ExecutableStage}.
   *
   * <p>The input is the initial collection of siblings sets which will be fused into {@link
   * ExecutableStage stages}. A sibling in this context represents a pair of (PCollection,
   * PTransform), where the PTransform consumes input elements on a per-element basis from the
   * PCollection, represented by a {@link CollectionConsumer}. A sibling set is a collection of
   * siblings which can execute within a single {@link ExecutableStage}, determined by {@link
   * GreedyPCollectionFusers#isCompatible(PTransformNode, PTransformNode, QueryablePipeline)}.
   *
   * <p>While a pending sibling set exists:
   *
   * <ul>
   *   <li>Retrieve a pending sibling set from the front of the queue.
   *   <li>If the pending sibling set has already been created, continue. Each materialized {@link
   *       PTransformNode} can be consumed by any number of {@link ExecutableStage stages}, but each
   *       {@link PTransformNode} may only be present in a single stage rooted at a single {@link
   *       PCollectionNode}, otherwise it will process elements of that {@link PCollectionNode}
   *       multiple times.
   *   <li>Create a {@link GreedyStageFuser} with those siblings as the initial
   *       consuming transforms of the stage
   *   <li>For each materialized {@link PCollectionNode}, find all of the descendant in-environment
   *       consumers. See {@link #getDescendantConsumersInEnv(PCollectionNode)} for details.
   *   <li>Construct all of the sibling sets from the descendant in-environment consumers, and add
   *       them to the queue of sibling sets.
   * </ul>
   */
  private void fusePipeline(NavigableSet<NavigableSet<CollectionConsumer>> initialConsumers) {
    Queue<Set<CollectionConsumer>> pendingSiblingSets = new ArrayDeque<>();
    pendingSiblingSets.addAll(initialConsumers);
    while (!pendingSiblingSets.isEmpty()) {
      // Only introduce new PCollection consumers. Not performing this introduces potential
      // duplicate paths through the pipeline.
      Set<CollectionConsumer> candidateSiblings = pendingSiblingSets.poll();
      Set<CollectionConsumer> siblingSet =
          Sets.difference(candidateSiblings, consumedCollectionsAndTransforms.keySet());
      checkState(
          siblingSet.equals(candidateSiblings) || siblingSet.isEmpty(),
          "Inconsistent collection of siblings reported for a %s. Initial attempt missed %s",
          PCollectionNode.class.getSimpleName(),
          siblingSet);
      if (siblingSet.isEmpty()) {
        LOG.debug("Filtered out duplicate stage root {}", candidateSiblings);
        continue;
      }
      // Create the stage with these siblings as the initial consuming transforms
      ExecutableStage stage = fuseSiblings(siblingSet);
      // Mark each of the root transforms of the stage as consuming the input PCollection, so we
      // don't place them in multiple stages.
      for (CollectionConsumer sibling : siblingSet) {
        consumedCollectionsAndTransforms.put(sibling, stage);
      }
      stages.add(stage);
      for (PCollectionNode materializedOutput : stage.getOutputPCollections()) {
        // Get all of the descendant consumers of each materialized PCollection, and add them to the
        // queue of pending siblings.
        NavigableSet<CollectionConsumer> materializedConsumers =
            getDescendantConsumersInEnv(materializedOutput);
        NavigableSet<NavigableSet<CollectionConsumer>> siblings =
            groupSiblings(materializedConsumers);

        pendingSiblingSets.addAll(siblings);
      }
    }
    // TODO: Stages can be fused with each other, if doing so does not introduce duplicate paths
    // for an element to take through the Pipeline. Compatible siblings can generally be fused,
    // as can compatible producers/consumers if a PCollection is only materialized once.
  }

  private Set<CollectionConsumer> getRootEnvTransforms(
      PTransformNode rootNode) {
    checkArgument(
        rootNode.getTransform().getInputsCount() == 0,
        "%s is not at the root of the graph (consumes %s)",
        PTransformNode.class.getSimpleName(),
        rootNode.getTransform().getInputsMap());
    checkArgument(
        !pipeline.getEnvironment(rootNode).isPresent(),
        "%s requires all root nodes to be runner-implemented %s primitives",
        GreedyPipelineFuser.class.getSimpleName(),
        PTransformTranslation.IMPULSE_TRANSFORM_URN);
    unfusedTransforms.add(rootNode);
    Set<CollectionConsumer> environmentNodes = new HashSet<>();
    // Walk down until the first environments are found, and fuse them as appropriate.
    for (PCollectionNode output : pipeline.getOutputPCollections(rootNode)) {
      environmentNodes.addAll(getDescendantConsumersInEnv(output));
    }
    return environmentNodes;
  }

  /**
   * Retrieve all descendant {@link PTransformNode PTransforms} which are executed within an {@link
   * Environment}, such that there is a path between this input {@link PCollectionNode} and the
   * descendant {@link PTransformNode} with no intermediate {@link PTransformNode} which executes
   * within an environment.
   *
   * <p>This occurs as follows:
   *
   * <ul>
   *   <li>For each consumer of the input {@link PCollectionNode}:
   *       <ul>
   *         <li>If that {@link PTransformNode} executes within an environment, add it to the
   *             collection of descendants
   *         <li>If that {@link PTransformNode} does not execute within an environment, for each
   *             output {@link PCollectionNode} that that {@link PTransformNode} produces, add the
   *             result of recursively applying this method to that {@link PCollectionNode}.
   *       </ul>
   * </ul>
   *
   * <p>As {@link PCollectionNode PCollections} output by a {@link PTransformNode} that executes
   * within an {@link Environment} are not recursively inspected, {@link PTransformNode PTransforms}
   * reachable only via a path including that node as an intermediate node cannot be returned as a
   * descendant consumer of the original {@link PCollectionNode}.
   */
  private NavigableSet<CollectionConsumer> getDescendantConsumersInEnv(
      PCollectionNode inputPCollection) {
    NavigableSet<CollectionConsumer> downstreamConsumers = new TreeSet<>();
    for (PTransformNode consumer : pipeline.getPerElementConsumers(inputPCollection)) {
      if (pipeline.getEnvironment(consumer).isPresent()) {
        // The base case: this descendant consumes elements from
        downstreamConsumers.add(CollectionConsumer.of(inputPCollection, consumer));
      } else {
        LOG.debug(
            "Adding {} {} to the set of runner-executed transforms",
            PTransformNode.class.getSimpleName(),
            consumer.getId());
        unfusedTransforms.add(consumer);
        for (PCollectionNode output : pipeline.getOutputPCollections(consumer)) {
          // Recurse to all of the ouput PCollections of this PTransform.
          downstreamConsumers.addAll(getDescendantConsumersInEnv(output));
        }
      }
    }
    return downstreamConsumers;
  }

  /**
   * The minimum requirement to fuse two {@link CollectionConsumer consumers} as siblings.
   *
   * <p>This is the minimum requirement for {@link PTransformNode transforms} to be siblings.
   * Different {@link PTransformNode transforms} may have additional restrictions.
   */
  @AutoValue
  abstract static class SiblingKey {
    abstract PCollectionNode getInputCollection();
    abstract Environment getEnv();
  }

  /**
   * Produce the set of sets of {@link CollectionConsumer consumers} that can be fused into a single
   * {@link ExecutableStage}. This identifies available siblings for sibling fusion.
   *
   * <p>For each set in the returned collection, each of {@link CollectionConsumer consumers}
   * present consumes from the same {@link PCollection} and is compatible, as determined by {@link
   * GreedyPCollectionFusers#isCompatible(PTransformNode, PTransformNode, QueryablePipeline)}.
   *
   * <p>Each input {@link CollectionConsumer} must have an associated {@link Environment}.
   */
  private NavigableSet<NavigableSet<CollectionConsumer>> groupSiblings(
      NavigableSet<CollectionConsumer>
          newConsumers /* Use a navigable set for consistent iteration order */) {
    Multimap<SiblingKey, NavigableSet<CollectionConsumer>> compatibleConsumers =
        HashMultimap.create();
    // This is O(N**2) with the number of siblings we consider, which is generally the number of
    // parallel consumers of a PCollection. This usually is unlikely to be high,
    // but has potential to be a pretty significant slowdown.
    for (CollectionConsumer newConsumer : newConsumers) {
      SiblingKey key =
          new AutoValue_GreedyPipelineFuser_SiblingKey(
              newConsumer.consumedCollection(),
              pipeline.getEnvironment(newConsumer.consumingTransform()).get());
      boolean foundSiblings = false;
      for (Set<CollectionConsumer> existingConsumers : compatibleConsumers.get(key)) {
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
          foundSiblings = true;
          break;
        }
      }
      if (!foundSiblings) {
        NavigableSet<CollectionConsumer> newConsumerSet = new TreeSet<>();
        newConsumerSet.add(newConsumer);
        compatibleConsumers.put(key, newConsumerSet);
      }
    }
    // Order sibling sets by their least siblings. This is stable across the order siblings are
    // generated, given stable IDs.
    NavigableSet<NavigableSet<CollectionConsumer>> orderedSiblings =
        new TreeSet<>(Comparator.comparing(SortedSet::first));
    orderedSiblings.addAll(compatibleConsumers.values());
    return orderedSiblings;
  }

  private ExecutableStage fuseSiblings(Set<CollectionConsumer> mutuallyCompatible) {
    PCollectionNode rootCollection = mutuallyCompatible.iterator().next().consumedCollection();
    return GreedyStageFuser.forGrpcPortRead(
        pipeline,
        rootCollection,
        mutuallyCompatible
            .stream()
            .map(CollectionConsumer::consumingTransform)
            .collect(Collectors.toSet()));
  }

  /**
   * A ({@link PCollectionNode}, {@link PTransformNode}) pair representing a single {@link
   * PTransformNode} consuming a single materialized {@link PCollectionNode}.
   *
   * <p>For convenience, {@link CollectionConsumer} implements {@link Comparable}. The natural
   * ordering of {@link CollectionConsumer} is first by the ID of the {@link #consumedCollection()},
   * then by the ID of the {@link #consumingTransform()}.
   */
  @AutoValue
  abstract static class CollectionConsumer implements Comparable<CollectionConsumer> {
    static CollectionConsumer of(PCollectionNode collection, PTransformNode consumer) {
      return new AutoValue_GreedyPipelineFuser_CollectionConsumer(collection, consumer);
    }

    abstract PCollectionNode consumedCollection();

    abstract PTransformNode consumingTransform();

    /**
     * {@inheritDoc}.
     *
     * <p>The natural ordering of {@link CollectionConsumer} is first by the ID of the {@link
     * #consumedCollection()}, then by the ID of the {@link #consumingTransform()}.
     */
    @Override
    public int compareTo(CollectionConsumer that) {
      return ComparisonChain.start()
          .compare(this.consumedCollection().getId(), that.consumedCollection().getId())
          .compare(this.consumingTransform().getId(), that.consumingTransform().getId())
          .result();
    }
  }
}
