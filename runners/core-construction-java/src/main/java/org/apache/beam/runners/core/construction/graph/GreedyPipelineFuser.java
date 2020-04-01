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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.graph.OutputDeduplicator.DeduplicationResult;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ComparisonChain;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Fuses a {@link Pipeline} into some set of single-environment executable transforms. */
// The use of NavigableSets everywhere provides consistent ordering but may be overkill for this
// cause.
public class GreedyPipelineFuser {
  private static final Logger LOG = LoggerFactory.getLogger(GreedyPipelineFuser.class);

  private final QueryablePipeline pipeline;
  private final FusedPipeline fusedPipeline;

  private GreedyPipelineFuser(Pipeline p) {
    // Validate that the original pipeline is well-formed.
    PipelineValidator.validate(p);
    this.pipeline = QueryablePipeline.forPrimitivesIn(p.getComponents());
    Set<PTransformNode> unfusedRootNodes = new LinkedHashSet<>();
    NavigableSet<CollectionConsumer> rootConsumers = new TreeSet<>();
    for (PTransformNode pTransformNode : pipeline.getRootTransforms()) {
      // This will usually be a single node, the downstream of an Impulse, but may be of any size
      DescendantConsumers descendants = getRootConsumers(pTransformNode);
      unfusedRootNodes.addAll(descendants.getUnfusedNodes());
      rootConsumers.addAll(descendants.getFusibleConsumers());
    }
    this.fusedPipeline =
        fusePipeline(
            unfusedRootNodes,
            groupSiblings(rootConsumers),
            ImmutableSet.copyOf(p.getRequirementsList()));
  }

  /**
   * Fuses a {@link Pipeline} into a collection of {@link ExecutableStage ExecutableStages}.
   *
   * <p>This fuser expects each ExecutableStage to have exactly one input. This means that pipelines
   * must be rooted at Impulse, or other runner-executed primitive transforms.
   */
  public static FusedPipeline fuse(Pipeline p) {
    return new GreedyPipelineFuser(p).fusedPipeline;
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
   *   <li>Create a {@link GreedyStageFuser} with those siblings as the initial consuming transforms
   *       of the stage
   *   <li>For each materialized {@link PCollectionNode}, find all of the descendant in-environment
   *       consumers. See {@link #getDescendantConsumers(PCollectionNode)} for details.
   *   <li>Construct all of the sibling sets from the descendant in-environment consumers, and add
   *       them to the queue of sibling sets.
   * </ul>
   */
  private FusedPipeline fusePipeline(
      Collection<PTransformNode> initialUnfusedTransforms,
      NavigableSet<NavigableSet<CollectionConsumer>> initialConsumers,
      Set<String> requirements) {
    Map<CollectionConsumer, ExecutableStage> consumedCollectionsAndTransforms = new HashMap<>();
    Set<ExecutableStage> stages = new LinkedHashSet<>();
    Set<PTransformNode> unfusedTransforms = new LinkedHashSet<>(initialUnfusedTransforms);

    Queue<Set<CollectionConsumer>> pendingSiblingSets = new ArrayDeque<>(initialConsumers);
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
        DescendantConsumers descendantConsumers = getDescendantConsumers(materializedOutput);
        unfusedTransforms.addAll(descendantConsumers.getUnfusedNodes());
        NavigableSet<NavigableSet<CollectionConsumer>> siblings =
            groupSiblings(descendantConsumers.getFusibleConsumers());

        pendingSiblingSets.addAll(siblings);
      }
    }
    // TODO: Figure out where to store this.
    DeduplicationResult deduplicated =
        OutputDeduplicator.ensureSingleProducer(pipeline, stages, unfusedTransforms);
    // TODO: Stages can be fused with each other, if doing so does not introduce duplicate paths
    // for an element to take through the Pipeline. Compatible siblings can generally be fused,
    // as can compatible producers/consumers if a PCollection is only materialized once.
    return FusedPipeline.of(
        deduplicated.getDeduplicatedComponents(),
        stages.stream()
            .map(stage -> deduplicated.getDeduplicatedStages().getOrDefault(stage, stage))
            .map(GreedyPipelineFuser::sanitizeDanglingPTransformInputs)
            .collect(Collectors.toSet()),
        Sets.union(
            deduplicated.getIntroducedTransforms(),
            unfusedTransforms.stream()
                .map(
                    transform ->
                        deduplicated
                            .getDeduplicatedTransforms()
                            .getOrDefault(transform.getId(), transform))
                .collect(Collectors.toSet())),
        requirements);
  }

  private DescendantConsumers getRootConsumers(PTransformNode rootNode) {
    checkArgument(
        rootNode.getTransform().getInputsCount() == 0,
        "Transform %s is not at the root of the graph (consumes %s)",
        rootNode.getId(),
        rootNode.getTransform().getInputsMap());
    checkArgument(
        !pipeline.getEnvironment(rootNode).isPresent(),
        "%s requires all root nodes to be runner-implemented %s or %s primitives, "
            + "but transform %s executes in environment %s",
        GreedyPipelineFuser.class.getSimpleName(),
        PTransformTranslation.IMPULSE_TRANSFORM_URN,
        PTransformTranslation.READ_TRANSFORM_URN,
        rootNode.getId(),
        pipeline.getEnvironment(rootNode));
    Set<PTransformNode> unfused = new HashSet<>();
    unfused.add(rootNode);
    NavigableSet<CollectionConsumer> environmentNodes = new TreeSet<>();
    // Walk down until the first environments are found, and fuse them as appropriate.
    for (PCollectionNode output : pipeline.getOutputPCollections(rootNode)) {
      DescendantConsumers descendants = getDescendantConsumers(output);
      unfused.addAll(descendants.getUnfusedNodes());
      environmentNodes.addAll(descendants.getFusibleConsumers());
    }
    return DescendantConsumers.of(unfused, environmentNodes);
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
  private DescendantConsumers getDescendantConsumers(PCollectionNode inputPCollection) {
    Set<PTransformNode> unfused = new HashSet<>();
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
        unfused.add(consumer);
        for (PCollectionNode output : pipeline.getOutputPCollections(consumer)) {
          // Recurse to all of the ouput PCollections of this PTransform.
          DescendantConsumers descendants = getDescendantConsumers(output);
          unfused.addAll(descendants.getUnfusedNodes());
          downstreamConsumers.addAll(descendants.getFusibleConsumers());
        }
      }
    }
    return DescendantConsumers.of(unfused, downstreamConsumers);
  }

  @AutoValue
  abstract static class DescendantConsumers {
    static DescendantConsumers of(
        Set<PTransformNode> unfusible, NavigableSet<CollectionConsumer> fusible) {
      return new AutoValue_GreedyPipelineFuser_DescendantConsumers(unfusible, fusible);
    }

    abstract Set<PTransformNode> getUnfusedNodes();

    abstract NavigableSet<CollectionConsumer> getFusibleConsumers();
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
        if (existingConsumers.stream()
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
    @SuppressWarnings("JdkObsolete")
    NavigableSet<NavigableSet<CollectionConsumer>> orderedSiblings =
        new TreeSet<>(Comparator.comparing(NavigableSet::first));
    orderedSiblings.addAll(compatibleConsumers.values());
    return orderedSiblings;
  }

  private ExecutableStage fuseSiblings(Set<CollectionConsumer> mutuallyCompatible) {
    PCollectionNode rootCollection = mutuallyCompatible.iterator().next().consumedCollection();
    return GreedyStageFuser.forGrpcPortRead(
        pipeline,
        rootCollection,
        mutuallyCompatible.stream()
            .map(CollectionConsumer::consumingTransform)
            .collect(Collectors.toSet()));
  }

  private static ExecutableStage sanitizeDanglingPTransformInputs(ExecutableStage stage) {
    /* Possible inputs to a PTransform can only be those which are:
     * <ul>
     *  <li>Explicit input PCollection to the stage
     *  <li>Outputs of a PTransform within the same stage
     *  <li>Timer PCollections
     *  <li>Side input PCollections
     *  <li>Explicit outputs from the stage
     * </ul>
     */
    Set<String> possibleInputs = new HashSet<>();
    possibleInputs.add(stage.getInputPCollection().getId());
    possibleInputs.addAll(
        stage.getOutputPCollections().stream()
            .map(PCollectionNode::getId)
            .collect(Collectors.toSet()));
    possibleInputs.addAll(
        stage.getSideInputs().stream()
            .map(s -> s.collection().getId())
            .collect(Collectors.toSet()));
    possibleInputs.addAll(
        stage.getTransforms().stream()
            .flatMap(t -> t.getTransform().getOutputsMap().values().stream())
            .collect(Collectors.toSet()));
    Set<String> danglingInputs =
        stage.getTransforms().stream()
            .flatMap(t -> t.getTransform().getInputsMap().values().stream())
            .filter(in -> !possibleInputs.contains(in))
            .collect(Collectors.toSet());

    ImmutableList.Builder<PTransformNode> pTransformNodesBuilder = ImmutableList.builder();
    for (PTransformNode transformNode : stage.getTransforms()) {
      PTransform transform = transformNode.getTransform();
      Map<String, String> validInputs =
          transform.getInputsMap().entrySet().stream()
              .filter(e -> !danglingInputs.contains(e.getValue()))
              .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

      if (!validInputs.equals(transform.getInputsMap())) {
        // Dangling inputs found so recreate pTransform without the dangling inputs.
        transformNode =
            PipelineNode.pTransform(
                transformNode.getId(),
                transform.toBuilder().clearInputs().putAllInputs(validInputs).build());
      }

      pTransformNodesBuilder.add(transformNode);
    }
    ImmutableList<PTransformNode> pTransformNodes = pTransformNodesBuilder.build();
    Components.Builder componentBuilder = stage.getComponents().toBuilder();
    // Update the pTransforms in components.
    componentBuilder
        .clearTransforms()
        .putAllTransforms(
            pTransformNodes.stream()
                .collect(Collectors.toMap(PTransformNode::getId, PTransformNode::getTransform)));
    Map<String, PCollection> validPCollectionMap =
        stage.getComponents().getPcollectionsMap().entrySet().stream()
            .filter(e -> !danglingInputs.contains(e.getKey()))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    // Update pCollections in the components.
    componentBuilder.clearPcollections().putAllPcollections(validPCollectionMap);

    return ImmutableExecutableStage.of(
        componentBuilder.build(),
        stage.getEnvironment(),
        stage.getInputPCollection(),
        stage.getSideInputs(),
        stage.getUserStates(),
        stage.getTimers(),
        pTransformNodes,
        stage.getOutputPCollections(),
        stage.getWireCoderSettings());
  }

  /**
   * A ({@link PCollectionNode}, {@link PTransformNode}) pair representing a single {@link
   * PTransformNode} consuming a single materialized {@link PCollectionNode}.
   *
   * <p>For convenience, {@link CollectionConsumer} implements {@link Comparable}. The natural
   * ordering of {@link CollectionConsumer} is first by the IDs of the {@link
   * #consumedCollection()}, then by the ID of the {@link #consumingTransform()}.
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
