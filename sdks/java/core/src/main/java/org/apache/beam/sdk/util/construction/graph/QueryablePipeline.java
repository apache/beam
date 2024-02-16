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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ParDoPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.NativeTransforms;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.MutableNetwork;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.Network;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.NetworkBuilder;

/**
 * A {@link Pipeline} which has additional methods to relate nodes in the graph relative to each
 * other.
 */
@SuppressWarnings({"nullness", "keyfor"}) // TODO(https://github.com/apache/beam/issues/20497)
public class QueryablePipeline {
  // TODO: Is it better to have the signatures here require nodes in almost all contexts, or should
  // they all take strings? Nodes gives some degree of type signalling that names might not, but
  // it's more painful to construct the node. However, right now the traversal is done starting
  // at the roots and using nodes everywhere based on what any query has returned.
  /**
   * Create a new {@link QueryablePipeline} based on the provided components.
   *
   * <p>The returned {@link QueryablePipeline} will contain only the primitive transforms present
   * within the provided components.
   */
  public static QueryablePipeline forPrimitivesIn(Components components) {
    return new QueryablePipeline(getPrimitiveTransformIds(components), components);
  }

  /**
   * Create a new {@link QueryablePipeline} which uses the root transform IDs and components of the
   * provided {@link Pipeline}.
   */
  public static QueryablePipeline forPipeline(RunnerApi.Pipeline p) {
    return forTransforms(p.getRootTransformIdsList(), p.getComponents());
  }

  /**
   * Create a new {@link QueryablePipeline} based on the provided components containing only the
   * provided {@code transformIds}.
   */
  public static QueryablePipeline forTransforms(
      Collection<String> transformIds, Components components) {
    return new QueryablePipeline(transformIds, components);
  }

  private final Components components;

  /**
   * The {@link Pipeline} represented by a {@link Network}.
   *
   * <p>This is a directed bipartite graph consisting of {@link PipelineNode.PTransformNode
   * PTransformNodes} and {@link PipelineNode.PCollectionNode PCollectionNodes}. Each {@link
   * PipelineNode.PCollectionNode} has exactly one in edge, and an arbitrary number of out edges.
   * Each {@link PipelineNode.PTransformNode} has an arbitrary number of in and out edges.
   *
   * <p>Parallel edges are permitted, as a {@link PipelineNode.PCollectionNode} can be consumed by a
   * single {@link PipelineNode.PTransformNode} any number of times with different local names.
   */
  private final Network<PipelineNode, PipelineEdge> pipelineNetwork;

  private QueryablePipeline(Collection<String> transformIds, Components components) {
    this.components = components;
    this.pipelineNetwork = buildNetwork(transformIds, this.components);
  }

  /** Produces a {@link RunnerApi.Components} which contains only primitive transforms. */
  @VisibleForTesting
  static Collection<String> getPrimitiveTransformIds(RunnerApi.Components components) {
    Collection<String> ids = new LinkedHashSet<>();

    for (Map.Entry<String, PTransform> transformEntry : components.getTransformsMap().entrySet()) {
      PTransform transform = transformEntry.getValue();
      boolean isPrimitive = isPrimitiveTransform(transform);
      if (isPrimitive) {
        // Sometimes "primitive" transforms have sub-transforms (and even deeper-nested
        // descendents), due to runners
        // either rewriting them in terms of runner-specific transforms, or SDKs constructing them
        // in terms of other
        // underlying transforms (see https://issues.apache.org/jira/browse/BEAM-5441).
        // We consider any "leaf" descendents of these "primitive" transforms to be the true
        // "primitives" that we
        // preserve here; in the common case, this is just the "primitive" itself, which has no
        // descendents).
        Deque<String> transforms = new ArrayDeque<>();
        transforms.push(transformEntry.getKey());
        while (!transforms.isEmpty()) {
          String id = transforms.pop();
          PTransform next = components.getTransformsMap().get(id);
          List<String> subtransforms = next.getSubtransformsList();
          if (subtransforms.isEmpty()) {
            ids.add(id);
          } else {
            transforms.addAll(subtransforms);
          }
        }
      }
    }
    return ids;
  }

  private static final Set<String> PRIMITIVE_URNS =
      ImmutableSet.of(
          PTransformTranslation.PAR_DO_TRANSFORM_URN,
          PTransformTranslation.FLATTEN_TRANSFORM_URN,
          PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN,
          PTransformTranslation.IMPULSE_TRANSFORM_URN,
          PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN,
          PTransformTranslation.TEST_STREAM_TRANSFORM_URN,
          PTransformTranslation.MAP_WINDOWS_TRANSFORM_URN,
          PTransformTranslation.READ_TRANSFORM_URN,
          PTransformTranslation.CREATE_VIEW_TRANSFORM_URN,
          PTransformTranslation.COMBINE_PER_KEY_PRECOMBINE_TRANSFORM_URN,
          PTransformTranslation.COMBINE_PER_KEY_MERGE_ACCUMULATORS_TRANSFORM_URN,
          PTransformTranslation.COMBINE_PER_KEY_EXTRACT_OUTPUTS_TRANSFORM_URN,
          PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN,
          PTransformTranslation.SPLITTABLE_PROCESS_KEYED_URN,
          PTransformTranslation.SPLITTABLE_PROCESS_ELEMENTS_URN,
          PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN,
          PTransformTranslation.SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN);

  /** Returns true if the provided transform is a primitive. */
  private static boolean isPrimitiveTransform(PTransform transform) {
    String urn = PTransformTranslation.urnForTransformOrNull(transform);
    return PRIMITIVE_URNS.contains(urn) || NativeTransforms.isNative(transform);
  }

  private MutableNetwork<PipelineNode, PipelineEdge> buildNetwork(
      Collection<String> transformIds, Components components) {
    MutableNetwork<PipelineNode, PipelineEdge> network =
        NetworkBuilder.directed().allowsParallelEdges(true).allowsSelfLoops(false).build();
    Set<PipelineNode.PCollectionNode> unproducedCollections = new HashSet<>();
    for (String transformId : transformIds) {
      PTransform transform = components.getTransformsOrThrow(transformId);
      PipelineNode.PTransformNode transformNode =
          PipelineNode.pTransform(transformId, this.components.getTransformsOrThrow(transformId));
      network.addNode(transformNode);
      for (String produced : transform.getOutputsMap().values()) {
        PipelineNode.PCollectionNode producedNode =
            PipelineNode.pCollection(produced, components.getPcollectionsOrThrow(produced));
        network.addNode(producedNode);
        network.addEdge(transformNode, producedNode, new PerElementEdge());
        checkArgument(
            network.inDegree(producedNode) == 1,
            "A %s should have exactly one producing %s, but found %s:\nPCollection:\n%s\nProducers:\n%s",
            PipelineNode.PCollectionNode.class.getSimpleName(),
            PipelineNode.PTransformNode.class.getSimpleName(),
            network.predecessors(producedNode).size(),
            producedNode,
            network.predecessors(producedNode));
        unproducedCollections.remove(producedNode);
      }
      for (Map.Entry<String, String> consumed : transform.getInputsMap().entrySet()) {
        // This loop may add an edge between the consumed PCollection and the current PTransform.
        // The local name of the transform must be used to determine the type of edge.
        String pcollectionId = consumed.getValue();
        PipelineNode.PCollectionNode consumedNode =
            PipelineNode.pCollection(
                pcollectionId, this.components.getPcollectionsOrThrow(pcollectionId));
        if (network.addNode(consumedNode)) {
          // This node has been added to the network for the first time, so it has no producer.
          unproducedCollections.add(consumedNode);
        }
        if (getLocalSideInputNames(transform).contains(consumed.getKey())) {
          network.addEdge(consumedNode, transformNode, new SingletonEdge());
        } else {
          network.addEdge(consumedNode, transformNode, new PerElementEdge());
        }
      }
    }
    checkArgument(
        unproducedCollections.isEmpty(),
        "%ss %s were consumed but never produced",
        PipelineNode.PCollectionNode.class.getSimpleName(),
        unproducedCollections);
    return network;
  }

  public Collection<PipelineNode.PTransformNode> getTransforms() {
    return pipelineNetwork.nodes().stream()
        .filter(PipelineNode.PTransformNode.class::isInstance)
        .map(PipelineNode.PTransformNode.class::cast)
        .collect(Collectors.toList());
  }

  public Iterable<PipelineNode.PTransformNode> getTopologicallyOrderedTransforms() {
    return StreamSupport.stream(
            Networks.topologicalOrder(pipelineNetwork, Comparator.comparing(PipelineNode::getId))
                .spliterator(),
            false)
        .filter(PipelineNode.PTransformNode.class::isInstance)
        .map(PipelineNode.PTransformNode.class::cast)
        .collect(Collectors.toList());
  }

  /**
   * Get the transforms that are roots of this {@link QueryablePipeline}. These are all nodes which
   * have no input {@link PCollection}.
   */
  public Set<PipelineNode.PTransformNode> getRootTransforms() {
    return pipelineNetwork.nodes().stream()
        .filter(pipelineNode -> pipelineNetwork.inEdges(pipelineNode).isEmpty())
        .map(pipelineNode -> (PipelineNode.PTransformNode) pipelineNode)
        .collect(Collectors.toSet());
  }

  public PipelineNode.PTransformNode getProducer(PipelineNode.PCollectionNode pcollection) {
    return (PipelineNode.PTransformNode)
        Iterables.getOnlyElement(pipelineNetwork.predecessors(pcollection));
  }

  /**
   * Get all of the {@link PipelineNode.PTransformNode PTransforms} which consume the provided
   * {@link PipelineNode.PCollectionNode} on a per-element basis.
   *
   * <p>If a {@link PipelineNode.PTransformNode} consumes a {@link PipelineNode.PCollectionNode} on
   * a per-element basis one or more times, it will appear a single time in the result.
   *
   * <p>In theory, a transform may consume a single {@link PipelineNode.PCollectionNode} in both a
   * per-element and singleton manner. If this is the case, the transform node is included in the
   * result, as it does consume the {@link PipelineNode.PCollectionNode} on a per-element basis.
   */
  public Set<PipelineNode.PTransformNode> getPerElementConsumers(
      PipelineNode.PCollectionNode pCollection) {
    return pipelineNetwork.successors(pCollection).stream()
        .filter(
            consumer ->
                pipelineNetwork.edgesConnecting(pCollection, consumer).stream()
                    .anyMatch(PipelineEdge::isPerElement))
        .map(pipelineNode -> (PipelineNode.PTransformNode) pipelineNode)
        .collect(Collectors.toSet());
  }

  /**
   * Same as {@link #getPerElementConsumers(PipelineNode.PCollectionNode)}, but returns transforms
   * that consume the collection as a singleton.
   */
  public Set<PipelineNode.PTransformNode> getSingletonConsumers(
      PipelineNode.PCollectionNode pCollection) {
    return pipelineNetwork.successors(pCollection).stream()
        .filter(
            consumer ->
                pipelineNetwork.edgesConnecting(pCollection, consumer).stream()
                    .anyMatch(edge -> !edge.isPerElement()))
        .map(pipelineNode -> (PipelineNode.PTransformNode) pipelineNode)
        .collect(Collectors.toSet());
  }

  /**
   * Gets each {@link PipelineNode.PCollectionNode} that the provided {@link
   * PipelineNode.PTransformNode} consumes on a per-element basis.
   */
  public Set<PipelineNode.PCollectionNode> getPerElementInputPCollections(
      PipelineNode.PTransformNode ptransform) {
    return pipelineNetwork.inEdges(ptransform).stream()
        .filter(PipelineEdge::isPerElement)
        .map(edge -> (PipelineNode.PCollectionNode) pipelineNetwork.incidentNodes(edge).source())
        .collect(Collectors.toSet());
  }

  public Set<PipelineNode.PCollectionNode> getOutputPCollections(
      PipelineNode.PTransformNode ptransform) {
    return pipelineNetwork.successors(ptransform).stream()
        .map(pipelineNode -> (PipelineNode.PCollectionNode) pipelineNode)
        .collect(Collectors.toSet());
  }

  public Components getComponents() {
    return components;
  }

  /**
   * Returns the {@link SideInputReference SideInputReferences} that the provided transform consumes
   * as side inputs.
   */
  public Collection<SideInputReference> getSideInputs(PipelineNode.PTransformNode transform) {
    return getLocalSideInputNames(transform.getTransform()).stream()
        .map(
            localName -> {
              String transformId = transform.getId();
              PTransform transformProto = components.getTransformsOrThrow(transformId);
              String collectionId = transform.getTransform().getInputsOrThrow(localName);
              PCollection collection = components.getPcollectionsOrThrow(collectionId);
              return SideInputReference.of(
                  PipelineNode.pTransform(transformId, transformProto),
                  localName,
                  PipelineNode.pCollection(collectionId, collection));
            })
        .collect(Collectors.toSet());
  }

  public Collection<UserStateReference> getUserStates(PipelineNode.PTransformNode transform) {
    return getLocalUserStateNames(transform.getTransform()).stream()
        .map(
            localName -> {
              String transformId = transform.getId();
              PTransform transformProto = components.getTransformsOrThrow(transformId);
              // Get the main input PCollection id.
              String collectionId =
                  transform
                      .getTransform()
                      .getInputsOrThrow(
                          Iterables.getOnlyElement(
                              Sets.difference(
                                  transform.getTransform().getInputsMap().keySet(),
                                  ImmutableSet.builder()
                                      .addAll(getLocalSideInputNames(transformProto))
                                      .addAll(getLocalTimerNames(transformProto))
                                      .build())));
              PCollection collection = components.getPcollectionsOrThrow(collectionId);
              return UserStateReference.of(
                  PipelineNode.pTransform(transformId, transformProto),
                  localName,
                  PipelineNode.pCollection(collectionId, collection));
            })
        .collect(Collectors.toSet());
  }

  public Collection<TimerReference> getTimers(PipelineNode.PTransformNode transform) {
    return getLocalTimerNames(transform.getTransform()).stream()
        .map(
            localName -> {
              String transformId = transform.getId();
              PTransform transformProto = components.getTransformsOrThrow(transformId);
              return TimerReference.of(
                  PipelineNode.pTransform(transformId, transformProto), localName);
            })
        .collect(Collectors.toSet());
  }

  private Set<String> getLocalSideInputNames(PTransform transform) {
    if (PTransformTranslation.PAR_DO_TRANSFORM_URN.equals(transform.getSpec().getUrn())
        || PTransformTranslation.SPLITTABLE_PAIR_WITH_RESTRICTION_URN.equals(
            transform.getSpec().getUrn())
        || PTransformTranslation.SPLITTABLE_SPLIT_AND_SIZE_RESTRICTIONS_URN.equals(
            transform.getSpec().getUrn())
        || PTransformTranslation.SPLITTABLE_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS_URN.equals(
            transform.getSpec().getUrn())) {
      try {
        return ParDoPayload.parseFrom(transform.getSpec().getPayload()).getSideInputsMap().keySet();
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    } else {
      return Collections.emptySet();
    }
  }

  private Set<String> getLocalUserStateNames(PTransform transform) {
    if (PTransformTranslation.PAR_DO_TRANSFORM_URN.equals(transform.getSpec().getUrn())) {
      try {
        return ParDoPayload.parseFrom(transform.getSpec().getPayload()).getStateSpecsMap().keySet();
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    } else {
      return Collections.emptySet();
    }
  }

  private Set<String> getLocalTimerNames(PTransform transform) {
    if (PTransformTranslation.PAR_DO_TRANSFORM_URN.equals(transform.getSpec().getUrn())) {
      try {
        return ParDoPayload.parseFrom(transform.getSpec().getPayload())
            .getTimerFamilySpecsMap()
            .keySet();
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    } else {
      return Collections.emptySet();
    }
  }

  public Optional<Environment> getEnvironment(PipelineNode.PTransformNode parDo) {
    return Environments.getEnvironment(parDo.getId(), components);
  }

  private interface PipelineEdge {
    boolean isPerElement();
  }

  private static class PerElementEdge implements PipelineEdge {
    @Override
    public boolean isPerElement() {
      return true;
    }
  }

  private static class SingletonEdge implements PipelineEdge {
    @Override
    public boolean isPerElement() {
      return false;
    }
  }
}
