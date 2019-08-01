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
package org.apache.beam.runners.dataflow.worker.graph;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ExecutionLocation;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableTable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;

/**
 * A function which sets the location for {@link FlattenInstruction}s by looking at the locations of
 * the nodes' predecessors and successors.
 *
 * <p>Locations for flatten nodes are chosen to minimize the amount of gRPC ports that data must be
 * transferred over. Thus for any select flatten node the location is deduced based on if its
 * predecessors and successors execute in the Runner, SDK harness, both, or neither. The final
 * location of the flatten node is chosen based on the following table.
 *
 * <p>(Predecessors along Y axis, Successors along X axis)
 *
 * <pre>{@code
 *         || SDK    | Runner | Both      | Neither |
 * ==================================================
 * SDK     || SDK    | Runner | SDK       | SDK     |
 * --------------------------------------------------
 * Runner  || Runner | Runner | Runner    | Runner  |
 * --------------------------------------------------
 * Both    || SDK    | Runner | Ambiguous | Runner  |
 * --------------------------------------------------
 * Neither || SDK    | Runner | Runner    | Runner  |
 * --------------------------------------------------
 * }</pre>
 *
 * <p>The ambiguous result means that executing the flatten in either the SDK or Runner is equally
 * inefficient, and thus it can execute in either one.
 */
public class DeduceFlattenLocationsFunction
    implements Function<MutableNetwork<Node, Edge>, MutableNetwork<Node, Edge>> {

  /** Represents the execution location of an group of connected nodes. */
  private enum AggregatedLocation {
    NEITHER, // None of the nodes have a set execution location.
    SDK_HARNESS, // All the nodes execute in the SDK harness.
    RUNNER_HARNESS, // All of the nodes execute in the runner harness.
    BOTH, // Some nodes execute in the SDK harness and some in the runner harness.
  }

  private static final ImmutableTable<AggregatedLocation, AggregatedLocation, ExecutionLocation>
      DEDUCTION_TABLE =
          new ImmutableTable.Builder<AggregatedLocation, AggregatedLocation, ExecutionLocation>()
              .put(
                  AggregatedLocation.SDK_HARNESS,
                  AggregatedLocation.SDK_HARNESS,
                  ExecutionLocation.SDK_HARNESS)
              .put(
                  AggregatedLocation.SDK_HARNESS,
                  AggregatedLocation.RUNNER_HARNESS,
                  ExecutionLocation.RUNNER_HARNESS)
              .put(
                  AggregatedLocation.SDK_HARNESS,
                  AggregatedLocation.BOTH,
                  ExecutionLocation.SDK_HARNESS)
              .put(
                  AggregatedLocation.SDK_HARNESS,
                  AggregatedLocation.NEITHER,
                  ExecutionLocation.SDK_HARNESS)
              .put(
                  AggregatedLocation.RUNNER_HARNESS,
                  AggregatedLocation.SDK_HARNESS,
                  ExecutionLocation.RUNNER_HARNESS)
              .put(
                  AggregatedLocation.RUNNER_HARNESS,
                  AggregatedLocation.RUNNER_HARNESS,
                  ExecutionLocation.RUNNER_HARNESS)
              .put(
                  AggregatedLocation.RUNNER_HARNESS,
                  AggregatedLocation.BOTH,
                  ExecutionLocation.RUNNER_HARNESS)
              .put(
                  AggregatedLocation.RUNNER_HARNESS,
                  AggregatedLocation.NEITHER,
                  ExecutionLocation.RUNNER_HARNESS)
              .put(
                  AggregatedLocation.BOTH,
                  AggregatedLocation.SDK_HARNESS,
                  ExecutionLocation.SDK_HARNESS)
              .put(
                  AggregatedLocation.BOTH,
                  AggregatedLocation.RUNNER_HARNESS,
                  ExecutionLocation.RUNNER_HARNESS)
              .put(AggregatedLocation.BOTH, AggregatedLocation.BOTH, ExecutionLocation.AMBIGUOUS)
              .put(
                  AggregatedLocation.BOTH,
                  AggregatedLocation.NEITHER,
                  ExecutionLocation.RUNNER_HARNESS)
              .put(
                  AggregatedLocation.NEITHER,
                  AggregatedLocation.SDK_HARNESS,
                  ExecutionLocation.SDK_HARNESS)
              .put(
                  AggregatedLocation.NEITHER,
                  AggregatedLocation.RUNNER_HARNESS,
                  ExecutionLocation.RUNNER_HARNESS)
              .put(
                  AggregatedLocation.NEITHER,
                  AggregatedLocation.BOTH,
                  ExecutionLocation.RUNNER_HARNESS)
              .put(
                  AggregatedLocation.NEITHER,
                  AggregatedLocation.NEITHER,
                  ExecutionLocation.RUNNER_HARNESS)
              .build();

  /**
   * Deduces an {@link ExecutionLocation} for each flatten by first checking the locations of all
   * the predecessors and successors to each node. These locations are aggregated to a single result
   * representing all successors/predecessors. Once the aggregated location for both successors and
   * predecessors are found they are used to determine the execution location of the flatten node
   * itself and the flattens are replaced by copies that include the updated {@link
   * ExecutionLocation}.
   */
  @Override
  public MutableNetwork<Node, Edge> apply(MutableNetwork<Node, Edge> network) {
    Map<Node, AggregatedLocation> predecessorLocationsMap = new HashMap<>();
    Map<Node, AggregatedLocation> successorLocationsMap = new HashMap<>();
    Map<Node, ExecutionLocation> deducedLocationsMap = new HashMap<>();
    ImmutableList<Node> flattens =
        ImmutableList.copyOf(Iterables.filter(network.nodes(), IsFlatten.INSTANCE));

    // Find all predecessor and successor locations for every flatten.
    for (Node flatten : flattens) {
      AggregatedLocation predecessorLocations = AggregatedLocation.NEITHER;
      AggregatedLocation successorLocations = AggregatedLocation.NEITHER;
      predecessorLocations = getPredecessorLocations(flatten, network, predecessorLocationsMap);
      successorLocations = getSuccessorLocations(flatten, network, successorLocationsMap);

      deducedLocationsMap.put(
          flatten, DEDUCTION_TABLE.get(predecessorLocations, successorLocations));
    }

    // Actually set the locations of the flattens permanently.
    Networks.replaceDirectedNetworkNodes(
        network,
        (Node node) -> {
          if (!deducedLocationsMap.containsKey(node)) {
            return node;
          }

          ParallelInstructionNode castNode = ((ParallelInstructionNode) node);
          ExecutionLocation deducedLocation = deducedLocationsMap.get(node);
          return ParallelInstructionNode.create(castNode.getParallelInstruction(), deducedLocation);
        });

    return network;
  }

  /** Enum for {@link getConnectedNodeLocations} to specify which direction to search in. */
  private enum SearchDirection {
    PREDECESSORS,
    SUCCESSORS,
  }

  /**
   * Helper function to retrieve the aggregated location of a node's predecessors. See {@link
   * DeduceFlattenLocationsFunction#getConnectedNodeLocations} for details.
   */
  private AggregatedLocation getPredecessorLocations(
      Node node,
      MutableNetwork<Node, Edge> network,
      Map<Node, AggregatedLocation> predecessorLocationsMap) {
    return getConnectedNodeLocations(
        node, network, predecessorLocationsMap, SearchDirection.PREDECESSORS);
  }

  /**
   * Helper function to retrieve the aggregated location of a node's successors. See {@link
   * DeduceFlattenLocationsFunction#getConnectedNodeLocations} for details.
   */
  private AggregatedLocation getSuccessorLocations(
      Node node,
      MutableNetwork<Node, Edge> network,
      Map<Node, AggregatedLocation> successorLocationsMap) {
    return getConnectedNodeLocations(
        node, network, successorLocationsMap, SearchDirection.SUCCESSORS);
  }

  /**
   * A function which retrieves the aggregated location of a node's connecting nodes in one
   * direction, either checking the target node's successors or predecessors. This is done by
   * checking all the connected node's locations. For nodes that do not have locations embedded in
   * the actual node (they may have unknown location or might not even be {@link
   * ParallelInstructionNode}s) the location can be deduced by recursively checking that node's
   * predecessors. To prevent a large amount of needless recursion a map is used for memoization;
   * The results of this function will be stored in the map so that they can be retrieved later if
   * needed without having to perform the recursions again.
   */
  private AggregatedLocation getConnectedNodeLocations(
      Node node,
      MutableNetwork<Node, Edge> network,
      Map<Node, AggregatedLocation> connectedLocationsMap,
      SearchDirection direction) {
    // First check the map
    if (connectedLocationsMap.containsKey(node)) {
      return connectedLocationsMap.get(node);
    }

    boolean hasSdkConnections = false;
    boolean hasRunnerConnections = false;

    Set<Node> connectedNodes;
    if (direction == SearchDirection.SUCCESSORS) {
      connectedNodes = network.successors(node);
    } else {
      connectedNodes = network.predecessors(node);
    }

    // Get the location of each connected node by checking three different places for it. First
    // try checking the ExecutionLocation of the node directly if it's a ParallelInstructionNode.
    // If that doesn't work, try checking the map passed in as a parameter, and if that doesn't
    // work recurse this function to the unknown node.
    for (Node connectedNode : connectedNodes) {
      if (connectedNode instanceof ParallelInstructionNode
          && ((ParallelInstructionNode) connectedNode).getExecutionLocation()
              != ExecutionLocation.UNKNOWN) {
        ExecutionLocation executionLocation =
            ((ParallelInstructionNode) connectedNode).getExecutionLocation();
        switch (executionLocation) {
          case SDK_HARNESS:
            hasSdkConnections = true;
            break;
          case RUNNER_HARNESS:
            hasRunnerConnections = true;
            break;
          case AMBIGUOUS:
            hasSdkConnections = true;
            hasRunnerConnections = true;
            break;
          default:
            throw new IllegalStateException("Unknown case " + executionLocation);
        }
      } else {
        AggregatedLocation connectedLocation =
            getConnectedNodeLocations(connectedNode, network, connectedLocationsMap, direction);
        switch (connectedLocation) {
          case SDK_HARNESS:
            hasSdkConnections = true;
            break;
          case RUNNER_HARNESS:
            hasRunnerConnections = true;
            break;
          case BOTH:
            hasSdkConnections = true;
            hasRunnerConnections = true;
            break;
          case NEITHER:
            break;
          default:
            throw new IllegalStateException("Unknown case " + connectedLocation);
        }
      }

      // If nodes in the SDK and Runner have been found, the result for this node is "Both", so no
      // need to continue checking.
      if (hasSdkConnections && hasRunnerConnections) {
        break;
      }
    }

    // Return aggregated locations for this node's connections and store it in the map.
    AggregatedLocation aggregatedLocation;
    if (hasSdkConnections && hasRunnerConnections) {
      aggregatedLocation = AggregatedLocation.BOTH;
    } else if (hasSdkConnections) {
      aggregatedLocation = AggregatedLocation.SDK_HARNESS;
    } else if (hasRunnerConnections) {
      aggregatedLocation = AggregatedLocation.RUNNER_HARNESS;
    } else {
      aggregatedLocation = AggregatedLocation.NEITHER;
    }

    connectedLocationsMap.put(node, aggregatedLocation);
    return aggregatedLocation;
  }

  /**
   * A {@link Predicate} which returns true iff the {@link Node} represents a {@link
   * ParallelInstructionNode} with a {@link FlattenInstruction}.
   */
  private static class IsFlatten implements Predicate<Node> {
    private static final IsFlatten INSTANCE = new IsFlatten();

    @Override
    public boolean apply(Node node) {
      return node instanceof ParallelInstructionNode
          && ((ParallelInstructionNode) node).getParallelInstruction().getFlatten() != null;
    }

    // Hide visibility to prevent instantiation
    private IsFlatten() {}
  }
}
