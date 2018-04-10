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
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.graph.ElementOrder;
import com.google.common.graph.EndpointPair;
import com.google.common.graph.MutableNetwork;
import com.google.common.graph.Network;
import com.google.common.graph.NetworkBuilder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.function.Function;

/** Static utility methods for {@link Network} instances that are directed. */
public class Networks {
  /**
   * An abstract class that can be extended to apply a function in a type safe manner.
   *
   * <p>Applies {@link #typedApply} to all instances of {@code type}. Otherwise returns the existing
   * {@code Node} unmodified.
   */
  public abstract static class TypeSafeNodeFunction<NodeT, T extends NodeT>
      implements Function<NodeT, NodeT> {
    private final Class<T> type;

    public TypeSafeNodeFunction(Class<T> type) {
      checkNotNull(type);
      this.type = type;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final NodeT apply(NodeT input) {
      if (type.isInstance(input)) {
        return typedApply((T) input);
      }
      return input;
    }

    public abstract NodeT typedApply(T input);
  }

  /**
   * Applies the {@code function} to all nodes within the {@code network}. Replaces any node which
   * is not {@link #equals(Object)} to the original node, maintaining all existing edges between
   * nodes.
   */
  public static <NodeT, EdgeT> void replaceDirectedNetworkNodes(
      MutableNetwork<NodeT, EdgeT> network, Function<NodeT, NodeT> function) {
    checkArgument(network.isDirected(), "Only directed networks are supported, given %s", network);
    checkArgument(
        !network.allowsSelfLoops(),
        "Only networks without self loops are supported, given %s",
        network);

    // A map from the existing node to the replacement node
    Map<NodeT, NodeT> oldNodesToNewNodes = new HashMap<>(network.nodes().size());
    for (NodeT currentNode : network.nodes()) {
      NodeT newNode = function.apply(currentNode);
      // Skip updating the network if the old node is equivalent to the new node
      if (!currentNode.equals(newNode)) {
        oldNodesToNewNodes.put(currentNode, newNode);
      }
    }

    // For each replacement, connect up the existing predecessors and successors to the new node
    // and then remove the old node.
    for (Map.Entry<NodeT, NodeT> entry : oldNodesToNewNodes.entrySet()) {
      NodeT oldNode = entry.getKey();
      NodeT newNode = entry.getValue();
      network.addNode(newNode);
      for (NodeT predecessor : ImmutableSet.copyOf(network.predecessors(oldNode))) {
        for (EdgeT edge : ImmutableSet.copyOf(network.edgesConnecting(predecessor, oldNode))) {
          network.removeEdge(edge);
          network.addEdge(predecessor, newNode, edge);
        }
      }
      for (NodeT successor : ImmutableSet.copyOf(network.successors(oldNode))) {
        for (EdgeT edge : ImmutableSet.copyOf(network.edgesConnecting(oldNode, successor))) {
          network.removeEdge(edge);
          network.addEdge(newNode, successor, edge);
        }
      }
      network.removeNode(oldNode);
    }
  }

  /**
   * Returns the set of nodes that are reachable from {@code startNodes} up to and including {@code
   * endNodes}. Node B is defined as reachable from node A if there exists a path (a sequence of
   * adjacent outgoing edges) starting at node A and ending at node B which does not pass through
   * any node in {@code endNodes}. Note that a node is always reachable from itself via a
   * zero-length path.
   *
   * <p>This is a "snapshot" based on the current topology of the {@code network}, rather than a
   * live view of the set of nodes reachable from {@code node}. In other words, the returned {@link
   * Set} will not be updated after modifications to the {@code network}.
   */
  public static <NodeT, EdgeT> Set<NodeT> reachableNodes(
      Network<NodeT, EdgeT> network, Set<NodeT> startNodes, Set<NodeT> endNodes) {
    Set<NodeT> visitedNodes = new HashSet<>();
    Queue<NodeT> queuedNodes = new ArrayDeque<>();
    queuedNodes.addAll(startNodes);
    // Perform a breadth-first traversal rooted at the input node.
    while (!queuedNodes.isEmpty()) {
      NodeT currentNode = queuedNodes.remove();
      // If we have already visited this node or it is a terminal node than do not add any
      // successors.
      if (!visitedNodes.add(currentNode) || endNodes.contains(currentNode)) {
        continue;
      }
      queuedNodes.addAll(network.successors(currentNode));
    }
    return visitedNodes;
  }

  /**
   * Return a set of nodes in sorted topological order.
   *
   * <p>Nodes will be considered in the order specified by the {@link Network Network's} {@link
   * Network#nodeOrder()}.
   */
  public static <NodeT> Iterable<NodeT> topologicalOrder(Network<NodeT, ?> network) {
    return computeTopologicalOrder(network);
  }

  /**
   * Return a set of nodes in sorted topological order.
   *
   * <p>Nodes will be considered in the order specified by the {@link
   * ElementOrder#sorted(Comparator) sorted ElementOrder} created with the provided comparator.
   */
  public static <NodeT, EdgeT> Iterable<NodeT> topologicalOrder(
      Network<NodeT, EdgeT> network, Comparator<NodeT> nodeOrder) {
    // Copy the characteristics of the network to ensure that the result network can represent the
    // original network, just with the provided suborder
    MutableNetwork<NodeT, EdgeT> orderedNetwork =
        NetworkBuilder.from(network).nodeOrder(ElementOrder.sorted(nodeOrder)).build();
    for (NodeT node : network.nodes()) {
      orderedNetwork.addNode(node);
    }
    for (EdgeT edge : network.edges()) {
      EndpointPair<NodeT> incident = network.incidentNodes(edge);
      orderedNetwork.addEdge(incident.source(), incident.target(), edge);
    }
    return computeTopologicalOrder(orderedNetwork);
  }

  /**
   * Compute the topological order for a {@link Network}.
   *
   * <p>Nodes must be considered in the order specified by the {@link Network Network's} {@link
   * Network#nodeOrder()}. This ensures that any two Networks with the same nodes and node orders
   * produce the same result.
   */
  private static <NodeT> Iterable<NodeT> computeTopologicalOrder(Network<NodeT, ?> network) {
    // TODO: (github/guava/2641) Upgrade Guava and remove this method if topological sorting becomes
    // supported externally or remove this comment if its not going to be supported externally.

    checkArgument(network.isDirected(), "Only directed networks are supported, given %s", network);
    checkArgument(
        !network.allowsSelfLoops(),
        "Only networks without self loops are supported, given %s",
        network);

    // Linked hashset will prevent duplicates from appearing and will maintain insertion order.
    LinkedHashSet<NodeT> nodes = new LinkedHashSet<>(network.nodes().size());
    Queue<NodeT> processingOrder = new LinkedList<>();
    // Add all the roots
    for (NodeT node : network.nodes()) {
      if (network.inDegree(node) == 0) {
        processingOrder.add(node);
      }
    }

    while (!processingOrder.isEmpty()) {
      NodeT current = processingOrder.remove();
      // If all predecessors have already been added, then we can add this node, otherwise
      // we need to add the node to the back of the processing queue.
      if (nodes.containsAll(network.predecessors(current))) {
        nodes.add(current);
        processingOrder.addAll(network.successors(current));
      } else {
        processingOrder.add(current);
      }
    }

    return nodes;
  }

  public static <NodeT, EdgeT> String toDot(Network<NodeT, EdgeT> network) {
    StringBuilder builder = new StringBuilder();
    builder.append(String.format("digraph network {%n"));
    Map<NodeT, String> nodeName = Maps.newIdentityHashMap();
    network.nodes().forEach(node -> nodeName.put(node, "n" + nodeName.size()));
    for (Entry<NodeT, String> nodeEntry : nodeName.entrySet()) {
      builder.append(
          String.format(
              "  %s [fontname=\"Courier New\" label=\"%s\"];%n",
              nodeEntry.getValue(), escapeDot(nodeEntry.getKey().toString())));
    }
    for (EdgeT edge : network.edges()) {
      EndpointPair<NodeT> endpoints = network.incidentNodes(edge);
      builder.append(
          String.format(
              "  %s -> %s [fontname=\"Courier New\" label=\"%s\"];%n",
              nodeName.get(endpoints.source()),
              nodeName.get(endpoints.target()),
              escapeDot(edge.toString())));
    }
    builder.append("}");
    return builder.toString();
  }

  private static String escapeDot(String s) {
    return s.replace("\\", "\\\\")
        .replace("\"", "\\\"")
        // http://www.graphviz.org/doc/info/attrs.html#k:escString
        // The escape sequences "\n", "\l" and "\r" divide the label into lines, centered,
        // left-justified, and right-justified, respectively.
        .replace("\n", "\\l");
  }

  /**
   * Returns a list of all distinct paths from roots of the network to leaves. The list can be in
   * arbitrary orders and can contain duplicate paths if there are multiple edges from two nodes.
   */
  public static <NodeT, EdgeT> List<List<NodeT>> allPathsFromRootsToLeaves(
      Network<NodeT, EdgeT> network) {
    Stack<List<NodeT>> paths = new Stack<>();
    // Populate the list with all roots
    for (NodeT node : network.nodes()) {
      if (network.inDegree(node) == 0) {
        paths.add(ImmutableList.of(node));
      }
    }

    List<List<NodeT>> distinctPathsFromRootsToLeaves = new ArrayList<>();
    while (!paths.empty()) {
      List<NodeT> path = paths.pop();
      NodeT lastNode = path.get(path.size() - 1);
      if (network.outDegree(lastNode) == 0) {
        distinctPathsFromRootsToLeaves.add(new ArrayList<>(path));
      } else {
        for (EdgeT edge : network.outEdges(lastNode)) {
          paths.push(
              ImmutableList.<NodeT>builder()
                  .addAll(path)
                  .add(network.incidentNodes(edge).target())
                  .build());
        }
      }
    }
    return distinctPathsFromRootsToLeaves;
  }

  // Hide visibility to prevent instantiation
  private Networks() {}
}
