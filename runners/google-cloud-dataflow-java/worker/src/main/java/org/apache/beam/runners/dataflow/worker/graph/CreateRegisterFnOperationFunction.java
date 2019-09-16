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

import com.google.api.services.dataflow.model.FlattenInstruction;
import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.ParallelInstruction;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.HappensBeforeEdge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.MultiOutputInfoEdge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.Graphs;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.ImmutableNetwork;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;

/**
 * Splits the instruction graph into SDK and runner harness portions replacing the SDK sub-graphs
 * with operations linked by gRPC ports.
 *
 * <p>This transform expects that all {@link FlattenInstruction}s have been removed. {@link
 * FlattenInstruction}s are not required because Beam Fn API multiplexers handle multiple inputs.
 * Also, {@link FlattenInstruction}s complicate the implementation as to what fused section of the
 * graph executes in the Runner harness versus what executes in the SDK harness since you need to
 * walk the instruction graph across flattens to find whether the neighboring instruction executes
 * in the Runner or in the SDK harness.
 *
 * <p>Note that this implementation minimizes the amount of data that will be transferred across a
 * port by creating new edges which represent a smaller portion of each PCollection. For example:
 *
 * <pre><code>
 * RunnerSource --\   /--> RunnerParDo
 *                 out
 * CustomSource --/   \--> SdkParDo
 * </code></pre>
 *
 * Should produce:
 *
 * <pre><code>
 *        PortB --> out --\
 * RunnerSource --> out --> RunnerParDo
 *                     \--> PortA
 *        PortA --> out --\
 * CustomSource --> out --> SdkParDo
 *                     \--> PortB
 * </code></pre>
 */
public class CreateRegisterFnOperationFunction
    implements Function<MutableNetwork<Node, Edge>, MutableNetwork<Node, Edge>> {
  private final IdGenerator idGenerator;
  private final Supplier<Node> portSupplier;
  private final Function<MutableNetwork<Node, Edge>, Node> registerFnOperationFunction;
  private final boolean useExecutableStageBundleExecution;

  /**
   * Constructs a function which is able to break up the instruction graph into SDK and Runner
   * harness portions utilizing the passed in {@code portSupplier} to create communication ports
   * between the SDK and Runner harness and the {@code registerFnOperationFunction} to notify the
   * SDK of functions it is required to execute.
   *
   * @param portSupplier A {@link Supplier} which produces nodes to communicate data between the SDK
   *     and Runner harness.
   * @param registerFnOperationFunction A {@link Function} which takes a {@link MutableNetwork} and
   *     produces a {@link Node} that is able to register the SDK functions within the SDK harness.
   */
  public CreateRegisterFnOperationFunction(
      IdGenerator idGenerator,
      Supplier<Node> portSupplier,
      Function<MutableNetwork<Node, Edge>, Node> registerFnOperationFunction,
      boolean useExecutableStageBundleExecution) {
    this.idGenerator = idGenerator;
    this.portSupplier = portSupplier;
    this.registerFnOperationFunction = registerFnOperationFunction;
    this.useExecutableStageBundleExecution = useExecutableStageBundleExecution;
  }

  @Override
  public MutableNetwork<Node, Edge> apply(MutableNetwork<Node, Edge> network) {

    // Record all SDK nodes, and all root nodes.
    Set<Node> runnerRootNodes = new HashSet<>();
    Set<Node> sdkNodes = new HashSet<>();
    Set<Node> sdkRootNodes = new HashSet<>();
    for (ParallelInstructionNode node :
        Iterables.filter(network.nodes(), ParallelInstructionNode.class)) {
      if (executesInSdkHarness(node)) {
        sdkNodes.add(node);
        if (network.inDegree(node) == 0) {
          sdkRootNodes.add(node);
        }
      } else if (network.inDegree(node) == 0) {
        runnerRootNodes.add(node);
      }
    }

    // If nothing executes within the SDK harness, return the original network.
    if (sdkNodes.isEmpty()) {
      return network;
    }

    // Represents the set of nodes which represent gRPC boundaries from the Runner to the SDK.
    Set<Node> runnerToSdkBoundaries = new HashSet<>();
    // Represents the set of nodes which represent gRPC boundaries from the SDK to the Runner.
    Set<Node> sdkToRunnerBoundaries = new HashSet<>();

    ImmutableNetwork<Node, Edge> originalNetwork = ImmutableNetwork.copyOf(network);

    // Update the network with outputs which are meant to bridge the instructions
    // that execute in different harnesses. One output per direction of information
    // flow from runner to SDK and SDK to runner per original output node.
    for (InstructionOutputNode outputNode :
        Iterables.filter(originalNetwork.nodes(), InstructionOutputNode.class)) {

      // Categorize all predecessor instructions
      Set<Node> predecessorRunnerInstructions = new HashSet<>();
      Set<Node> predecessorSdkInstructions = new HashSet<>();
      for (Node predecessorInstruction : originalNetwork.predecessors(outputNode)) {
        if (sdkNodes.contains(predecessorInstruction)) {
          predecessorSdkInstructions.add(predecessorInstruction);
        } else {
          predecessorRunnerInstructions.add(predecessorInstruction);
        }
      }

      // Categorize all successor instructions
      Set<Node> successorRunnerInstructions = new HashSet<>();
      Set<Node> successorSdkInstructions = new HashSet<>();
      for (Node successorInstruction : originalNetwork.successors(outputNode)) {
        if (sdkNodes.contains(successorInstruction)) {
          successorSdkInstructions.add(successorInstruction);
        } else {
          successorRunnerInstructions.add(successorInstruction);
        }
      }

      // If there is data that will be flowing from the Runner to the SDK, rewire network to have
      // nodes connected across a gRPC node. Also add the gRPC node as an SDK root.
      if (!predecessorRunnerInstructions.isEmpty() && !successorSdkInstructions.isEmpty()) {
        runnerToSdkBoundaries.add(
            rewireAcrossSdkRunnerPortNode(
                network, outputNode, predecessorRunnerInstructions, successorSdkInstructions));
      }

      // If there is data that will be flowing from the SDK to the Runner, rewire network to have
      // nodes connected across a gRPC node.
      if (!predecessorSdkInstructions.isEmpty() && !successorRunnerInstructions.isEmpty()) {
        sdkToRunnerBoundaries.add(
            rewireAcrossSdkRunnerPortNode(
                network, outputNode, predecessorSdkInstructions, successorRunnerInstructions));
      }

      // Remove original output node if it was rewired because it will have become disconnected
      // through the new output node.
      if (network.inDegree(outputNode) == 0) {
        network.removeNode(outputNode);
      }
    }

    // Create the subnetworks that represent potentially multiple fused SDK portions and a single
    // fused Runner portion replacing the SDK portion that is embedded within the Runner portion
    // with a RegisterFnOperation, adding edges to maintain proper happens before relationships.
    Set<Node> allRunnerNodes =
        Networks.reachableNodes(
            network, Sets.union(runnerRootNodes, sdkToRunnerBoundaries), runnerToSdkBoundaries);
    if (this.useExecutableStageBundleExecution) {
      // When using shared library, there is no grpc node in runner graph.
      allRunnerNodes =
          Sets.difference(allRunnerNodes, Sets.union(runnerToSdkBoundaries, sdkToRunnerBoundaries));
    }
    MutableNetwork<Node, Edge> runnerNetwork = Graphs.inducedSubgraph(network, allRunnerNodes);

    // TODO: Reduce the amount of 'copying' of SDK nodes by breaking potential cycles
    // between the SDK networks and the Runner network. Cycles can occur because entire
    // SDK subnetworks are replaced by a singular node within the Runner network.
    // khines@ suggested to look at go/priority-based-fusion for an algorithm based upon
    // using poison paths.
    for (Node sdkRoot : Sets.union(sdkRootNodes, runnerToSdkBoundaries)) {
      Set<Node> sdkSubnetworkNodes =
          Networks.reachableNodes(network, ImmutableSet.of(sdkRoot), sdkToRunnerBoundaries);
      MutableNetwork<Node, Edge> sdkNetwork = Graphs.inducedSubgraph(network, sdkSubnetworkNodes);
      Node registerFnNode = registerFnOperationFunction.apply(sdkNetwork);

      runnerNetwork.addNode(registerFnNode);
      // Create happens before relationships between all Runner and SDK nodes which are in the
      // SDK subnetwork; direction dependent on whether its a predecessor of the SDK subnetwork or
      // a successor.
      if (this.useExecutableStageBundleExecution) {
        // When using shared library, there is no gprc node in runner graph. Then the registerFnNode
        // should be linked directly to 2 OutputInstruction nodes.
        for (Node predecessor : Sets.intersection(sdkSubnetworkNodes, runnerToSdkBoundaries)) {
          predecessor = network.predecessors(predecessor).iterator().next();
          runnerNetwork.addEdge(predecessor, registerFnNode, HappensBeforeEdge.create());
        }
        for (Node successor : Sets.intersection(sdkSubnetworkNodes, sdkToRunnerBoundaries)) {
          successor = network.successors(successor).iterator().next();
          runnerNetwork.addEdge(registerFnNode, successor, HappensBeforeEdge.create());
        }
      } else {
        for (Node predecessor : Sets.intersection(sdkSubnetworkNodes, runnerToSdkBoundaries)) {
          runnerNetwork.addEdge(predecessor, registerFnNode, HappensBeforeEdge.create());
        }
        for (Node successor : Sets.intersection(sdkSubnetworkNodes, sdkToRunnerBoundaries)) {
          runnerNetwork.addEdge(registerFnNode, successor, HappensBeforeEdge.create());
        }
      }
    }

    return runnerNetwork;
  }

  /**
   * Rewires the given set of predecessors and successors across a gRPC port surrounded by output
   * nodes. Edges to the remaining successors are copied over to the new output node that is placed
   * before the port node. For example:
   *
   * <pre><code>
   * predecessors --> outputNode --> successors
   *                            \--> existingSuccessors
   * </pre></code> becomes:
   *
   * <pre><code>
   *
   *                                outputNode -------------------------------\
   *                                          \                                \
   *                                           |-> existingSuccessors           \
   *                                          /                                  \
   * predecessors --> newPredecessorOutputNode --> portNode --> portOutputNode --> successors}.
   * </code></pre>
   */
  private Node rewireAcrossSdkRunnerPortNode(
      MutableNetwork<Node, Edge> network,
      InstructionOutputNode outputNode,
      Set<Node> predecessors,
      Set<Node> successors) {

    InstructionOutputNode newPredecessorOutputNode =
        InstructionOutputNode.create(
            outputNode.getInstructionOutput(), outputNode.getPcollectionId());
    InstructionOutputNode portOutputNode =
        InstructionOutputNode.create(
            outputNode.getInstructionOutput(), outputNode.getPcollectionId());
    Node portNode = portSupplier.get();
    network.addNode(newPredecessorOutputNode);
    network.addNode(portNode);
    for (Node predecessor : predecessors) {
      for (Edge edge : ImmutableList.copyOf(network.edgesConnecting(predecessor, outputNode))) {
        network.removeEdge(edge);
        network.addEdge(predecessor, newPredecessorOutputNode, edge);
      }
    }

    // Maintain edges for existing successors.
    List<Node> existingSuccessors =
        ImmutableList.copyOf(Sets.difference(network.successors(outputNode), successors));
    for (Node existingSuccessor : existingSuccessors) {
      List<Edge> existingSuccessorEdges =
          ImmutableList.copyOf(network.edgesConnecting(outputNode, existingSuccessor));
      for (Edge existingSuccessorEdge : existingSuccessorEdges) {
        network.addEdge(newPredecessorOutputNode, existingSuccessor, existingSuccessorEdge.clone());
      }
    }
    // Rewire the requested successors over the port node.
    network.addEdge(
        newPredecessorOutputNode,
        portNode,
        MultiOutputInfoEdge.create(new MultiOutputInfo().setTag(idGenerator.getId())));
    network.addEdge(
        portNode,
        portOutputNode,
        MultiOutputInfoEdge.create(new MultiOutputInfo().setTag(idGenerator.getId())));
    for (Node successor : successors) {
      for (Edge edge : ImmutableList.copyOf(network.edgesConnecting(outputNode, successor))) {
        network.addEdge(portOutputNode, successor, edge.clone());
      }
    }
    return portNode;
  }

  /**
   * Returns true iff the {@link ParallelInstruction} represents an instruction that executes within
   * the SDK harness as part of the fused sub-graph. For details on how node locations are deduced
   * refer to {@link DeduceNodeLocationsFunction#executesInSdkHarness}.
   */
  private static boolean executesInSdkHarness(ParallelInstructionNode node)
      throws IllegalStateException {
    Nodes.ExecutionLocation location = node.getExecutionLocation();
    if (location == Nodes.ExecutionLocation.UNKNOWN) {
      throw new IllegalStateException("Node must not have unknown execution location: " + node);
    }
    return location == Nodes.ExecutionLocation.SDK_HARNESS;
  }
}
