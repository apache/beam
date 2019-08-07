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

import com.google.api.services.dataflow.model.ParallelInstruction;
import java.util.Set;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ExecutionLocation;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;

/**
 * A function which optimises the execution of {@link FlattenInstruction}s with ambiguous {@link
 * ExecutionLocation}s by splitting it into two copies, with one executing on the SDK harness and
 * one on the runner harness.
 *
 * <p>After performing this function, each of the new flattens will retain the predecessors of the
 * original flatten, but only the successors that occur in the same {@link ExecutionLocation}. For
 * example, the following graph:
 *
 * <pre><code>
 * SdkPredecessor -----> out --\                        /--> SdkSuccessor
 *                              AmbiguousFlatten --> out
 * RunnerPredecessor --> out --/                        \--> RunnerSuccessor
 * </code></pre>
 *
 * Should produce:
 *
 * <pre><code>
 * SdkPredecessor -----> out --> SdkFlatten  --> out --> SdkSuccessor
 *                           X
 * RunnerPredecessor --> out --> RunnerFlatten --> out --> RunnerSuccessor
 * </code></pre>
 *
 * <p>The reason for performing this cloning is to prevent data from having to perform a "round
 * trip" through gRPC ports, which is what would happen if ambiguous flattens were executed on only
 * one harness. For example, if a flatten is executed in the runner harness, then the path from SDK
 * predecessor to SDK successor will require data to cross to the runner harness, get flattened, and
 * then cross back. With this optimization, the round trip will not occur on any paths.
 */
public class CloneAmbiguousFlattensFunction
    implements Function<MutableNetwork<Node, Edge>, MutableNetwork<Node, Edge>> {

  @Override
  public MutableNetwork<Node, Edge> apply(MutableNetwork<Node, Edge> network) {
    // Important: The cloning technique only works when the flatten being cloned has no ambiguous
    // descendants, so to ensure this is always true we iterate through the network in reverse
    // topological order.
    Set<Node> sortedNodesSet = Networks.topologicalOrder(network);
    Node[] sortedNodes = sortedNodesSet.toArray(new Node[sortedNodesSet.size()]);

    for (int i = sortedNodes.length - 1; i >= 0; i--) {
      Node node = sortedNodes[i];
      if (node instanceof ParallelInstructionNode
          && ((ParallelInstructionNode) node).getParallelInstruction().getFlatten() != null
          && ((ParallelInstructionNode) node).getExecutionLocation()
              == ExecutionLocation.AMBIGUOUS) {
        cloneFlatten(node, network);
      }
    }

    return network;
  }

  /**
   * A helper function which performs the actual cloning procedure, which means creating the runner
   * and SDK versions of both the ambiguous flatten and its PCollection, attaching the old flatten's
   * predecessors and successors properly, and then removing the ambiguous flatten from the network.
   */
  private void cloneFlatten(Node flatten, MutableNetwork<Node, Edge> network) {
    // Start by creating the clones of the flatten and its PCollection.
    InstructionOutputNode flattenOut =
        (InstructionOutputNode) Iterables.getOnlyElement(network.successors(flatten));
    ParallelInstruction flattenInstruction =
        ((ParallelInstructionNode) flatten).getParallelInstruction();

    Node runnerFlatten =
        ParallelInstructionNode.create(flattenInstruction, ExecutionLocation.RUNNER_HARNESS);
    Node runnerFlattenOut =
        InstructionOutputNode.create(
            flattenOut.getInstructionOutput(), flattenOut.getPcollectionId());
    network.addNode(runnerFlatten);
    network.addNode(runnerFlattenOut);

    Node sdkFlatten =
        ParallelInstructionNode.create(flattenInstruction, ExecutionLocation.SDK_HARNESS);
    Node sdkFlattenOut =
        InstructionOutputNode.create(
            flattenOut.getInstructionOutput(), flattenOut.getPcollectionId());
    network.addNode(sdkFlatten);
    network.addNode(sdkFlattenOut);

    for (Edge edge : ImmutableList.copyOf(network.edgesConnecting(flatten, flattenOut))) {
      network.addEdge(runnerFlatten, runnerFlattenOut, edge.clone());
      network.addEdge(sdkFlatten, sdkFlattenOut, edge.clone());
    }

    // Copy over predecessor edges to both cloned nodes.
    for (Node predecessor : network.predecessors(flatten)) {
      for (Edge edge : ImmutableList.copyOf(network.edgesConnecting(predecessor, flatten))) {
        network.addEdge(predecessor, runnerFlatten, edge.clone());
        network.addEdge(predecessor, sdkFlatten, edge.clone());
      }
    }

    // Copy over successor edges depending on execution locations of successors.
    for (Node successor : network.successors(flattenOut)) {
      // Connect successor to SDK harness only if sure it executes in SDK.
      Node selectedOutput = executesInSdkHarness(successor) ? sdkFlattenOut : runnerFlattenOut;
      for (Edge edge : ImmutableList.copyOf(network.edgesConnecting(flattenOut, successor))) {
        network.addEdge(selectedOutput, successor, edge.clone());
      }
    }

    network.removeNode(flatten);
    network.removeNode(flattenOut);
  }

  /**
   * Returns true iff the given node is a {@link ParallelInstruction} which represents an
   * instruction that executes within the SDK harness. For details on how node locations are deduced
   * refer to {@link DeduceNodeLocationsFunction#executesInSdkHarness}.
   */
  private static boolean executesInSdkHarness(Node node) {
    return node instanceof ParallelInstructionNode
        && ((ParallelInstructionNode) node).getExecutionLocation()
            == Nodes.ExecutionLocation.SDK_HARNESS;
  }
}
