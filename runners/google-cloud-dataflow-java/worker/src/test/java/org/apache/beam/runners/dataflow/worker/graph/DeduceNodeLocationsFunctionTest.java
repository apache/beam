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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.api.services.dataflow.model.FlattenInstruction;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import com.google.api.services.dataflow.model.Source;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.worker.graph.Edges.DefaultEdge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ExecutionLocation;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Equivalence;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.ImmutableNetwork;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.Network;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.NetworkBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DeduceNodeLocationsFunction}. */
@RunWith(JUnit4.class)
public final class DeduceNodeLocationsFunctionTest {

  private static final String CUSTOM_SOURCE =
      "org.apache.beam.runners.dataflow.internal.CustomSources";
  private static final String RUNNER_SOURCE = "GroupingShuffleSource";
  private static final String DO_FN = "DoFn";

  private static final Equivalence<Node> NODE_EQUIVALENCE = NodeEquivalence.INSTANCE;

  private static final class NodeEquivalence extends Equivalence<Node> {
    static final NodeEquivalence INSTANCE = new NodeEquivalence();

    @Override
    protected boolean doEquivalent(Node a, Node b) {
      if (a instanceof ParallelInstructionNode && b instanceof ParallelInstructionNode) {
        ParallelInstruction contentsA = ((ParallelInstructionNode) a).getParallelInstruction();
        ParallelInstruction contentsB = ((ParallelInstructionNode) b).getParallelInstruction();
        return contentsA.equals(contentsB);
      } else {
        return a.equals(b); // Make sure non-deducible nodes haven't been modified.
      }
    }

    @Override
    protected int doHash(Node n) {
      return n.hashCode();
    }
  }

  @Test
  public void testEmptyNetwork() {
    assertEquals(
        createEmptyNetwork(), new DeduceNodeLocationsFunction().apply(createEmptyNetwork()));
  }

  @Test
  public void testSingleNodeWithSdkRead() throws Exception {
    Node unknown = createReadNode("Unknown", CUSTOM_SOURCE);

    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    network.addNode(unknown);

    Network<Node, Edge> inputNetwork = ImmutableNetwork.copyOf(network);
    network = new DeduceNodeLocationsFunction().apply(network);

    assertThatNetworksAreIdentical(inputNetwork, network);

    for (Node node : ImmutableList.copyOf(network.nodes())) {
      assertNodesIdenticalExceptForExecutionLocation(unknown, node);
      assertThatLocationIsProperlyDeduced(node, ExecutionLocation.SDK_HARNESS);
    }
  }

  @Test
  public void testSingleNodeWithRunnerRead() throws Exception {
    Node unknown = createReadNode("Unknown", RUNNER_SOURCE);

    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    network.addNode(unknown);

    Network<Node, Edge> inputNetwork = ImmutableNetwork.copyOf(network);
    network = new DeduceNodeLocationsFunction().apply(network);

    assertThatNetworksAreIdentical(inputNetwork, network);

    for (Node node : ImmutableList.copyOf(network.nodes())) {
      assertNodesIdenticalExceptForExecutionLocation(unknown, node);
      assertThatLocationIsProperlyDeduced(node, ExecutionLocation.RUNNER_HARNESS);
    }
  }

  @Test
  public void testSingleNodeWithSdkParDo() throws Exception {
    Node unknown = createParDoNode("Unknown", DO_FN);

    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    network.addNode(unknown);

    Network<Node, Edge> inputNetwork = ImmutableNetwork.copyOf(network);
    network = new DeduceNodeLocationsFunction().apply(network);

    assertThatNetworksAreIdentical(inputNetwork, network);

    for (Node node : ImmutableList.copyOf(network.nodes())) {
      assertNodesIdenticalExceptForExecutionLocation(unknown, node);
      assertThatLocationIsProperlyDeduced(node, ExecutionLocation.SDK_HARNESS);
    }
  }

  @Test
  public void testSingleNodeWithRunnerParDo() throws Exception {
    Node unknown = createParDoNode("Unknown", "RunnerDoFn");

    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    network.addNode(unknown);

    Network<Node, Edge> inputNetwork = ImmutableNetwork.copyOf(network);
    network = new DeduceNodeLocationsFunction().apply(network);

    assertThatNetworksAreIdentical(inputNetwork, network);

    for (Node node : ImmutableList.copyOf(network.nodes())) {
      assertNodesIdenticalExceptForExecutionLocation(unknown, node);
      assertThatLocationIsProperlyDeduced(node, ExecutionLocation.RUNNER_HARNESS);
    }
  }

  /** Tests that multiple deduced nodes with connecting edges are maintained correctly. */
  @Test
  public void testMultipleNodesDeduced() throws Exception {

    // A --\     /--> C
    //      -> E
    // B --/     \--> D
    Node a = createReadNode("A", CUSTOM_SOURCE);
    Node b = createReadNode("B", RUNNER_SOURCE);
    Node c = createParDoNode("C", "RunnerDoFn");
    Node d = createParDoNode("D", DO_FN);
    Node e = createParDoNode("E", DO_FN);

    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    network.addNode(a);
    network.addNode(b);
    network.addNode(c);
    network.addNode(d);
    network.addNode(e);
    network.addEdge(a, e, DefaultEdge.create());
    network.addEdge(b, e, DefaultEdge.create());
    network.addEdge(e, c, DefaultEdge.create());
    network.addEdge(e, d, DefaultEdge.create());

    Network<Node, Edge> inputNetwork = ImmutableNetwork.copyOf(network);
    network = new DeduceNodeLocationsFunction().apply(network);

    assertThatNetworksAreIdentical(inputNetwork, network);
    assertAllNodesDeducedExceptFlattens(network);
  }

  /** Tests that graphs with deducible and non-deducible nodes are maintained correctly. */
  @Test
  public void testGraphWithNonDeducibleNodes() throws Exception {

    // A --> out1 --\
    //               --> Flatten --> D
    // B --> out2 --/-->C
    Node a = createReadNode("A", CUSTOM_SOURCE);
    Node out1 = InstructionOutputNode.create(new InstructionOutput(), "fakeId");
    Node b = createReadNode("B", RUNNER_SOURCE);
    Node out2 = InstructionOutputNode.create(new InstructionOutput(), "fakeId");
    Node c = createParDoNode("C", "RunnerDoFn");
    Node flatten =
        ParallelInstructionNode.create(
            new ParallelInstruction().setName("Flatten").setFlatten(new FlattenInstruction()),
            Nodes.ExecutionLocation.UNKNOWN);
    Node d = createParDoNode("D", DO_FN);

    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    network.addNode(a);
    network.addNode(out1);
    network.addNode(b);
    network.addNode(out2);
    network.addNode(c);
    network.addNode(flatten);
    network.addNode(d);
    network.addEdge(a, out1, DefaultEdge.create());
    network.addEdge(b, out2, DefaultEdge.create());
    network.addEdge(out1, flatten, DefaultEdge.create());
    network.addEdge(out2, flatten, DefaultEdge.create());
    network.addEdge(out2, c, DefaultEdge.create());
    network.addEdge(flatten, d, DefaultEdge.create());

    Network<Node, Edge> inputNetwork = ImmutableNetwork.copyOf(network);
    network = new DeduceNodeLocationsFunction().apply(network);

    assertThatNetworksAreIdentical(inputNetwork, network);
    assertAllNodesDeducedExceptFlattens(network);
  }

  private static MutableNetwork<Node, Edge> createEmptyNetwork() {
    return NetworkBuilder.directed()
        .allowsSelfLoops(false)
        .allowsParallelEdges(true)
        .<Node, Edge>build();
  }

  private void assertThatLocationIsProperlyDeduced(Node node, ExecutionLocation expectedLocation) {
    assertThat(node, instanceOf(ParallelInstructionNode.class));

    ExecutionLocation location = ((ParallelInstructionNode) node).getExecutionLocation();
    assertEquals(location, expectedLocation);
  }

  /** Asserts two nodes are identical except for ExecutionLocation, which can differ. */
  private void assertNodesIdenticalExceptForExecutionLocation(Node expected, Node actual) {
    assertThat(expected, instanceOf(ParallelInstructionNode.class));
    assertThat(actual, instanceOf(ParallelInstructionNode.class));

    ParallelInstruction expectedContents =
        ((ParallelInstructionNode) expected).getParallelInstruction();
    ParallelInstruction actualContents =
        ((ParallelInstructionNode) actual).getParallelInstruction();
    assertEquals(expectedContents, actualContents);
  }

  /**
   * Asserts that the structure and nodes of two graphs are identical except for the deduced
   * ExecutionLocations, and that all paths through the graph still exist.
   */
  private void assertThatNetworksAreIdentical(
      Network<Node, Edge> oldNetwork, Network<Node, Edge> newNetwork) {
    // Assert that both networks still have same number of nodes and edges.
    assertEquals(oldNetwork.nodes().size(), newNetwork.nodes().size());
    assertEquals(oldNetwork.edges().size(), newNetwork.edges().size());

    // Assert that all paths still exist with identical (except for location) nodes in each path.
    List<List<Equivalence.Wrapper<Node>>> oldPaths = allPathsWithWrappedNodes(oldNetwork);
    List<List<Equivalence.Wrapper<Node>>> newPaths = allPathsWithWrappedNodes(newNetwork);
    assertThat(oldPaths, containsInAnyOrder(newPaths.toArray()));
  }

  private List<List<Equivalence.Wrapper<Node>>> allPathsWithWrappedNodes(
      Network<Node, Edge> network) {
    List<List<Node>> paths = Networks.allPathsFromRootsToLeaves(network);
    List<List<Equivalence.Wrapper<Node>>> wrappedPaths = new ArrayList<>();
    for (List<Node> path : paths) {
      List<Equivalence.Wrapper<Node>> wrappedPath = new ArrayList<>();
      for (Node node : path) {
        wrappedPath.add(NODE_EQUIVALENCE.wrap(node));
      }
      wrappedPaths.add(wrappedPath);
    }

    return wrappedPaths;
  }

  /**
   * Asserts that all {@link ParallelInstructionNode}s in a graph have had locations deduced except
   * for flattens which should remain undeduced.
   */
  private void assertAllNodesDeducedExceptFlattens(Network<Node, Edge> network) {
    for (Node node : network.nodes()) {
      if (node instanceof ParallelInstructionNode) {
        // Flattens should remain undeduced,
        if (((ParallelInstructionNode) node).getParallelInstruction().getFlatten() != null) {
          assertTrue(
              ((ParallelInstructionNode) node).getExecutionLocation() == ExecutionLocation.UNKNOWN);
        } else {
          assertTrue(
              ((ParallelInstructionNode) node).getExecutionLocation() != ExecutionLocation.UNKNOWN);
        }
      }
    }
  }

  private static ParallelInstructionNode createReadNode(String name, String readClassName) {
    return ParallelInstructionNode.create(
        new ParallelInstruction()
            .setName(name)
            .setRead(
                new ReadInstruction()
                    .setSource(new Source().setSpec(CloudObject.forClassName(readClassName)))),
        Nodes.ExecutionLocation.UNKNOWN);
  }

  private static ParallelInstructionNode createParDoNode(String name, String parDoClassName) {
    return ParallelInstructionNode.create(
        new ParallelInstruction()
            .setName(name)
            .setParDo(new ParDoInstruction().setUserFn(CloudObject.forClassName(parDoClassName))),
        Nodes.ExecutionLocation.UNKNOWN);
  }

  /** Creates a node already set to execute in the SDK harness. */
  private static ParallelInstructionNode createSdkNode(String name) {
    return ParallelInstructionNode.create(
        new ParallelInstruction().setName(name), Nodes.ExecutionLocation.SDK_HARNESS);
  }
}
