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

import static org.junit.Assert.assertEquals;

import com.google.api.services.dataflow.model.FlattenInstruction;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.ParallelInstruction;
import org.apache.beam.runners.dataflow.worker.graph.Edges.DefaultEdge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ExecutionLocation;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.Network;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.NetworkBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link DeduceFlattenLocationsFunction}. Certain tests are based on the table described
 * in {@link DeduceFlattenLocationsFunction}.
 */
@RunWith(JUnit4.class)
public final class DeduceFlattenLocationsFunctionTest {

  @Test
  public void testEmptyNetwork() throws Exception {
    assertEquals(
        createEmptyNetwork(), new DeduceFlattenLocationsFunction().apply(createEmptyNetwork()));
  }

  /*
   * In the following tests, the desired results should match the table described in {@link
   * DeduceFlattenLocationsFunction}.
   */

  @Test
  public void testDeductionFromSdkToSdk() throws Exception {
    // sdk_predecessor --> flatten --> pcollection --> sdk_successor
    assertSingleFlattenLocationDeduction(
        ExecutionLocation.SDK_HARNESS,
        ExecutionLocation.SDK_HARNESS,
        ExecutionLocation.SDK_HARNESS);
  }

  @Test
  public void testDeductionFromSdkToRunner() throws Exception {
    // sdk_predecessor --> flatten --> pcollection --> runner_successor
    assertSingleFlattenLocationDeduction(
        ExecutionLocation.SDK_HARNESS,
        ExecutionLocation.RUNNER_HARNESS,
        ExecutionLocation.RUNNER_HARNESS);
  }

  @Test
  public void testDeductionFromSdkToBoth() throws Exception {
    // sdk_predecessor --> flatten --> pcollection --> sdk_successor
    //                                             \-> runner_successor
    assertSingleFlattenLocationDeduction(
        ExecutionLocation.SDK_HARNESS, ExecutionLocation.AMBIGUOUS, ExecutionLocation.SDK_HARNESS);
  }

  @Test
  public void testDeductionFromSdkToNeither() throws Exception {
    // sdk_predecessor --> flatten --> pcollection
    assertSingleFlattenLocationDeduction(
        ExecutionLocation.SDK_HARNESS, ExecutionLocation.UNKNOWN, ExecutionLocation.SDK_HARNESS);
  }

  @Test
  public void testDeductionFromRunnerToSdk() throws Exception {
    // runner_predecessor --> flatten --> pcollection --> sdk_successor
    assertSingleFlattenLocationDeduction(
        ExecutionLocation.RUNNER_HARNESS,
        ExecutionLocation.SDK_HARNESS,
        ExecutionLocation.RUNNER_HARNESS);
  }

  @Test
  public void testDeductionFromRunnerToRunner() throws Exception {
    // sdk_predecessor --> flatten --> pcollection --> runner_successor
    assertSingleFlattenLocationDeduction(
        ExecutionLocation.RUNNER_HARNESS,
        ExecutionLocation.RUNNER_HARNESS,
        ExecutionLocation.RUNNER_HARNESS);
  }

  @Test
  public void testDeductionFromRunnerToBoth() throws Exception {
    // runner_predecessor --> flatten --> pcollection --> sdk_successor
    //                                                \-> runner_successor
    assertSingleFlattenLocationDeduction(
        ExecutionLocation.RUNNER_HARNESS,
        ExecutionLocation.AMBIGUOUS,
        ExecutionLocation.RUNNER_HARNESS);
  }

  @Test
  public void testDeductionFromRunnerToNeither() throws Exception {
    // runner_predecessor --> flatten --> pcollection
    assertSingleFlattenLocationDeduction(
        ExecutionLocation.RUNNER_HARNESS,
        ExecutionLocation.UNKNOWN,
        ExecutionLocation.RUNNER_HARNESS);
  }

  @Test
  public void testDeductionFromBothToSdk() throws Exception {
    // sdk_predecessor ----> flatten --> pcollection --> sdk_successor
    // runner_predecessor -/
    assertSingleFlattenLocationDeduction(
        ExecutionLocation.AMBIGUOUS, ExecutionLocation.SDK_HARNESS, ExecutionLocation.SDK_HARNESS);
  }

  @Test
  public void testDeductionFromBothToRunner() throws Exception {
    // sdk_predecessor ----> flatten --> pcollection --> runner_successor
    // runner_predecessor -/
    assertSingleFlattenLocationDeduction(
        ExecutionLocation.AMBIGUOUS,
        ExecutionLocation.RUNNER_HARNESS,
        ExecutionLocation.RUNNER_HARNESS);
  }

  @Test
  public void testDeductionFromBothToBoth() throws Exception {
    // sdk_predecessor ----> flatten --> pcollection --> sdk_successor
    // runner_predecessor -/                         \-> runner_successor
    assertSingleFlattenLocationDeduction(
        ExecutionLocation.AMBIGUOUS, ExecutionLocation.AMBIGUOUS, ExecutionLocation.AMBIGUOUS);
  }

  @Test
  public void testDeductionFromBothToNeither() throws Exception {
    // sdk_predecessor ----> flatten --> pcollection
    // runner_predecessor -/
    assertSingleFlattenLocationDeduction(
        ExecutionLocation.AMBIGUOUS, ExecutionLocation.UNKNOWN, ExecutionLocation.RUNNER_HARNESS);
  }

  @Test
  public void testDeductionFromNeitherToSdk() throws Exception {
    // flatten --> pcollection --> sdk_successor
    assertSingleFlattenLocationDeduction(
        ExecutionLocation.UNKNOWN, ExecutionLocation.SDK_HARNESS, ExecutionLocation.SDK_HARNESS);
  }

  @Test
  public void testDeductionFromNeitherToRunner() throws Exception {
    // flatten --> pcollection --> runner_successor
    assertSingleFlattenLocationDeduction(
        ExecutionLocation.UNKNOWN,
        ExecutionLocation.RUNNER_HARNESS,
        ExecutionLocation.RUNNER_HARNESS);
  }

  @Test
  public void testDeductionFromNeitherToBoth() throws Exception {
    // flatten --> pcollection --> sdk_successor
    //                         \-> runner_successor
    assertSingleFlattenLocationDeduction(
        ExecutionLocation.UNKNOWN, ExecutionLocation.AMBIGUOUS, ExecutionLocation.RUNNER_HARNESS);
  }

  @Test
  public void testDeductionFromNeitherToNeither() throws Exception {
    // flatten --> pcollection
    //
    assertSingleFlattenLocationDeduction(
        ExecutionLocation.UNKNOWN, ExecutionLocation.UNKNOWN, ExecutionLocation.RUNNER_HARNESS);
  }

  /** Test that when multiple flattens with PCollections are connected, they are deduced. */
  @Test
  public void testDeductionOfChainedFlattens() throws Exception {
    // sdk_node1 --> out --\
    // sdk_node2 --> out --> flatten1 --> out ----\                /-> sdk_node3 --> out
    //                                             flatten3 --> out
    // runner_node1 --> out --> flatten2 --> out -/                \-> runner_node3 --> out
    // runner_node2 --> out --/
    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    Node sdkNode1 = createSdkNode("sdk_node1");
    Node sdkNode1Output = createPCollection("sdk_node1.out");
    Node sdkNode2 = createSdkNode("sdk_node2");
    Node sdkNode2Output = createPCollection("sdk_node2.out");
    Node sdkNode3 = createSdkNode("sdk_node3");
    Node sdkNode3Output = createPCollection("sdk_node3.out");
    Node runnerNode1 = createRunnerNode("runner_node1");
    Node runnerNode1Output = createPCollection("runner_node1.out");
    Node runnerNode2 = createRunnerNode("runner_node2");
    Node runnerNode2Output = createPCollection("runner_node2.out");
    Node runnerNode3 = createRunnerNode("runner_node3");
    Node runnerNode3Output = createPCollection("runner_node3.out");
    Node flatten1 = createFlatten("flatten1");
    Node flatten1Output = createPCollection("flatten1.out");
    Node flatten2 = createFlatten("flatten2");
    Node flatten2Output = createPCollection("flatten2.out");
    Node flatten3 = createFlatten("flatten3");
    Node flatten3Output = createPCollection("flatten3.out");

    network.addNode(sdkNode1);
    network.addNode(sdkNode2);
    network.addNode(sdkNode3);
    network.addNode(runnerNode1);
    network.addNode(runnerNode2);
    network.addNode(runnerNode3);
    network.addNode(flatten1);
    network.addNode(flatten1Output);
    network.addNode(flatten2);
    network.addNode(flatten2Output);
    network.addNode(flatten3);
    network.addNode(flatten3Output);

    network.addEdge(sdkNode1, sdkNode1Output, DefaultEdge.create());
    network.addEdge(sdkNode2, sdkNode2Output, DefaultEdge.create());
    network.addEdge(runnerNode1, runnerNode1Output, DefaultEdge.create());
    network.addEdge(runnerNode2, runnerNode2Output, DefaultEdge.create());
    network.addEdge(sdkNode1Output, flatten1, DefaultEdge.create());
    network.addEdge(sdkNode2Output, flatten1, DefaultEdge.create());
    network.addEdge(runnerNode1Output, flatten2, DefaultEdge.create());
    network.addEdge(runnerNode2Output, flatten2, DefaultEdge.create());
    network.addEdge(flatten1, flatten1Output, DefaultEdge.create());
    network.addEdge(flatten2, flatten2Output, DefaultEdge.create());
    network.addEdge(flatten1Output, flatten3, DefaultEdge.create());
    network.addEdge(flatten2Output, flatten3, DefaultEdge.create());
    network.addEdge(flatten3, flatten3Output, DefaultEdge.create());
    network.addEdge(flatten3Output, sdkNode3, DefaultEdge.create());
    network.addEdge(flatten3Output, runnerNode3, DefaultEdge.create());
    network.addEdge(sdkNode3, sdkNode3Output, DefaultEdge.create());
    network.addEdge(runnerNode3, runnerNode3Output, DefaultEdge.create());

    network = new DeduceFlattenLocationsFunction().apply(network);

    ExecutionLocation flatten1Location = getExecutionLocationOf("flatten1", network);
    assertEquals(flatten1Location, ExecutionLocation.SDK_HARNESS);

    ExecutionLocation flatten2Location = getExecutionLocationOf("flatten2", network);
    assertEquals(flatten2Location, ExecutionLocation.RUNNER_HARNESS);

    ExecutionLocation flatten3Location = getExecutionLocationOf("flatten3", network);
    assertEquals(flatten3Location, ExecutionLocation.AMBIGUOUS);
  }

  private static MutableNetwork<Node, Edge> createEmptyNetwork() {
    return NetworkBuilder.directed()
        .allowsSelfLoops(false)
        .allowsParallelEdges(true)
        .<Node, Edge>build();
  }

  /**
   * For testing deducing the location of a single flatten. This function checks that a flatten with
   * the given aggregated locations for predecessors and successors deduces to the expected {@code
   * ExecutionLocation}.
   */
  private static void assertSingleFlattenLocationDeduction(
      ExecutionLocation predecessorLocations,
      ExecutionLocation successorLocations,
      ExecutionLocation expectedLocation)
      throws Exception {
    MutableNetwork<Node, Edge> network =
        createSingleFlattenNetwork(predecessorLocations, successorLocations);
    network = new DeduceFlattenLocationsFunction().apply(network);

    ExecutionLocation flattenLocation = getExecutionLocationOf("flatten", network);
    assertEquals(expectedLocation, flattenLocation);
  }

  /**
   * In order to test the result of deducing a single flatten's result, this returns a network of a
   * single flatten with a PCollection, with predecessors and successors with specified {@link
   * ExecutionLocation}s. A location of {@code AMBIGUOUS} passed as a parameter for this function
   * indicates to include both predecessors/successors while a location of {@code UNKNOWN} passed as
   * a parameter indicates to include no predecessors/successors.
   *
   * <p>This function promises that the single flatten node will be named "flatten" and that the
   * network will be structured as follows:
   *
   * <pre>{@code
   * sdk_node --> out -----\                          /--> sdk_node --> out
   *                        -> flatten --> pcollection
   * runner_node --> out --/                          \--> runner_node --> out
   * }</pre>
   *
   * <p>With the possibility of one or both predecessor/successor being omitted depending on the
   * parameters.
   */
  private static MutableNetwork<Node, Edge> createSingleFlattenNetwork(
      ExecutionLocation predecessorLocations, ExecutionLocation successorLocations)
      throws Exception {
    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    Node flatten = createFlatten("flatten");
    Node flattenOutput = createPCollection("pcollection");
    network.addNode(flatten);
    network.addNode(flattenOutput);
    network.addEdge(flatten, flattenOutput, DefaultEdge.create());

    if (predecessorLocations == ExecutionLocation.SDK_HARNESS
        || predecessorLocations == ExecutionLocation.AMBIGUOUS) {
      Node node = createSdkNode("sdk_predecessor");
      Node out = createPCollection("sdk_predecessor.out");
      network.addNode(node);
      network.addNode(out);
      network.addEdge(node, out, DefaultEdge.create());
      network.addEdge(out, flatten, DefaultEdge.create());
    }
    if (predecessorLocations == ExecutionLocation.RUNNER_HARNESS
        || predecessorLocations == ExecutionLocation.AMBIGUOUS) {
      Node node = createRunnerNode("runner_predecessor");
      Node out = createPCollection("runner_predecessor.out");
      network.addNode(node);
      network.addNode(out);
      network.addEdge(node, out, DefaultEdge.create());
      network.addEdge(out, flatten, DefaultEdge.create());
    }

    if (successorLocations == ExecutionLocation.SDK_HARNESS
        || successorLocations == ExecutionLocation.AMBIGUOUS) {
      Node node = createSdkNode("sdk_successor");
      Node out = createPCollection("sdk_successor.out");
      network.addNode(node);
      network.addNode(out);
      network.addEdge(flatten, node, DefaultEdge.create());
      network.addEdge(node, out, DefaultEdge.create());
    }
    if (successorLocations == ExecutionLocation.RUNNER_HARNESS
        || successorLocations == ExecutionLocation.AMBIGUOUS) {
      Node node = createRunnerNode("runner_successor");
      Node out = createPCollection("runner_successor.out");
      network.addNode(node);
      network.addNode(out);
      network.addEdge(flatten, node, DefaultEdge.create());
      network.addEdge(node, out, DefaultEdge.create());
    }

    return network;
  }

  /** Creates a node set to execute in the SDK harness. */
  private static ParallelInstructionNode createSdkNode(String name) {
    return ParallelInstructionNode.create(
        new ParallelInstruction().setName(name), Nodes.ExecutionLocation.SDK_HARNESS);
  }

  /** Creates a node set to execute in the SDK harness. */
  private static ParallelInstructionNode createRunnerNode(String name) {
    return ParallelInstructionNode.create(
        new ParallelInstruction().setName(name), Nodes.ExecutionLocation.RUNNER_HARNESS);
  }

  /** Creates a flatten node with no location set. */
  private static ParallelInstructionNode createFlatten(String name) {
    return ParallelInstructionNode.create(
        new ParallelInstruction().setFlatten(new FlattenInstruction()).setName(name),
        Nodes.ExecutionLocation.UNKNOWN);
  }

  /** Creates an {@link InstructionOutputNode} to act as a PCollection. */
  private static InstructionOutputNode createPCollection(String name) {
    return InstructionOutputNode.create(new InstructionOutput().setName(name), "fakeID");
  }

  private static ExecutionLocation getExecutionLocationOf(
      String nodeName, Network<Node, Edge> network) throws Exception {
    for (Node node : ImmutableList.copyOf(network.nodes())) {
      if (node instanceof ParallelInstructionNode
          && nodeName.equals(((ParallelInstructionNode) node).getParallelInstruction().getName())) {
        return ((ParallelInstructionNode) node).getExecutionLocation();
      }
    }

    throw new Exception("Node with name " + nodeName + " not found in network.");
  }
}
