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
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.graph.Edges.DefaultEdge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.HappensBeforeEdge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.Graphs;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.Network;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.NetworkBuilder;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link CreateRegisterFnOperationFunction}. */
@RunWith(JUnit4.class)
public class CreateRegisterFnOperationFunctionTest {

  @Mock private Supplier<Node> portSupplier;
  @Mock private Function<MutableNetwork<Node, Edge>, Node> registerFnOperationFunction;
  private Function<MutableNetwork<Node, Edge>, MutableNetwork<Node, Edge>>
      createRegisterFnOperation;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    createRegisterFnOperation =
        new CreateRegisterFnOperationFunction(
            IdGenerators.decrementingLongs(), portSupplier, registerFnOperationFunction, false);
  }

  @Test
  public void testEmptyGraph() {
    MutableNetwork<Node, Edge> appliedNetwork =
        createRegisterFnOperation.apply(createEmptyNetwork());
    MutableNetwork<Node, Edge> expectedNetwork = createEmptyNetwork();

    assertNetworkMaintainsBipartiteStructure(appliedNetwork);
    assertEquals(
        String.format("Expected network %s but got network %s", expectedNetwork, appliedNetwork),
        expectedNetwork,
        appliedNetwork);
  }

  @Test
  public void testAllRunnerGraph() {
    Node readNode = createReadNode("Read", Nodes.ExecutionLocation.RUNNER_HARNESS);
    Edge readNodeEdge = DefaultEdge.create();
    Node readNodeOut = createInstructionOutputNode("Read.out");
    Edge readNodeOutEdge = DefaultEdge.create();
    Node parDoNode = createParDoNode("ParDo", Nodes.ExecutionLocation.RUNNER_HARNESS);
    Edge parDoNodeEdge = DefaultEdge.create();
    Node parDoNodeOut = createInstructionOutputNode("ParDo.out");

    // Read -out-> ParDo
    MutableNetwork<Node, Edge> expectedNetwork = createEmptyNetwork();
    expectedNetwork.addNode(readNode);
    expectedNetwork.addNode(readNodeOut);
    expectedNetwork.addNode(parDoNode);
    expectedNetwork.addNode(parDoNodeOut);
    expectedNetwork.addEdge(readNode, readNodeOut, readNodeEdge);
    expectedNetwork.addEdge(readNodeOut, parDoNode, readNodeOutEdge);
    expectedNetwork.addEdge(parDoNode, parDoNodeOut, parDoNodeEdge);

    MutableNetwork<Node, Edge> appliedNetwork =
        createRegisterFnOperation.apply(Graphs.copyOf(expectedNetwork));

    assertNetworkMaintainsBipartiteStructure(appliedNetwork);
    assertEquals(
        String.format("Expected network %s but got network %s", expectedNetwork, appliedNetwork),
        expectedNetwork,
        appliedNetwork);
  }

  @Test
  public void testAllSdkGraph() {
    Node sdkPortionNode = TestNode.create("SdkPortion");
    @SuppressWarnings({"unchecked", "rawtypes"})
    ArgumentCaptor<MutableNetwork<Node, Edge>> networkCapture =
        ArgumentCaptor.forClass((Class) MutableNetwork.class);

    when(registerFnOperationFunction.apply(networkCapture.capture())).thenReturn(sdkPortionNode);

    // Read -out-> ParDo
    Node readNode = createReadNode("Read", Nodes.ExecutionLocation.SDK_HARNESS);
    Edge readNodeEdge = DefaultEdge.create();
    Node readNodeOut = createInstructionOutputNode("Read.out");
    Edge readNodeOutEdge = DefaultEdge.create();
    Node parDoNode = createParDoNode("ParDo", Nodes.ExecutionLocation.SDK_HARNESS);
    Edge parDoNodeEdge = DefaultEdge.create();
    Node parDoNodeOut = createInstructionOutputNode("ParDo.out");

    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    network.addNode(readNode);
    network.addNode(readNodeOut);
    network.addNode(parDoNode);
    network.addNode(parDoNodeOut);
    network.addEdge(readNode, readNodeOut, readNodeEdge);
    network.addEdge(readNodeOut, parDoNode, readNodeOutEdge);
    network.addEdge(parDoNode, parDoNodeOut, parDoNodeEdge);

    MutableNetwork<Node, Edge> expectedNetwork = createEmptyNetwork();
    expectedNetwork.addNode(sdkPortionNode);

    MutableNetwork<Node, Edge> appliedNetwork =
        createRegisterFnOperation.apply(Graphs.copyOf(network));

    assertNetworkMaintainsBipartiteStructure(appliedNetwork);
    assertNetworkMaintainsBipartiteStructure(networkCapture.getValue());
    assertEquals(
        String.format("Expected network %s but got network %s", expectedNetwork, appliedNetwork),
        expectedNetwork,
        appliedNetwork);
    assertEquals(
        String.format("Expected network %s but got network %s", network, networkCapture.getValue()),
        network,
        networkCapture.getValue());
  }

  @Test
  public void testRunnerToSdkToRunnerGraph() {
    Node sdkPortion = TestNode.create("SdkPortion");
    @SuppressWarnings({"unchecked", "rawtypes"})
    ArgumentCaptor<MutableNetwork<Node, Edge>> networkCapture =
        ArgumentCaptor.forClass((Class) MutableNetwork.class);
    when(registerFnOperationFunction.apply(networkCapture.capture())).thenReturn(sdkPortion);

    Node firstPort = TestNode.create("FirstPort");
    Node secondPort = TestNode.create("SecondPort");
    when(portSupplier.get()).thenReturn(firstPort, secondPort);

    Node readNode = createReadNode("Read", Nodes.ExecutionLocation.RUNNER_HARNESS);
    Edge readNodeEdge = DefaultEdge.create();
    Node readNodeOut = createInstructionOutputNode("Read.out");
    Edge readNodeOutEdge = DefaultEdge.create();
    Node sdkParDoNode = createParDoNode("SdkParDo", Nodes.ExecutionLocation.SDK_HARNESS);
    Edge sdkParDoNodeEdge = DefaultEdge.create();
    Node sdkParDoNodeOut = createInstructionOutputNode("SdkParDo.out");
    Edge sdkParDoNodeOutEdge = DefaultEdge.create();
    Node runnerParDoNode = createParDoNode("RunnerParDo", Nodes.ExecutionLocation.RUNNER_HARNESS);
    Edge runnerParDoNodeEdge = DefaultEdge.create();
    Node runnerParDoNodeOut = createInstructionOutputNode("RunnerParDo.out");

    // Read -out-> SdkParDo -out-> RunnerParDo
    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    network.addNode(readNode);
    network.addNode(readNodeOut);
    network.addNode(sdkParDoNodeOut);
    network.addNode(sdkParDoNodeOut);
    network.addNode(runnerParDoNode);
    network.addNode(runnerParDoNodeOut);
    network.addEdge(readNode, readNodeOut, readNodeEdge);
    network.addEdge(readNodeOut, sdkParDoNode, readNodeOutEdge);
    network.addEdge(sdkParDoNode, sdkParDoNodeOut, sdkParDoNodeEdge);
    network.addEdge(sdkParDoNodeOut, runnerParDoNode, sdkParDoNodeOutEdge);
    network.addEdge(runnerParDoNode, runnerParDoNodeOut, runnerParDoNodeEdge);

    MutableNetwork<Node, Edge> appliedNetwork =
        createRegisterFnOperation.apply(Graphs.copyOf(network));
    assertNetworkMaintainsBipartiteStructure(appliedNetwork);

    // On each rewire between runner and SDK and vice versa, we use a new output node
    Node newOutA = Iterables.getOnlyElement(appliedNetwork.predecessors(firstPort));
    Node newOutB = Iterables.getOnlyElement(appliedNetwork.successors(secondPort));

    // readNode -newOutA-> firstPort --> sdkPortion --> secondPort -newOutB-> runnerParDoNode
    assertThat(
        appliedNetwork.nodes(),
        containsInAnyOrder(
            readNode,
            newOutA,
            firstPort,
            sdkPortion,
            secondPort,
            newOutB,
            runnerParDoNode,
            runnerParDoNodeOut));
    assertThat(appliedNetwork.successors(readNode), containsInAnyOrder(newOutA));
    assertThat(appliedNetwork.successors(newOutA), containsInAnyOrder(firstPort));
    assertThat(appliedNetwork.successors(firstPort), containsInAnyOrder(sdkPortion));
    assertThat(appliedNetwork.successors(sdkPortion), containsInAnyOrder(secondPort));
    assertThat(appliedNetwork.successors(secondPort), containsInAnyOrder(newOutB));
    assertThat(appliedNetwork.successors(newOutB), containsInAnyOrder(runnerParDoNode));
    assertThat(appliedNetwork.successors(runnerParDoNode), containsInAnyOrder(runnerParDoNodeOut));
    assertThat(
        appliedNetwork.edgesConnecting(firstPort, sdkPortion),
        everyItem(Matchers.<Edges.Edge>instanceOf(HappensBeforeEdge.class)));
    assertThat(
        appliedNetwork.edgesConnecting(sdkPortion, secondPort),
        everyItem(Matchers.<Edges.Edge>instanceOf(HappensBeforeEdge.class)));

    MutableNetwork<Node, Edge> sdkSubnetwork = networkCapture.getValue();
    assertNetworkMaintainsBipartiteStructure(sdkSubnetwork);

    Node sdkNewOutA = Iterables.getOnlyElement(sdkSubnetwork.successors(firstPort));
    Node sdkNewOutB = Iterables.getOnlyElement(sdkSubnetwork.predecessors(secondPort));

    // firstPort -sdkNewOutA-> sdkParDoNode -sdkNewOutB-> secondPort
    assertThat(
        sdkSubnetwork.nodes(),
        containsInAnyOrder(firstPort, sdkNewOutA, sdkParDoNode, sdkNewOutB, secondPort));
    assertThat(sdkSubnetwork.successors(firstPort), containsInAnyOrder(sdkNewOutA));
    assertThat(sdkSubnetwork.successors(sdkNewOutA), containsInAnyOrder(sdkParDoNode));
    assertThat(sdkSubnetwork.successors(sdkParDoNode), containsInAnyOrder(sdkNewOutB));
    assertThat(sdkSubnetwork.successors(sdkNewOutB), containsInAnyOrder(secondPort));
  }

  @Test
  public void testSdkToRunnerToSdkGraph() {
    Node firstSdkPortion = TestNode.create("FirstSdkPortion");
    Node secondSdkPortion = TestNode.create("SecondSdkPortion");
    @SuppressWarnings({"unchecked", "rawtypes"})
    ArgumentCaptor<MutableNetwork<Node, Edge>> networkCapture =
        ArgumentCaptor.forClass((Class) MutableNetwork.class);
    when(registerFnOperationFunction.apply(networkCapture.capture()))
        .thenReturn(firstSdkPortion, secondSdkPortion);

    Node firstPort = TestNode.create("FirstPort");
    Node secondPort = TestNode.create("SecondPort");
    when(portSupplier.get()).thenReturn(firstPort, secondPort);

    Node readNode = createReadNode("Read", Nodes.ExecutionLocation.SDK_HARNESS);
    Edge readNodeEdge = DefaultEdge.create();
    Node readNodeOut = createInstructionOutputNode("Read.out");
    Edge readNodeOutEdge = DefaultEdge.create();
    Node runnerParDoNode = createParDoNode("RunnerParDo", Nodes.ExecutionLocation.RUNNER_HARNESS);
    Edge runnerParDoNodeEdge = DefaultEdge.create();
    Node runnerParDoNodeOut = createInstructionOutputNode("RunnerParDo.out");
    Edge runnerParDoNodeOutEdge = DefaultEdge.create();
    Node sdkParDoNode = createParDoNode("SdkParDo", Nodes.ExecutionLocation.SDK_HARNESS);
    Edge sdkParDoNodeEdge = DefaultEdge.create();
    Node sdkParDoNodeOut = createInstructionOutputNode("SdkParDo.out");

    // Read -out-> RunnerParDo -out-> SdkParDo
    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    network.addNode(readNode);
    network.addNode(readNodeOut);
    network.addNode(runnerParDoNode);
    network.addNode(runnerParDoNodeOut);
    network.addNode(sdkParDoNodeOut);
    network.addNode(sdkParDoNodeOut);
    network.addEdge(readNode, readNodeOut, readNodeEdge);
    network.addEdge(readNodeOut, runnerParDoNode, readNodeOutEdge);
    network.addEdge(runnerParDoNode, runnerParDoNodeOut, runnerParDoNodeEdge);
    network.addEdge(runnerParDoNodeOut, sdkParDoNode, runnerParDoNodeOutEdge);
    network.addEdge(sdkParDoNode, sdkParDoNodeOut, sdkParDoNodeEdge);

    MutableNetwork<Node, Edge> appliedNetwork =
        createRegisterFnOperation.apply(Graphs.copyOf(network));
    assertNetworkMaintainsBipartiteStructure(appliedNetwork);

    // On each rewire between runner and SDK, we use a new output node
    Node newOutA = Iterables.getOnlyElement(appliedNetwork.successors(firstPort));
    Node newOutB = Iterables.getOnlyElement(appliedNetwork.predecessors(secondPort));

    // firstSdkPortion -> firstPort -newOutA-> RunnerParDo -newOutB-> secondPort -> secondSdkPortion
    assertThat(
        appliedNetwork.nodes(),
        containsInAnyOrder(
            firstSdkPortion,
            firstPort,
            newOutA,
            runnerParDoNode,
            newOutB,
            secondPort,
            secondSdkPortion));
    assertThat(appliedNetwork.successors(firstSdkPortion), containsInAnyOrder(firstPort));
    assertThat(appliedNetwork.successors(firstPort), containsInAnyOrder(newOutA));
    assertThat(appliedNetwork.successors(newOutA), containsInAnyOrder(runnerParDoNode));
    assertThat(appliedNetwork.successors(runnerParDoNode), containsInAnyOrder(newOutB));
    assertThat(appliedNetwork.successors(newOutB), containsInAnyOrder(secondPort));
    assertThat(appliedNetwork.successors(secondPort), containsInAnyOrder(secondSdkPortion));
    assertThat(
        appliedNetwork.edgesConnecting(firstSdkPortion, firstPort),
        everyItem(Matchers.<Edges.Edge>instanceOf(HappensBeforeEdge.class)));
    assertThat(
        appliedNetwork.edgesConnecting(secondPort, secondSdkPortion),
        everyItem(Matchers.<Edges.Edge>instanceOf(HappensBeforeEdge.class)));

    // The order of the calls to create the SDK subnetworks is indeterminate
    List<MutableNetwork<Node, Edge>> sdkSubnetworks = networkCapture.getAllValues();
    MutableNetwork<Node, Edge> firstSdkSubnetwork;
    MutableNetwork<Node, Edge> secondSdkSubnetwork;
    if (sdkSubnetworks.get(0).nodes().contains(readNode)) {
      firstSdkSubnetwork = sdkSubnetworks.get(0);
      secondSdkSubnetwork = sdkSubnetworks.get(1);
    } else {
      firstSdkSubnetwork = sdkSubnetworks.get(1);
      secondSdkSubnetwork = sdkSubnetworks.get(0);
    }
    assertNetworkMaintainsBipartiteStructure(firstSdkSubnetwork);
    assertNetworkMaintainsBipartiteStructure(secondSdkSubnetwork);

    Node sdkNewOutA = Iterables.getOnlyElement(firstSdkSubnetwork.predecessors(firstPort));

    // readNode -sdkNewOutA-> firstPort
    assertThat(firstSdkSubnetwork.nodes(), containsInAnyOrder(readNode, sdkNewOutA, firstPort));
    assertThat(firstSdkSubnetwork.successors(readNode), containsInAnyOrder(sdkNewOutA));
    assertThat(firstSdkSubnetwork.successors(sdkNewOutA), containsInAnyOrder(firstPort));

    Node sdkNewOutB = Iterables.getOnlyElement(secondSdkSubnetwork.successors(secondPort));

    // secondPort -sdkNewOutB-> sdkParDoNode -> sdkParDoNodeOut
    assertThat(
        secondSdkSubnetwork.nodes(),
        containsInAnyOrder(secondPort, sdkNewOutB, sdkParDoNode, sdkParDoNodeOut));
    assertThat(secondSdkSubnetwork.successors(secondPort), containsInAnyOrder(sdkNewOutB));
    assertThat(secondSdkSubnetwork.successors(sdkNewOutB), containsInAnyOrder(sdkParDoNode));
    assertThat(secondSdkSubnetwork.successors(sdkParDoNode), containsInAnyOrder(sdkParDoNodeOut));
  }

  @Test
  public void testRunnerAndSdkToRunnerAndSdkGraph() {
    // RunnerSource --\   /--> RunnerParDo
    //                 out
    // CustomSource --/   \--> SdkParDo
    //
    // Should produce:
    //        PortB --> out --\
    // RunnerSource --> out --> RunnerParDo
    //                     \--> PortA
    //        PortA --> out --\
    // CustomSource --> out --> SdkParDo
    //                     \--> PortB
    Node firstSdkPortion = TestNode.create("FirstSdkPortion");
    Node secondSdkPortion = TestNode.create("SecondSdkPortion");
    @SuppressWarnings({"unchecked", "rawtypes"})
    ArgumentCaptor<MutableNetwork<Node, Edge>> networkCapture =
        ArgumentCaptor.forClass((Class) MutableNetwork.class);
    when(registerFnOperationFunction.apply(networkCapture.capture()))
        .thenReturn(firstSdkPortion, secondSdkPortion);

    Node firstPort = TestNode.create("FirstPort");
    Node secondPort = TestNode.create("SecondPort");
    when(portSupplier.get()).thenReturn(firstPort, secondPort);

    Node runnerReadNode = createReadNode("RunnerRead", Nodes.ExecutionLocation.RUNNER_HARNESS);
    Edge runnerReadNodeEdge = DefaultEdge.create();
    Node sdkReadNode = createReadNode("SdkRead", Nodes.ExecutionLocation.SDK_HARNESS);
    Edge sdkReadNodeEdge = DefaultEdge.create();
    Node readNodeOut = createInstructionOutputNode("Read.out");
    Edge readNodeOutToRunnerEdge = DefaultEdge.create();
    Edge readNodeOutToSdkEdge = DefaultEdge.create();
    Node runnerParDoNode = createParDoNode("RunnerParDo", Nodes.ExecutionLocation.RUNNER_HARNESS);
    Edge runnerParDoNodeEdge = DefaultEdge.create();
    Node runnerParDoNodeOut = createInstructionOutputNode("RunnerParDo.out");
    Node sdkParDoNode = createParDoNode("SdkParDo", Nodes.ExecutionLocation.SDK_HARNESS);
    Edge sdkParDoNodeEdge = DefaultEdge.create();
    Node sdkParDoNodeOut = createInstructionOutputNode("SdkParDo.out");

    // Read -out-> RunnerParDo -out-> SdkParDo
    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    network.addNode(sdkReadNode);
    network.addNode(runnerReadNode);
    network.addNode(readNodeOut);
    network.addNode(runnerParDoNode);
    network.addNode(runnerParDoNodeOut);
    network.addNode(sdkParDoNodeOut);
    network.addNode(sdkParDoNodeOut);
    network.addEdge(sdkReadNode, readNodeOut, sdkReadNodeEdge);
    network.addEdge(runnerReadNode, readNodeOut, runnerReadNodeEdge);
    network.addEdge(readNodeOut, runnerParDoNode, readNodeOutToRunnerEdge);
    network.addEdge(readNodeOut, sdkParDoNode, readNodeOutToSdkEdge);
    network.addEdge(runnerParDoNode, runnerParDoNodeOut, runnerParDoNodeEdge);
    network.addEdge(sdkParDoNode, sdkParDoNodeOut, sdkParDoNodeEdge);

    MutableNetwork<Node, Edge> appliedNetwork =
        createRegisterFnOperation.apply(Graphs.copyOf(network));
    assertNetworkMaintainsBipartiteStructure(appliedNetwork);

    // Node wiring is indeterministic, must be detected from generated graph.
    Node sdkPortionA;
    Node sdkPortionB;
    if (appliedNetwork.inDegree(firstSdkPortion) == 0) {
      sdkPortionA = firstSdkPortion;
      sdkPortionB = secondSdkPortion;
    } else {
      sdkPortionA = secondSdkPortion;
      sdkPortionB = firstSdkPortion;
    }
    Node portA = Iterables.getOnlyElement(appliedNetwork.successors(sdkPortionA));
    Node portB = Iterables.getOnlyElement(appliedNetwork.predecessors(sdkPortionB));
    // On each rewire between runner and SDK, we use a new output node
    Node newOutA = Iterables.getOnlyElement(appliedNetwork.successors(portA));
    Node newOutB = Iterables.getOnlyElement(appliedNetwork.predecessors(portB));

    // sdkPortionA -> portA -newOutA-> runnerParDoNode -> runnerParDoNodeOut
    //       runnerReadNode -newOutB-/
    //                               \--> portB -> sdkPortionB
    assertThat(
        appliedNetwork.nodes(),
        containsInAnyOrder(
            runnerReadNode,
            firstSdkPortion,
            secondSdkPortion,
            portA,
            newOutA,
            portB,
            newOutB,
            runnerParDoNode,
            runnerParDoNodeOut));
    assertThat(appliedNetwork.successors(runnerReadNode), containsInAnyOrder(newOutB));
    assertThat(appliedNetwork.successors(newOutB), containsInAnyOrder(runnerParDoNode, portB));
    assertThat(appliedNetwork.successors(portB), containsInAnyOrder(sdkPortionB));
    assertThat(appliedNetwork.successors(sdkPortionA), containsInAnyOrder(portA));
    assertThat(appliedNetwork.successors(portA), containsInAnyOrder(newOutA));
    assertThat(appliedNetwork.successors(newOutA), containsInAnyOrder(runnerParDoNode));
    assertThat(appliedNetwork.successors(runnerParDoNode), containsInAnyOrder(runnerParDoNodeOut));
    assertThat(
        appliedNetwork.edgesConnecting(sdkPortionA, portA),
        everyItem(Matchers.<Edges.Edge>instanceOf(HappensBeforeEdge.class)));
    assertThat(
        appliedNetwork.edgesConnecting(portB, sdkPortionB),
        everyItem(Matchers.<Edges.Edge>instanceOf(HappensBeforeEdge.class)));

    // Argument captor call order can be indeterministic
    List<MutableNetwork<Node, Edge>> sdkSubnetworks = networkCapture.getAllValues();
    MutableNetwork<Node, Edge> sdkSubnetworkA;
    MutableNetwork<Node, Edge> sdkSubnetworkB;
    if (sdkSubnetworks.get(0).nodes().contains(sdkReadNode)) {
      sdkSubnetworkA = sdkSubnetworks.get(0);
      sdkSubnetworkB = sdkSubnetworks.get(1);
    } else {
      sdkSubnetworkA = sdkSubnetworks.get(1);
      sdkSubnetworkB = sdkSubnetworks.get(0);
    }
    assertNetworkMaintainsBipartiteStructure(sdkSubnetworkA);
    assertNetworkMaintainsBipartiteStructure(sdkSubnetworkB);

    //                       /-> portA
    // sdkReadNode -sdkNewOutA-> sdkParDoNode -> sdkParDoNodeOut
    Node sdkNewOutA = Iterables.getOnlyElement(sdkSubnetworkA.predecessors(portA));
    assertThat(
        sdkSubnetworkA.nodes(),
        containsInAnyOrder(sdkReadNode, portA, sdkNewOutA, sdkParDoNode, sdkParDoNodeOut));
    assertThat(sdkSubnetworkA.successors(sdkReadNode), containsInAnyOrder(sdkNewOutA));
    assertThat(sdkSubnetworkA.successors(sdkNewOutA), containsInAnyOrder(portA, sdkParDoNode));
    assertThat(sdkSubnetworkA.successors(sdkParDoNode), containsInAnyOrder(sdkParDoNodeOut));

    // portB -sdkNewOutB-> sdkParDoNode -> sdkParDoNodeOut
    Node sdkNewOutB = Iterables.getOnlyElement(sdkSubnetworkB.successors(portB));
    assertThat(
        sdkSubnetworkB.nodes(),
        containsInAnyOrder(portB, sdkNewOutB, sdkParDoNode, sdkParDoNodeOut));
    assertThat(sdkSubnetworkB.successors(portB), containsInAnyOrder(sdkNewOutB));
    assertThat(sdkSubnetworkB.successors(sdkNewOutB), containsInAnyOrder(sdkParDoNode));
    assertThat(sdkSubnetworkB.successors(sdkParDoNode), containsInAnyOrder(sdkParDoNodeOut));
  }

  /**
   * This asserts that all root nodes are not InstructionOutputNodes and that there is a bipartite
   * relationship between non-InstructionOutputNodes and InstructionOutputNodes, ignoring
   * HappensBeforeEdges.
   */
  private static void assertNetworkMaintainsBipartiteStructure(
      Network<Node, Edge> originalNetwork) {
    // Copy the network before we remove happens before relationships.
    MutableNetwork<Node, Edge> network = Graphs.copyOf(originalNetwork);
    for (HappensBeforeEdge edge :
        ImmutableList.copyOf(Iterables.filter(network.edges(), HappensBeforeEdge.class))) {
      network.removeEdge(edge);
    }

    for (Node node : network.nodes()) {
      if (node instanceof InstructionOutputNode) {
        assertTrue(network.inDegree(node) > 0);
        for (Node adjacentNode : network.adjacentNodes(node)) {
          assertThat(adjacentNode, not(instanceOf(InstructionOutputNode.class)));
        }
      } else {
        for (Node adjacentNode : network.adjacentNodes(node)) {
          assertThat(adjacentNode, instanceOf(InstructionOutputNode.class));
        }
      }
    }
  }

  private static MutableNetwork<Node, Edge> createEmptyNetwork() {
    return NetworkBuilder.directed()
        .allowsSelfLoops(false)
        .allowsParallelEdges(true)
        .<Node, Edge>build();
  }

  private static ParallelInstructionNode createReadNode(
      String name, Nodes.ExecutionLocation location) {
    return ParallelInstructionNode.create(
        new ParallelInstruction().setName(name).setRead(new ReadInstruction()), location);
  }

  private static ParallelInstructionNode createParDoNode(
      String name, Nodes.ExecutionLocation location) {
    return ParallelInstructionNode.create(
        new ParallelInstruction().setName(name).setParDo(new ParDoInstruction()), location);
  }

  private static InstructionOutputNode createInstructionOutputNode(String name) {
    return InstructionOutputNode.create(new InstructionOutput().setName(name), "fakeId");
  }

  /** A named node to easily differentiate graph construction problems during testing. */
  @AutoValue
  public abstract static class TestNode extends Node {
    public static TestNode create(String value) {
      return new AutoValue_CreateRegisterFnOperationFunctionTest_TestNode(value);
    }

    public abstract String getName();

    @Override
    public String toString() {
      return hashCode() + " " + getName();
    }
  }
}
