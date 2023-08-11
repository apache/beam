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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsEmptyCollection.emptyCollectionOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.api.services.dataflow.model.FlattenInstruction;
import com.google.api.services.dataflow.model.InstructionInput;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.PartialGroupByKeyInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import com.google.api.services.dataflow.model.WriteInstruction;
import java.util.Arrays;
import org.apache.beam.runners.dataflow.worker.graph.Edges.DefaultEdge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.MultiOutputInfoEdge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.Network;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MapTaskToNetworkFunction}. */
@RunWith(JUnit4.class)
public class MapTaskToNetworkFunctionTest {
  @Test
  public void testEmptyMapTask() {
    Network<Node, Edge> network =
        new MapTaskToNetworkFunction(IdGenerators.decrementingLongs()).apply(new MapTask());
    assertTrue(network.isDirected());
    assertTrue(network.allowsParallelEdges());
    assertFalse(network.allowsSelfLoops());
    assertThat(network.nodes(), emptyCollectionOf(Node.class));
  }

  @Test
  public void testRead() {
    InstructionOutput readOutput = createInstructionOutput("Read.out");
    ParallelInstruction read = createParallelInstruction("Read", readOutput);
    read.setRead(new ReadInstruction());

    MapTask mapTask = new MapTask();
    mapTask.setInstructions(ImmutableList.of(read));
    mapTask.setFactory(Transport.getJsonFactory());

    Network<Node, Edge> network =
        new MapTaskToNetworkFunction(IdGenerators.decrementingLongs()).apply(mapTask);
    assertNetworkProperties(network);
    assertEquals(2, network.nodes().size());
    assertEquals(1, network.edges().size());

    ParallelInstructionNode readNode = get(network, read);
    InstructionOutputNode readOutputNode = getOnlySuccessor(network, readNode);
    assertEquals(readOutput, readOutputNode.getInstructionOutput());
  }

  @Test
  public void testParDo() {
    InstructionOutput readOutput = createInstructionOutput("Read.out");
    ParallelInstruction read = createParallelInstruction("Read", readOutput);
    read.setRead(new ReadInstruction());

    MultiOutputInfo parDoMultiOutput = createMultiOutputInfo("output");
    ParDoInstruction parDoInstruction = new ParDoInstruction();
    parDoInstruction.setInput(createInstructionInput(0, 0)); // Read.out
    parDoInstruction.setMultiOutputInfos(ImmutableList.of(parDoMultiOutput));
    InstructionOutput parDoOutput = createInstructionOutput("ParDo.out");
    ParallelInstruction parDo = createParallelInstruction("ParDo", parDoOutput);
    parDo.setParDo(parDoInstruction);

    MapTask mapTask = new MapTask();
    mapTask.setInstructions(ImmutableList.of(read, parDo));
    mapTask.setFactory(Transport.getJsonFactory());

    Network<Node, Edge> network =
        new MapTaskToNetworkFunction(IdGenerators.decrementingLongs()).apply(mapTask);
    assertNetworkProperties(network);
    assertEquals(4, network.nodes().size());
    assertEquals(3, network.edges().size());

    ParallelInstructionNode readNode = get(network, read);
    InstructionOutputNode readOutputNode = getOnlySuccessor(network, readNode);
    assertEquals(readOutput, readOutputNode.getInstructionOutput());

    ParallelInstructionNode parDoNode = getOnlySuccessor(network, readOutputNode);
    InstructionOutputNode parDoOutputNode = getOnlySuccessor(network, parDoNode);
    assertEquals(parDoOutput, parDoOutputNode.getInstructionOutput());

    assertEquals(
        parDoMultiOutput,
        ((MultiOutputInfoEdge)
                Iterables.getOnlyElement(network.edgesConnecting(parDoNode, parDoOutputNode)))
            .getMultiOutputInfo());
  }

  @Test
  public void testFlatten() {
    // ReadA --\
    //          |--> Flatten
    // ReadB --/
    InstructionOutput readOutputA = createInstructionOutput("ReadA.out");
    ParallelInstruction readA = createParallelInstruction("ReadA", readOutputA);
    readA.setRead(new ReadInstruction());

    InstructionOutput readOutputB = createInstructionOutput("ReadB.out");
    ParallelInstruction readB = createParallelInstruction("ReadB", readOutputB);
    readB.setRead(new ReadInstruction());

    FlattenInstruction flattenInstruction = new FlattenInstruction();
    flattenInstruction.setInputs(
        ImmutableList.of(
            createInstructionInput(0, 0), // ReadA.out
            createInstructionInput(1, 0))); // ReadB.out
    InstructionOutput flattenOutput = createInstructionOutput("Flatten.out");
    ParallelInstruction flatten = createParallelInstruction("Flatten", flattenOutput);
    flatten.setFlatten(flattenInstruction);

    MapTask mapTask = new MapTask();
    mapTask.setInstructions(ImmutableList.of(readA, readB, flatten));
    mapTask.setFactory(Transport.getJsonFactory());

    Network<Node, Edge> network =
        new MapTaskToNetworkFunction(IdGenerators.decrementingLongs()).apply(mapTask);
    assertNetworkProperties(network);
    assertEquals(6, network.nodes().size());
    assertEquals(5, network.edges().size());

    ParallelInstructionNode readANode = get(network, readA);
    InstructionOutputNode readOutputANode = getOnlySuccessor(network, readANode);
    assertEquals(readOutputA, readOutputANode.getInstructionOutput());

    ParallelInstructionNode readBNode = get(network, readB);
    InstructionOutputNode readOutputBNode = getOnlySuccessor(network, readBNode);
    assertEquals(readOutputB, readOutputBNode.getInstructionOutput());

    // Make sure the successors for both ReadA and ReadB output PCollections are the same
    assertEquals(network.successors(readOutputANode), network.successors(readOutputBNode));

    ParallelInstructionNode flattenNode = getOnlySuccessor(network, readOutputANode);
    InstructionOutputNode flattenOutputNode = getOnlySuccessor(network, flattenNode);
    assertEquals(flattenOutput, flattenOutputNode.getInstructionOutput());
  }

  @Test
  public void testParallelEdgeFlatten() {
    //                  /---\
    // Read --> Read.out --> Flatten
    //                  \---/
    InstructionOutput readOutput = createInstructionOutput("Read.out");
    ParallelInstruction read = createParallelInstruction("Read", readOutput);
    read.setRead(new ReadInstruction());

    FlattenInstruction flattenInstruction = new FlattenInstruction();
    flattenInstruction.setInputs(
        ImmutableList.of(
            createInstructionInput(0, 0), // Read.out
            createInstructionInput(0, 0), // Read.out
            createInstructionInput(0, 0))); // Read.out
    InstructionOutput flattenOutput = createInstructionOutput("Flatten.out");
    ParallelInstruction flatten = createParallelInstruction("Flatten", flattenOutput);
    flatten.setFlatten(flattenInstruction);

    MapTask mapTask = new MapTask();
    mapTask.setInstructions(ImmutableList.of(read, flatten));
    mapTask.setFactory(Transport.getJsonFactory());

    Network<Node, Edge> network =
        new MapTaskToNetworkFunction(IdGenerators.decrementingLongs()).apply(mapTask);
    assertNetworkProperties(network);
    assertEquals(4, network.nodes().size());
    assertEquals(5, network.edges().size());

    ParallelInstructionNode readNode = get(network, read);
    InstructionOutputNode readOutputNode = getOnlySuccessor(network, readNode);
    assertEquals(readOutput, readOutputNode.getInstructionOutput());

    ParallelInstructionNode flattenNode = getOnlySuccessor(network, readOutputNode);
    // Assert that the three parallel edges are maintained
    assertEquals(3, network.edgesConnecting(readOutputNode, flattenNode).size());

    InstructionOutputNode flattenOutputNode = getOnlySuccessor(network, flattenNode);
    assertEquals(flattenOutput, flattenOutputNode.getInstructionOutput());
  }

  @Test
  public void testWrite() {
    InstructionOutput readOutput = createInstructionOutput("Read.out");
    ParallelInstruction read = createParallelInstruction("Read", readOutput);
    read.setRead(new ReadInstruction());

    WriteInstruction writeInstruction = new WriteInstruction();
    writeInstruction.setInput(createInstructionInput(0, 0)); // Read.out
    ParallelInstruction write = createParallelInstruction("Write");
    write.setWrite(writeInstruction);

    MapTask mapTask = new MapTask();
    mapTask.setInstructions(ImmutableList.of(read, write));
    mapTask.setFactory(Transport.getJsonFactory());

    Network<Node, Edge> network =
        new MapTaskToNetworkFunction(IdGenerators.decrementingLongs()).apply(mapTask);
    assertNetworkProperties(network);
    assertEquals(3, network.nodes().size());
    assertEquals(2, network.edges().size());

    ParallelInstructionNode readNode = get(network, read);
    InstructionOutputNode readOutputNode = getOnlySuccessor(network, readNode);
    assertEquals(readOutput, readOutputNode.getInstructionOutput());

    ParallelInstructionNode writeNode = getOnlySuccessor(network, readOutputNode);
    assertNotNull(writeNode);
  }

  @Test
  public void testPartialGroupByKey() {
    // Read --> PGBK --> Write
    InstructionOutput readOutput = createInstructionOutput("Read.out");
    ParallelInstruction read = createParallelInstruction("Read", readOutput);
    read.setRead(new ReadInstruction());

    PartialGroupByKeyInstruction pgbkInstruction = new PartialGroupByKeyInstruction();
    pgbkInstruction.setInput(createInstructionInput(0, 0)); // Read.out
    InstructionOutput pgbkOutput = createInstructionOutput("PGBK.out");
    ParallelInstruction pgbk = createParallelInstruction("PGBK", pgbkOutput);
    pgbk.setPartialGroupByKey(pgbkInstruction);

    WriteInstruction writeInstruction = new WriteInstruction();
    writeInstruction.setInput(createInstructionInput(1, 0)); // PGBK.out
    ParallelInstruction write = createParallelInstruction("Write");
    write.setWrite(writeInstruction);

    MapTask mapTask = new MapTask();
    mapTask.setInstructions(ImmutableList.of(read, pgbk, write));
    mapTask.setFactory(Transport.getJsonFactory());

    Network<Node, Edge> network =
        new MapTaskToNetworkFunction(IdGenerators.decrementingLongs()).apply(mapTask);
    assertNetworkProperties(network);
    assertEquals(5, network.nodes().size());
    assertEquals(4, network.edges().size());

    ParallelInstructionNode readNode = get(network, read);
    InstructionOutputNode readOutputNode = getOnlySuccessor(network, readNode);
    assertEquals(readOutput, readOutputNode.getInstructionOutput());

    ParallelInstructionNode pgbkNode = getOnlySuccessor(network, readOutputNode);
    InstructionOutputNode pgbkOutputNode = getOnlySuccessor(network, pgbkNode);
    assertEquals(pgbkOutput, pgbkOutputNode.getInstructionOutput());

    getOnlySuccessor(network, pgbkOutputNode);
    assertNotNull(write);
  }

  @Test
  public void testMultipleOutput() {
    //                /---> WriteA
    // Read ---> ParDo
    //                \---> WriteB
    InstructionOutput readOutput = createInstructionOutput("Read.out");
    ParallelInstruction read = createParallelInstruction("Read", readOutput);
    read.setRead(new ReadInstruction());

    MultiOutputInfo parDoMultiOutput1 = createMultiOutputInfo("output1");
    MultiOutputInfo parDoMultiOutput2 = createMultiOutputInfo("output2");
    ParDoInstruction parDoInstruction = new ParDoInstruction();
    parDoInstruction.setInput(createInstructionInput(0, 0)); // Read.out
    parDoInstruction.setMultiOutputInfos(ImmutableList.of(parDoMultiOutput1, parDoMultiOutput2));
    InstructionOutput parDoOutput1 = createInstructionOutput("ParDo.out1");
    InstructionOutput parDoOutput2 = createInstructionOutput("ParDo.out2");
    ParallelInstruction parDo = createParallelInstruction("ParDo", parDoOutput1, parDoOutput2);
    parDo.setParDo(parDoInstruction);

    WriteInstruction writeAInstruction = new WriteInstruction();
    writeAInstruction.setInput(createInstructionInput(1, 0)); // ParDo.out1
    ParallelInstruction writeA = createParallelInstruction("WriteA");
    writeA.setWrite(writeAInstruction);

    WriteInstruction writeBInstruction = new WriteInstruction();
    writeBInstruction.setInput(createInstructionInput(1, 1)); // ParDo.out2
    ParallelInstruction writeB = createParallelInstruction("WriteB");
    writeB.setWrite(writeBInstruction);

    MapTask mapTask = new MapTask();
    mapTask.setInstructions(ImmutableList.of(read, parDo, writeA, writeB));
    mapTask.setFactory(Transport.getJsonFactory());

    Network<Node, Edge> network =
        new MapTaskToNetworkFunction(IdGenerators.decrementingLongs()).apply(mapTask);
    assertNetworkProperties(network);
    assertEquals(7, network.nodes().size());
    assertEquals(6, network.edges().size());

    ParallelInstructionNode parDoNode = get(network, parDo);
    ParallelInstructionNode writeANode = get(network, writeA);
    ParallelInstructionNode writeBNode = get(network, writeB);
    InstructionOutputNode parDoOutput1Node = getOnlyPredecessor(network, writeANode);
    assertEquals(parDoOutput1, parDoOutput1Node.getInstructionOutput());

    InstructionOutputNode parDoOutput2Node = getOnlyPredecessor(network, writeBNode);
    assertEquals(parDoOutput2, parDoOutput2Node.getInstructionOutput());

    assertThat(
        network.successors(parDoNode),
        Matchers.<Node>containsInAnyOrder(parDoOutput1Node, parDoOutput2Node));

    assertEquals(
        parDoMultiOutput1,
        ((MultiOutputInfoEdge)
                Iterables.getOnlyElement(network.edgesConnecting(parDoNode, parDoOutput1Node)))
            .getMultiOutputInfo());
    assertEquals(
        parDoMultiOutput2,
        ((MultiOutputInfoEdge)
                Iterables.getOnlyElement(network.edgesConnecting(parDoNode, parDoOutput2Node)))
            .getMultiOutputInfo());
  }

  private static void assertNetworkProperties(Network<Node, Edge> network) {
    assertTrue(network.isDirected());
    assertFalse(network.allowsSelfLoops());
    for (Node node : network.nodes()) {
      if (node instanceof ParallelInstructionNode) {
        assertNull(((ParallelInstructionNode) node).getParallelInstruction().getOutputs());
        // Validate that all successors are PCollections with DefaultEdge outgoing edges
        // except for ParDoInstructions
        for (Node successor : network.successors(node)) {
          assertThat(successor, instanceOf(InstructionOutputNode.class));
          // Assert that all outgoing edges for a ParDo are MultiOutputInfoEdges
          if (((ParallelInstructionNode) node).getParallelInstruction().getParDo() != null) {
            for (Edge edge : network.edgesConnecting(node, successor)) {
              assertThat(edge, instanceOf(MultiOutputInfoEdge.class));
            }
          } else {
            for (Edge edge : network.edgesConnecting(node, successor)) {
              assertThat(edge, instanceOf(DefaultEdge.class));
            }
          }
        }

      } else if (node instanceof InstructionOutputNode) {
        assertThat(network.inDegree(node), greaterThanOrEqualTo(1));
        // Validate that all successors are instructions with DefaultEdge outgoing edges
        for (Node successor : network.successors(node)) {
          assertThat(successor, instanceOf(ParallelInstructionNode.class));
          for (Edge edge : network.edgesConnecting(node, successor)) {
            assertThat(edge, instanceOf(DefaultEdge.class));
          }
        }
      }
    }
  }

  private static ParallelInstructionNode get(
      Network<Node, Edge> network, ParallelInstruction instruction) {
    for (Node node : network.nodes()) {
      if (node instanceof ParallelInstructionNode
          && ((ParallelInstructionNode) node)
              .getParallelInstruction()
              .getName()
              .equals(instruction.getName())) {
        return (ParallelInstructionNode) node;
      }
    }
    throw new AssertionError(
        String.format("Unable to find instruction %s in network %s", instruction, network));
  }

  private static ParallelInstructionNode getOnlySuccessor(
      Network<Node, Edge> network, InstructionOutputNode node) {
    return (ParallelInstructionNode) Iterables.getOnlyElement(network.successors(node));
  }

  private static InstructionOutputNode getOnlySuccessor(
      Network<Node, Edge> network, ParallelInstructionNode node) {
    return (InstructionOutputNode) Iterables.getOnlyElement(network.successors(node));
  }

  private static InstructionOutputNode getOnlyPredecessor(
      Network<Node, Edge> network, ParallelInstructionNode node) {
    return (InstructionOutputNode) Iterables.getOnlyElement(network.predecessors(node));
  }

  private static InstructionOutput createInstructionOutput(String name) {
    InstructionOutput rval = new InstructionOutput();
    rval.setName(name);
    return rval;
  }

  private static ParallelInstruction createParallelInstruction(
      String name, InstructionOutput... outputs) {
    ParallelInstruction rval = new ParallelInstruction();
    rval.setName(name);
    rval.setOutputs(Arrays.asList(outputs));
    return rval;
  }

  private static MultiOutputInfo createMultiOutputInfo(String tag) {
    MultiOutputInfo rval = new MultiOutputInfo();
    rval.setTag(tag);
    return rval;
  }

  private static InstructionInput createInstructionInput(int instructionIndex, int outputNum) {
    InstructionInput rval = new InstructionInput();
    rval.setProducerInstructionIndex(instructionIndex);
    rval.setOutputNum(outputNum);
    return rval;
  }
}
