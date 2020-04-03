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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.matchers.JUnitMatchers.hasItems;

import com.google.api.services.dataflow.model.FlattenInstruction;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.ParallelInstruction;
import java.util.List;
import org.apache.beam.runners.dataflow.worker.graph.Edges.DefaultEdge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ExecutionLocation;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.NetworkBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CloneAmbiguousFlattensFunction}. */
@RunWith(JUnit4.class)
public final class CloneAmbiguousFlattensFunctionTest {

  /** A node that stores nothing. Used for testing with nodes that have no ExecutionLocation. */
  private static class NoLocationNode extends Node {}

  @Test
  public void testEmptyNetwork() throws Exception {
    assertEquals(
        createEmptyNetwork(), new CloneAmbiguousFlattensFunction().apply(createEmptyNetwork()));
  }

  /**
   * Tests that a single ambiguous flatten clones properly, with the proper edges between
   * predecessors and successors, and that no new paths are created.
   */
  @Test
  public void testSingleFlatten() throws Exception {
    // sdk_predecessor -----> out -\                         /-> sdk_successor --> out
    //                              ambiguous_flatten --> out -> no_location_successor --> out
    // runner_predecessor --> out -/                         \-> runner_successor --> out
    MutableNetwork<Node, Edge> network = createEmptyNetwork();

    Node sdkPredecessor = createSdkNode("sdk_predecessor");
    Node runnerPredecessor = createRunnerNode("runner_predecessor");
    Node sdkPredecessorOutput = createPCollection("sdk_predecessor.out");
    Node runnerPredecessorOutput = createPCollection("runner_predecessor.out");
    Node ambiguousFlatten = createFlatten("ambiguous_flatten", ExecutionLocation.AMBIGUOUS);
    Node ambiguousFlattenOutput = createPCollection("ambiguous_flatten.out");
    Node sdkSuccessor = createSdkNode("sdk_successor");
    Node runnerSuccessor = createRunnerNode("runner_successor");
    Node noLocationSuccessor = createNoLocationNode();
    Node sdkSuccessorOutput = createPCollection("sdk_successor.out");
    Node runnerSuccessorOutput = createPCollection("runner_successor.out");
    Node noLocationSuccessorOutput = createPCollection("no_location_successor.out");

    network.addNode(sdkPredecessor);
    network.addNode(runnerPredecessor);
    network.addNode(sdkPredecessorOutput);
    network.addNode(runnerPredecessorOutput);
    network.addNode(ambiguousFlatten);
    network.addNode(ambiguousFlattenOutput);
    network.addNode(sdkSuccessor);
    network.addNode(runnerSuccessor);
    network.addNode(noLocationSuccessor);
    network.addNode(sdkSuccessorOutput);
    network.addNode(runnerSuccessorOutput);
    network.addNode(noLocationSuccessorOutput);

    network.addEdge(sdkPredecessor, sdkPredecessorOutput, DefaultEdge.create());
    network.addEdge(runnerPredecessor, runnerPredecessorOutput, DefaultEdge.create());
    network.addEdge(sdkPredecessorOutput, ambiguousFlatten, DefaultEdge.create());
    network.addEdge(runnerPredecessorOutput, ambiguousFlatten, DefaultEdge.create());
    network.addEdge(ambiguousFlatten, ambiguousFlattenOutput, DefaultEdge.create());
    network.addEdge(ambiguousFlattenOutput, sdkSuccessor, DefaultEdge.create());
    network.addEdge(ambiguousFlattenOutput, runnerSuccessor, DefaultEdge.create());
    network.addEdge(ambiguousFlattenOutput, noLocationSuccessor, DefaultEdge.create());
    network.addEdge(sdkSuccessor, sdkSuccessorOutput, DefaultEdge.create());
    network.addEdge(runnerSuccessor, runnerSuccessorOutput, DefaultEdge.create());
    network.addEdge(noLocationSuccessor, noLocationSuccessorOutput, DefaultEdge.create());

    // After:
    // SdkPredecessor -----> out --> SdkFlatten  --> out --> SdkSuccessor --> out
    //                           X
    // RunnerPredecessor --> out --> RunnerFlatten --> out --> RunnerSuccessor --> out
    //                                                     \-> NoLocationSuccessor --> out
    List<List<Node>> originalPaths = Networks.allPathsFromRootsToLeaves(network);
    network = new CloneAmbiguousFlattensFunction().apply(network);

    // Get sdk and runner flattens and outputs.
    ParallelInstructionNode sdkFlatten = null;
    ParallelInstructionNode runnerFlatten = null;
    for (Node node : network.nodes()) {
      if (node instanceof ParallelInstructionNode
          && ((ParallelInstructionNode) node).getParallelInstruction().getFlatten() != null) {
        ParallelInstructionNode castNode = ((ParallelInstructionNode) node);
        if (castNode.getExecutionLocation() == ExecutionLocation.SDK_HARNESS) {
          sdkFlatten = castNode;
        } else if (castNode.getExecutionLocation() == ExecutionLocation.RUNNER_HARNESS) {
          runnerFlatten = castNode;
        } else {
          assertTrue(
              "Ambiguous flatten not removed from network.",
              castNode.getExecutionLocation() != ExecutionLocation.AMBIGUOUS);
        }
      }
    }
    assertNotNull("Ambiguous flatten was not cloned into sdk flatten.", sdkFlatten);
    assertNotNull("Ambiguous flatten was not cloned into runner flatten.", runnerFlatten);

    Node sdkFlattenOutput = Iterables.getOnlyElement(network.successors(sdkFlatten));
    Node runnerFlattenOutput = Iterables.getOnlyElement(network.successors(runnerFlatten));

    assertEquals(2, network.predecessors(sdkFlatten).size());
    assertEquals(2, network.predecessors(runnerFlatten).size());
    assertEquals(1, network.successors(sdkFlattenOutput).size());
    assertEquals(2, network.successors(runnerFlattenOutput).size());

    assertSame(sdkSuccessor, Iterables.getOnlyElement(network.successors(sdkFlattenOutput)));
    assertThat(
        network.successors(runnerFlattenOutput), hasItems(runnerSuccessor, noLocationSuccessor));

    assertEquals(originalPaths.size(), Networks.allPathsFromRootsToLeaves(network).size());
  }

  /**
   * Tests that a network with non-ambiguous flattens can still clone the ambiguous flattens
   * properly, without leaving any ambiguous flattens, modifying the non-ambiguous flattens, or
   * changing the number of paths.
   */
  @Test
  public void testNonAmbiguousFlattens() throws Exception {
    //                      sdk2+out -\
    //                                 sdk_flatten+out --> sdk3+out
    // sdk1+out ----\                 /
    //               ambig_flatten+out
    // runner1+out -/                 \-> runner2+out -\
    //                                                  runner_flatten+out --> runner4+out
    //                                    runner3+out -/
    MutableNetwork<Node, Edge> network = createEmptyNetwork();

    Node sdk1 = createSdkNode("sdk1");
    Node sdk2 = createSdkNode("sdk2");
    Node sdk3 = createSdkNode("sdk3");
    Node sdk1Out = createPCollection("sdk1.out");
    Node sdk2Out = createPCollection("sdk2.out");
    Node sdk3Out = createPCollection("sdk3.out");

    Node runner1 = createRunnerNode("runner1");
    Node runner2 = createRunnerNode("runner2");
    Node runner3 = createRunnerNode("runner3");
    Node runner4 = createRunnerNode("runner4");
    Node runner1Out = createPCollection("runner1.out");
    Node runner2Out = createPCollection("runner2.out");
    Node runner3Out = createPCollection("runner3.out");
    Node runner4Out = createPCollection("runner4.out");

    Node ambiguousFlatten = createFlatten("ambiguous_flatten", ExecutionLocation.AMBIGUOUS);
    Node ambiguousFlattenOut = createPCollection("ambiguous_flatten.out");
    Node sdkFlatten = createFlatten("sdk_flatten", ExecutionLocation.SDK_HARNESS);
    Node sdkFlattenOut = createPCollection("sdk_flatten.out");
    Node runnerFlatten = createFlatten("runner_flatten", ExecutionLocation.RUNNER_HARNESS);
    Node runnerFlattenOut = createPCollection("runner_flatten.out");

    network.addNode(sdk1);
    network.addNode(sdk2);
    network.addNode(sdk3);
    network.addNode(sdk1Out);
    network.addNode(sdk2Out);
    network.addNode(sdk3Out);
    network.addNode(runner1);
    network.addNode(runner2);
    network.addNode(runner3);
    network.addNode(runner4);
    network.addNode(runner1Out);
    network.addNode(runner2Out);
    network.addNode(runner3Out);
    network.addNode(runner4Out);
    network.addNode(ambiguousFlatten);
    network.addNode(ambiguousFlattenOut);
    network.addNode(sdkFlatten);
    network.addNode(sdkFlattenOut);
    network.addNode(runnerFlatten);
    network.addNode(runnerFlattenOut);

    network.addEdge(sdk1, sdk1Out, DefaultEdge.create());
    network.addEdge(sdk2, sdk2Out, DefaultEdge.create());
    network.addEdge(sdk3, sdk3Out, DefaultEdge.create());
    network.addEdge(runner1, runner1Out, DefaultEdge.create());
    network.addEdge(runner2, runner2Out, DefaultEdge.create());
    network.addEdge(runner3, runner3Out, DefaultEdge.create());
    network.addEdge(runner4, runner4Out, DefaultEdge.create());
    network.addEdge(ambiguousFlatten, ambiguousFlattenOut, DefaultEdge.create());
    network.addEdge(sdkFlatten, sdkFlattenOut, DefaultEdge.create());
    network.addEdge(runnerFlatten, runnerFlattenOut, DefaultEdge.create());

    network.addEdge(sdk1Out, ambiguousFlatten, DefaultEdge.create());
    network.addEdge(runner1Out, ambiguousFlatten, DefaultEdge.create());
    network.addEdge(ambiguousFlattenOut, sdkFlatten, DefaultEdge.create());
    network.addEdge(sdk2Out, sdkFlatten, DefaultEdge.create());
    network.addEdge(sdkFlattenOut, sdk3, DefaultEdge.create());
    network.addEdge(ambiguousFlattenOut, runner2, DefaultEdge.create());
    network.addEdge(runner2Out, runnerFlatten, DefaultEdge.create());
    network.addEdge(runner3Out, runnerFlatten, DefaultEdge.create());
    network.addEdge(runnerFlattenOut, runner4, DefaultEdge.create());

    // Apply function and perform assertions
    List<List<Node>> originalPaths = Networks.allPathsFromRootsToLeaves(network);
    network = new CloneAmbiguousFlattensFunction().apply(network);

    for (Node node : network.nodes()) {
      if (node instanceof ParallelInstructionNode
          && ((ParallelInstructionNode) node).getParallelInstruction().getFlatten() != null) {
        ParallelInstructionNode castNode = ((ParallelInstructionNode) node);
        assertTrue(
            "Ambiguous flatten not removed from network.",
            castNode.getExecutionLocation() != ExecutionLocation.AMBIGUOUS);
        if ("sdk_flatten".equals(castNode.getParallelInstruction().getName())) {
          assertSame("SDK flatten has been incorrectly modified.", sdkFlatten, castNode);
        } else if ("runner_flatten".equals(castNode.getParallelInstruction().getName())) {
          assertSame("Runner flatten has been incorrectly modified.", runnerFlatten, castNode);
        }
      }
    }

    assertEquals(originalPaths.size(), Networks.allPathsFromRootsToLeaves(network).size());
  }

  /**
   * Tests that multiple connected ambiguous flattens in a network all get cloned without leaving
   * any ambiguous flattens left in the network and without changing the number of paths.
   */
  @Test
  public void testConnectedFlattens() throws Exception {
    // sdk1+out ----\
    //               ambig_flatten1+out --> sdk3+out
    // runner1+out -/                    \                     /-> sdk4+out
    // sdk2+out ----\                    /-> ambig_flatten3+out
    //               ambig_flatten2+out --> runner3+out        \-> runner4+out
    // runner2+out -/
    MutableNetwork<Node, Edge> network = createEmptyNetwork();

    Node sdk1 = createSdkNode("sdk1");
    Node sdk2 = createSdkNode("sdk2");
    Node sdk3 = createSdkNode("sdk3");
    Node sdk4 = createSdkNode("sdk4");
    Node sdk1Out = createPCollection("sdk1.out");
    Node sdk2Out = createPCollection("sdk2.out");
    Node sdk3Out = createPCollection("sdk3.out");
    Node sdk4Out = createPCollection("sdk4.out");

    Node runner1 = createRunnerNode("runner1");
    Node runner2 = createRunnerNode("runner2");
    Node runner3 = createRunnerNode("runner3");
    Node runner4 = createRunnerNode("runner4");
    Node runner1Out = createPCollection("runner1.out");
    Node runner2Out = createPCollection("runner2.out");
    Node runner3Out = createPCollection("runner3.out");
    Node runner4Out = createPCollection("runner4.out");

    Node ambiguousFlatten1 = createFlatten("ambiguous_flatten1", ExecutionLocation.AMBIGUOUS);
    Node ambiguousFlatten2 = createFlatten("ambiguous_flatten2", ExecutionLocation.AMBIGUOUS);
    Node ambiguousFlatten3 = createFlatten("ambiguous_flatten3", ExecutionLocation.AMBIGUOUS);
    Node ambiguousFlatten1Out = createPCollection("ambiguous_flatten1.out");
    Node ambiguousFlatten2Out = createPCollection("ambiguous_flatten2.out");
    Node ambiguousFlatten3Out = createPCollection("ambiguous_flatten3.out");

    network.addNode(sdk1);
    network.addNode(sdk2);
    network.addNode(sdk3);
    network.addNode(sdk4);
    network.addNode(sdk1Out);
    network.addNode(sdk2Out);
    network.addNode(sdk3Out);
    network.addNode(sdk4Out);
    network.addNode(runner1);
    network.addNode(runner2);
    network.addNode(runner3);
    network.addNode(runner4);
    network.addNode(runner1Out);
    network.addNode(runner2Out);
    network.addNode(runner3Out);
    network.addNode(runner4Out);
    network.addNode(ambiguousFlatten1);
    network.addNode(ambiguousFlatten2);
    network.addNode(ambiguousFlatten3);
    network.addNode(ambiguousFlatten1Out);
    network.addNode(ambiguousFlatten2Out);
    network.addNode(ambiguousFlatten3Out);

    network.addEdge(sdk1, sdk1Out, DefaultEdge.create());
    network.addEdge(sdk2, sdk2Out, DefaultEdge.create());
    network.addEdge(sdk3, sdk3Out, DefaultEdge.create());
    network.addEdge(sdk4, sdk4Out, DefaultEdge.create());
    network.addEdge(runner1, runner1Out, DefaultEdge.create());
    network.addEdge(runner2, runner2Out, DefaultEdge.create());
    network.addEdge(runner3, runner3Out, DefaultEdge.create());
    network.addEdge(runner4, runner4Out, DefaultEdge.create());
    network.addEdge(ambiguousFlatten1, ambiguousFlatten1Out, DefaultEdge.create());
    network.addEdge(ambiguousFlatten2, ambiguousFlatten2Out, DefaultEdge.create());
    network.addEdge(ambiguousFlatten3, ambiguousFlatten3Out, DefaultEdge.create());

    network.addEdge(sdk1Out, ambiguousFlatten1, DefaultEdge.create());
    network.addEdge(runner1Out, ambiguousFlatten1, DefaultEdge.create());
    network.addEdge(sdk2Out, ambiguousFlatten2, DefaultEdge.create());
    network.addEdge(runner2Out, ambiguousFlatten2, DefaultEdge.create());
    network.addEdge(ambiguousFlatten1Out, sdk3, DefaultEdge.create());
    network.addEdge(ambiguousFlatten1Out, ambiguousFlatten3, DefaultEdge.create());
    network.addEdge(ambiguousFlatten2Out, ambiguousFlatten3, DefaultEdge.create());
    network.addEdge(ambiguousFlatten2Out, runner3, DefaultEdge.create());
    network.addEdge(ambiguousFlatten3Out, sdk4, DefaultEdge.create());
    network.addEdge(ambiguousFlatten3Out, runner4, DefaultEdge.create());

    // Apply function and perform assertions
    List<List<Node>> originalPaths = Networks.allPathsFromRootsToLeaves(network);
    network = new CloneAmbiguousFlattensFunction().apply(network);

    for (Node node : network.nodes()) {
      if (node instanceof ParallelInstructionNode
          && ((ParallelInstructionNode) node).getParallelInstruction().getFlatten() != null) {
        ParallelInstructionNode castNode = ((ParallelInstructionNode) node);
        assertTrue(
            "Ambiguous flatten not removed from network.",
            castNode.getExecutionLocation() != ExecutionLocation.AMBIGUOUS);
      }
    }

    assertEquals(originalPaths.size(), Networks.allPathsFromRootsToLeaves(network).size());
  }

  private static MutableNetwork<Node, Edge> createEmptyNetwork() {
    return NetworkBuilder.directed()
        .allowsSelfLoops(false)
        .allowsParallelEdges(true)
        .<Node, Edge>build();
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

  /** Creates a flatten node with a given location set. */
  private static ParallelInstructionNode createFlatten(String name, ExecutionLocation location) {
    return ParallelInstructionNode.create(
        new ParallelInstruction().setFlatten(new FlattenInstruction()).setName(name), location);
  }

  /** Creates an {@link InstructionOutputNode} to act as a PCollection. */
  private static InstructionOutputNode createPCollection(String name) {
    return InstructionOutputNode.create(new InstructionOutput().setName(name), "fakeId");
  }

  /** Creates a {@link NoLocationNode} to use for testing nodes that have no ExecutionLocation */
  private static NoLocationNode createNoLocationNode() {
    return new NoLocationNode();
  }
}
