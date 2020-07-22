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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.PartialGroupByKeyInstruction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.worker.graph.Edges.DefaultEdge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.ImmutableNetwork;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.Network;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.NetworkBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ReplacePgbkWithPrecombineFunction}. */
@RunWith(JUnit4.class)
public final class ReplacePgbkWithPrecombineFunctionTest {

  @Test
  public void testPrecombinePgbkIsReplaced() throws Exception {
    // Network:
    //  out1 --> precombine_pgbk --> out2
    Map<String, Object> valueCombiningFn = new HashMap<>();

    Node out1 = createInstructionOutputNode("out1");
    String pgbkName = "precombine_pgbk";
    Node precombinePgbk = createPrecombinePgbkNode(pgbkName, valueCombiningFn);
    Node out2 = createInstructionOutputNode("out2");

    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    network.addNode(out1);
    network.addNode(precombinePgbk);
    network.addNode(out2);
    network.addEdge(out1, precombinePgbk, DefaultEdge.create());
    network.addEdge(precombinePgbk, out2, DefaultEdge.create());

    Network<Node, Edge> inputNetwork = ImmutableNetwork.copyOf(network);
    network = new ReplacePgbkWithPrecombineFunction().apply(network);

    // Assert that network has same structure (same number of nodes and paths).
    assertEquals(inputNetwork.nodes().size(), network.nodes().size());
    assertEquals(inputNetwork.edges().size(), network.edges().size());
    List<List<Node>> oldPaths = Networks.allPathsFromRootsToLeaves(inputNetwork);
    List<List<Node>> newPaths = Networks.allPathsFromRootsToLeaves(network);
    assertEquals(oldPaths.size(), newPaths.size());

    // Assert that the pgbk node has been replaced.
    for (Node node : network.nodes()) {
      if (node instanceof ParallelInstructionNode) {
        ParallelInstructionNode createdCombineNode = (ParallelInstructionNode) node;
        ParallelInstruction parallelInstruction = createdCombineNode.getParallelInstruction();
        assertEquals(parallelInstruction.getName(), pgbkName);
        assertNull(parallelInstruction.getPartialGroupByKey());
        assertNotNull(parallelInstruction.getParDo());

        ParDoInstruction parDoInstruction = parallelInstruction.getParDo();
        assertEquals(parDoInstruction.getUserFn(), valueCombiningFn);

        break;
      }
    }
  }

  @Test
  public void testNormalPgbkIsNotReplaced() throws Exception {
    // Network:
    //  out1 --> pgbk --> out2
    Node out1 = createInstructionOutputNode("out1");
    Node pgbk = createPrecombinePgbkNode("pgbk", null);
    Node out2 = createInstructionOutputNode("out2");

    MutableNetwork<Node, Edge> network = createEmptyNetwork();
    network.addNode(out1);
    network.addNode(pgbk);
    network.addNode(out2);
    network.addEdge(out1, pgbk, DefaultEdge.create());
    network.addEdge(pgbk, out2, DefaultEdge.create());

    Network<Node, Edge> inputNetwork = ImmutableNetwork.copyOf(network);
    network = new ReplacePgbkWithPrecombineFunction().apply(network);

    // Assert that network is unchanged (identical paths).
    List<List<Node>> oldPaths = Networks.allPathsFromRootsToLeaves(inputNetwork);
    List<List<Node>> newPaths = Networks.allPathsFromRootsToLeaves(network);
    assertThat(oldPaths, containsInAnyOrder(newPaths.toArray()));

    // Assert that the pgbk node is still present and unchanged.
    for (Node node : network.nodes()) {
      if (node instanceof ParallelInstructionNode) {
        ParallelInstructionNode newNode = (ParallelInstructionNode) node;
        ParallelInstruction parallelInstruction = newNode.getParallelInstruction();
        assertEquals(
            parallelInstruction, ((ParallelInstructionNode) pgbk).getParallelInstruction());
        break;
      }
    }
  }

  private static MutableNetwork<Node, Edge> createEmptyNetwork() {
    return NetworkBuilder.directed()
        .allowsSelfLoops(false)
        .allowsParallelEdges(true)
        .<Node, Edge>build();
  }

  /** Creates a PartialGroupByKey node with an optional combining function. */
  private static ParallelInstructionNode createPrecombinePgbkNode(
      String name, @Nullable Map<String, Object> valueCombiningFn) {
    return ParallelInstructionNode.create(
        new ParallelInstruction()
            .setName(name)
            .setPartialGroupByKey(
                new PartialGroupByKeyInstruction().setValueCombiningFn(valueCombiningFn)),
        Nodes.ExecutionLocation.SDK_HARNESS);
  }

  /** Creates an {@link InstructionOutputNode} to act as a PCollection. */
  private static InstructionOutputNode createInstructionOutputNode(String name) {
    return InstructionOutputNode.create(new InstructionOutput().setName(name), "fakeId");
  }
}
