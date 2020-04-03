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

import com.google.api.client.json.JsonFactory;
import com.google.api.services.dataflow.model.InstructionInput;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.apiary.Apiary;
import org.apache.beam.runners.dataflow.worker.graph.Edges.DefaultEdge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.MultiOutputInfoEdge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.NetworkBuilder;

/**
 * Creates a directed bipartite network of {@link ParallelInstructionNode}s and {@link
 * InstructionOutputNode}s. The resulting network has the following properties:
 *
 * <ul>
 *   <li>Each {@link ParallelInstructionNode} has zero or more {@link InstructionOutputNode}.
 *       predecessors.
 *   <li>Each {@link InstructionOutputNode} has one or more {@link ParallelInstructionNode}
 *       predecessors.
 *   <li>The indegree of each {@link ParallelInstructionNode} is at most one.
 *   <li>The indegree of each {@link InstructionOutputNode} is always one.
 * </ul>
 *
 * <p>The outgoing edges of a {@link ParallelInstructionNode} with a {@link ParDoInstruction} are
 * {@link MultiOutputInfoEdge}s. All other edges are {@link DefaultEdge}s.
 */
public class MapTaskToNetworkFunction implements Function<MapTask, MutableNetwork<Node, Edge>> {
  private static ParallelInstruction clone(JsonFactory factory, ParallelInstruction instruction) {
    try {
      return factory.fromString(factory.toString(instruction), ParallelInstruction.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private final IdGenerator idGenerator;

  public MapTaskToNetworkFunction(IdGenerator idGenerator) {
    this.idGenerator = idGenerator;
  }

  @Override
  public MutableNetwork<Node, Edge> apply(MapTask mapTask) {
    List<ParallelInstruction> parallelInstructions = Apiary.listOrEmpty(mapTask.getInstructions());
    MutableNetwork<Node, Edge> network =
        NetworkBuilder.directed()
            .allowsSelfLoops(false)
            .allowsParallelEdges(true)
            .expectedNodeCount(parallelInstructions.size() * 2)
            .build();

    // Add all the instruction nodes and output nodes
    ParallelInstructionNode[] instructionNodes =
        new ParallelInstructionNode[parallelInstructions.size()];
    InstructionOutputNode[][] outputNodes =
        new InstructionOutputNode[parallelInstructions.size()][];
    for (int i = 0; i < parallelInstructions.size(); ++i) {
      // InstructionOutputNode's are the source of truth on instruction outputs.
      // Clear the instruction's outputs to reduce chance for confusion.
      List<InstructionOutput> outputs =
          Apiary.listOrEmpty(parallelInstructions.get(i).getOutputs());
      outputNodes[i] = new InstructionOutputNode[outputs.size()];

      JsonFactory factory =
          MoreObjects.firstNonNull(mapTask.getFactory(), Transport.getJsonFactory());
      ParallelInstruction parallelInstruction =
          clone(factory, parallelInstructions.get(i)).setOutputs(null);

      ParallelInstructionNode instructionNode =
          ParallelInstructionNode.create(parallelInstruction, Nodes.ExecutionLocation.UNKNOWN);
      instructionNodes[i] = instructionNode;
      network.addNode(instructionNode);

      // Connect the instruction node output to the output PCollection node
      for (int j = 0; j < outputs.size(); ++j) {
        InstructionOutput instructionOutput = outputs.get(j);
        InstructionOutputNode outputNode =
            InstructionOutputNode.create(
                instructionOutput, "generatedPcollection" + this.idGenerator.getId());
        network.addNode(outputNode);
        if (parallelInstruction.getParDo() != null) {
          network.addEdge(
              instructionNode,
              outputNode,
              MultiOutputInfoEdge.create(
                  parallelInstruction.getParDo().getMultiOutputInfos().get(j)));
        } else {
          network.addEdge(instructionNode, outputNode, DefaultEdge.create());
        }
        outputNodes[i][j] = outputNode;
      }
    }

    // Connect PCollections as inputs to instructions
    for (ParallelInstructionNode instructionNode : instructionNodes) {
      ParallelInstruction parallelInstruction = instructionNode.getParallelInstruction();
      if (parallelInstruction.getFlatten() != null) {
        for (InstructionInput input :
            Apiary.listOrEmpty(parallelInstruction.getFlatten().getInputs())) {
          attachInput(input, network, instructionNode, outputNodes);
        }
      } else if (parallelInstruction.getParDo() != null) {
        attachInput(
            parallelInstruction.getParDo().getInput(), network, instructionNode, outputNodes);
      } else if (parallelInstruction.getPartialGroupByKey() != null) {
        attachInput(
            parallelInstruction.getPartialGroupByKey().getInput(),
            network,
            instructionNode,
            outputNodes);
      } else if (parallelInstruction.getRead() != null) {
        // Reads have no inputs so nothing to do
      } else if (parallelInstruction.getWrite() != null) {
        attachInput(
            parallelInstruction.getWrite().getInput(), network, instructionNode, outputNodes);
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Unknown type of instruction %s for map task %s", parallelInstruction, mapTask));
      }
    }
    return network;
  }

  /**
   * Adds an edge to the network from a PCollection to an instruction based upon the {@link
   * InstructionInput}.
   */
  private static void attachInput(
      InstructionInput input,
      MutableNetwork<Node, Edge> network,
      ParallelInstructionNode node,
      InstructionOutputNode[][] outputNodes) {
    int producerInstructionIndex = Apiary.intOrZero(input.getProducerInstructionIndex());
    int outputNum = Apiary.intOrZero(input.getOutputNum());
    network.addEdge(outputNodes[producerInstructionIndex][outputNum], node, DefaultEdge.create());
  }
}
