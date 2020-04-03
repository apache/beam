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
import com.google.api.services.dataflow.model.MapTask;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;

/**
 * A function which removes {@link FlattenInstruction}s from the network representation of a {@link
 * MapTask}. Remove a Flatten instruction and its PCollection by directly connecting the predecessor
 * PCollections of the Flatten with the successor instructions.
 */
public class RemoveFlattenInstructionsFunction
    implements Function<MutableNetwork<Node, Edge>, MutableNetwork<Node, Edge>> {

  @Override
  public MutableNetwork<Node, Edge> apply(MutableNetwork<Node, Edge> network) {
    for (Node node : ImmutableList.copyOf(Iterables.filter(network.nodes(), IsFlatten.INSTANCE))) {

      // For each successor instruction after the Flatten, connect it directly to the
      // predecessor PCollections of Flatten.
      Node flattenPCollection = Iterables.getOnlyElement(network.successors(node));
      for (Node successorInstruction :
          ImmutableList.copyOf(network.successors(flattenPCollection))) {
        for (Edge edge :
            ImmutableList.copyOf(
                network.edgesConnecting(flattenPCollection, successorInstruction))) {
          for (Node predecessorPCollection : ImmutableList.copyOf(network.predecessors(node))) {
            network.addEdge(predecessorPCollection, successorInstruction, edge.clone());
          }
        }
      }

      // Remove the Flatten instruction and its output PCollection.
      network.removeNode(flattenPCollection);
      network.removeNode(node);
    }
    return network;
  }

  /**
   * A {@link Predicate} which returns true iff the {@link Node} represents a {@link
   * ParallelInstructionNode} with a {@link FlattenInstruction}.
   */
  private static class IsFlatten implements Predicate<Node> {
    private static final IsFlatten INSTANCE = new IsFlatten();

    @Override
    public boolean apply(Node node) {
      return node instanceof ParallelInstructionNode
          && ((ParallelInstructionNode) node).getParallelInstruction().getFlatten() != null;
    }

    // Hide visibility to prevent instantiation
    private IsFlatten() {}
  }
}
