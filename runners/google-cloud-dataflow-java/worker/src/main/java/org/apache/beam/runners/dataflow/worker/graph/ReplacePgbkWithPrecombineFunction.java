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

import static org.apache.beam.runners.dataflow.util.Structs.addString;

import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.PartialGroupByKeyInstruction;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.CombinePhase;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ExecutionLocation;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;

/**
 * A function that replaces PartialGroupByKey nodes with ParDo nodes that can be translated by the
 * {@link RegisterNodeFunction} into a Pre-combine executed by the SDK harness.
 */
public class ReplacePgbkWithPrecombineFunction
    implements Function<MutableNetwork<Node, Edge>, MutableNetwork<Node, Edge>> {

  @Override
  public MutableNetwork<Node, Edge> apply(MutableNetwork<Node, Edge> network) {
    Networks.replaceDirectedNetworkNodes(
        network,
        (Node node) -> {
          if (!isPrecombinePgbk(node)) {
            return node;
          }

          // Turn the Pgbk into a ParDo with the combine function as a UserFn.
          ParallelInstructionNode castNode = ((ParallelInstructionNode) node);
          ParallelInstruction parallelInstruction = castNode.getParallelInstruction();
          Map<String, Object> cloudUserFnSpec =
              parallelInstruction.getPartialGroupByKey().getValueCombiningFn();
          addString(cloudUserFnSpec, WorkerPropertyNames.PHASE, CombinePhase.ADD);

          ParDoInstruction newParDoInstruction = new ParDoInstruction();
          newParDoInstruction.setUserFn(cloudUserFnSpec);

          ParallelInstruction newParallelInstruction = parallelInstruction.clone();
          newParallelInstruction.setPartialGroupByKey(null);
          newParallelInstruction.setParDo(newParDoInstruction);

          return ParallelInstructionNode.create(newParallelInstruction, ExecutionLocation.UNKNOWN);
        });

    return network;
  }

  /**
   * Returns true iff the {@link Node} is a {@link ParallelInstructionNode} with a {@link
   * PartialGroupByKeyInstruction} that contains a serialized combining function.
   */
  private boolean isPrecombinePgbk(Node node) {
    return node instanceof ParallelInstructionNode
        && ((ParallelInstructionNode) node).getParallelInstruction().getPartialGroupByKey() != null
        && ((ParallelInstructionNode) node)
                .getParallelInstruction()
                .getPartialGroupByKey()
                .getValueCombiningFn()
            != null;
  }
}
