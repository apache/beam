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
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ExecutionLocation;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.runners.dataflow.worker.util.CloudSourceUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;

/**
 * A function which sets the location for {@link ParallelInstructionNode}s in the network, with the
 * exception of {@link FlattenInstruction}s.
 */
public class DeduceNodeLocationsFunction
    implements Function<MutableNetwork<Node, Edge>, MutableNetwork<Node, Edge>> {

  /**
   * Sources that are executed on the runner harness. Note: Keep in sync with {@link ReaderRegistry}
   */
  private static final ImmutableSet<String> RUNNER_SOURCES =
      ImmutableSet.of(
          "AvroSource",
          "IsmSource",
          "ConcatSource",
          "UngroupedShuffleSource",
          "PartitioningShuffleSource",
          "GroupingShuffleSource",
          "InMemorySource",
          "WindowingWindmillReader",
          "org.apache.beam.runners.dataflow.worker.WindowingWindmillReader",
          "org.apache.beam.runners.dataflow.worker.BucketingWindmillSource",
          "UngroupedWindmillReader",
          "org.apache.beam.runners.dataflow.worker.UngroupedWindmillSource",
          "org.apache.beam.runners.dataflow.worker.UngroupedWindmillReader",
          "PubsubReader",
          "org.apache.beam.runners.dataflow.worker.PubsubSource",
          // Dax sources are dynamically registered. They are executed in the
          // runner.
          "dax");

  private static final ImmutableSet<String> SDK_COMPATIBLE_PAR_DO_FNS =
      ImmutableSet.of("DoFn", "CombineValuesFn", "KeyedCombineFn");

  @Override
  public MutableNetwork<Node, Edge> apply(MutableNetwork<Node, Edge> network) {

    // Replace deducible nodes with identical node except with location deduced.
    Networks.replaceDirectedNetworkNodes(
        network,
        (Node node) -> {
          if (!isDeducible(node)) {
            return node;
          }

          ParallelInstructionNode castNode = ((ParallelInstructionNode) node);
          ExecutionLocation location;

          if (executesInSdkHarness(castNode.getParallelInstruction())) {
            location = ExecutionLocation.SDK_HARNESS;
          } else {
            location = ExecutionLocation.RUNNER_HARNESS;
          }

          return ParallelInstructionNode.create(castNode.getParallelInstruction(), location);
        });

    return network;
  }

  /**
   * Returns true iff the {@link Node} represents a {@link ParallelInstructionNode} that does not
   * have a {@link FlattenInstruction} and has an {@link ExecutionLocation} of UNKNOWN.
   */
  private boolean isDeducible(Node node) {
    return node instanceof ParallelInstructionNode
        && ((ParallelInstructionNode) node).getParallelInstruction().getFlatten() == null
        && ((ParallelInstructionNode) node).getExecutionLocation() == ExecutionLocation.UNKNOWN;
  }

  /**
   * Returns true iff the {@link ParallelInstruction} represents an instruction that executes within
   * the SDK harness as part of the fused sub-graph. This excludes special instructions related to
   * {@link GroupByKey} such as window assignment and window merging as they cross the Beam Fn API
   * with a specialized protocol.
   */
  private static boolean executesInSdkHarness(ParallelInstruction parallelInstruction) {
    if (parallelInstruction.getRead() != null) {
      ReadInstruction readInstruction = parallelInstruction.getRead();
      CloudObject sourceSpec =
          CloudObject.fromSpec(
              CloudSourceUtils.flattenBaseSpecs(readInstruction.getSource()).getSpec());
      return !RUNNER_SOURCES.contains(sourceSpec.getClassName());
    } else if (parallelInstruction.getParDo() != null) {
      ParDoInstruction parDoInstruction = parallelInstruction.getParDo();
      CloudObject userFnSpec = CloudObject.fromSpec(parDoInstruction.getUserFn());
      return SDK_COMPATIBLE_PAR_DO_FNS.contains(userFnSpec.getClassName());
    }
    return false;
  }
}
