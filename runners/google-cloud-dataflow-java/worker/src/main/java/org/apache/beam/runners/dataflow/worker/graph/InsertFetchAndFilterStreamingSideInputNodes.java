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

import static org.apache.beam.runners.dataflow.util.Structs.getString;

import com.google.api.services.dataflow.model.ParDoInstruction;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ExecutionLocation;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.FetchAndFilterStreamingSideInputsNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Inserts a {@link ParDoFn} that handles filtering blocked side inputs and fetching ready side
 * inputs for streaming pipelines before user {@link ParDo ParDos} containing side inputs.
 */
public class InsertFetchAndFilterStreamingSideInputNodes {
  public static InsertFetchAndFilterStreamingSideInputNodes with(
      RunnerApi.@Nullable Pipeline pipeline) {
    return new InsertFetchAndFilterStreamingSideInputNodes(pipeline);
  }

  private final RunnerApi.@Nullable Pipeline pipeline;

  private InsertFetchAndFilterStreamingSideInputNodes(RunnerApi.Pipeline pipeline) {
    this.pipeline = pipeline;
  }

  public MutableNetwork<Node, Edge> forNetwork(MutableNetwork<Node, Edge> network) {
    if (pipeline == null) {
      return network;
    }
    RehydratedComponents rehydratedComponents =
        RehydratedComponents.forComponents(pipeline.getComponents());

    for (ParallelInstructionNode node :
        ImmutableList.copyOf(Iterables.filter(network.nodes(), ParallelInstructionNode.class))) {
      // If this isn't a ParDo or doesn't execute in the SDK harness then we don't have
      // to worry about it.
      if (node.getParallelInstruction().getParDo() == null
          || !ExecutionLocation.SDK_HARNESS.equals(node.getExecutionLocation())) {
        continue;
      }

      ParDoInstruction parDoInstruction = node.getParallelInstruction().getParDo();
      CloudObject userFnSpec = CloudObject.fromSpec(parDoInstruction.getUserFn());
      String parDoPTransformId = getString(userFnSpec, PropertyNames.SERIALIZED_FN);

      // Skip ParDoInstruction nodes that contain payloads without side inputs.
      String userFnClassName = userFnSpec.getClassName();
      if ("CombineValuesFn".equals(userFnClassName) || "KeyedCombineFn".equals(userFnClassName)) {
        continue; // These nodes have CombinePayloads which have no side inputs.
      }

      RunnerApi.PTransform parDoPTransform =
          pipeline.getComponents().getTransformsOrDefault(parDoPTransformId, null);

      // TODO: only the non-null branch should exist; for migration ease only
      if (parDoPTransform == null) {
        continue;
      }

      RunnerApi.ParDoPayload parDoPayload;
      try {
        parDoPayload = RunnerApi.ParDoPayload.parseFrom(parDoPTransform.getSpec().getPayload());
      } catch (InvalidProtocolBufferException exc) {
        throw new RuntimeException("ParDo did not have a ParDoPayload", exc);
      }

      // Skip any ParDo that doesn't have a side input.
      if (parDoPayload.getSideInputsMap().isEmpty()) {
        continue;
      }

      String mainInputPCollectionLocalName =
          Iterables.getOnlyElement(
              Sets.difference(
                  parDoPTransform.getInputsMap().keySet(),
                  parDoPayload.getSideInputsMap().keySet()));

      RunnerApi.WindowingStrategy windowingStrategyProto =
          pipeline
              .getComponents()
              .getWindowingStrategiesOrThrow(
                  pipeline
                      .getComponents()
                      .getPcollectionsOrThrow(
                          parDoPTransform.getInputsOrThrow(mainInputPCollectionLocalName))
                      .getWindowingStrategyId());

      WindowingStrategy windowingStrategy;
      try {
        windowingStrategy =
            WindowingStrategyTranslation.fromProto(windowingStrategyProto, rehydratedComponents);
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException(
            String.format("Unable to decode windowing strategy %s.", windowingStrategyProto), e);
      }

      // Gather all the side input window mapping fns which we need to request the SDK to map
      ImmutableMap.Builder<PCollectionView<?>, RunnerApi.FunctionSpec>
          pCollectionViewsToWindowMapingsFns = ImmutableMap.builder();
      parDoPayload
          .getSideInputsMap()
          .forEach(
              (sideInputTag, sideInput) ->
                  pCollectionViewsToWindowMapingsFns.put(
                      RegisterNodeFunction.transformSideInputForRunner(
                          pipeline, parDoPTransform, sideInputTag, sideInput),
                      sideInput.getWindowMappingFn()));
      Node streamingSideInputWindowHandlerNode =
          FetchAndFilterStreamingSideInputsNode.create(
              windowingStrategy,
              pCollectionViewsToWindowMapingsFns.build(),
              NameContext.create(
                  null,
                  node.getParallelInstruction().getOriginalName(),
                  node.getParallelInstruction().getSystemName(),
                  node.getParallelInstruction().getName()));

      // Rewire the graph such that streaming side inputs ParDos are preceded by a
      // node which filters any side inputs that aren't ready and fetches any ready side inputs.
      Edge mainInput = Iterables.getOnlyElement(network.inEdges(node));
      InstructionOutputNode predecessor =
          (InstructionOutputNode) network.incidentNodes(mainInput).source();
      InstructionOutputNode predecessorCopy =
          InstructionOutputNode.create(
              predecessor.getInstructionOutput(), predecessor.getPcollectionId());
      network.removeEdge(mainInput);
      network.addNode(streamingSideInputWindowHandlerNode);
      network.addNode(predecessorCopy);
      network.addEdge(predecessor, streamingSideInputWindowHandlerNode, mainInput.clone());
      network.addEdge(streamingSideInputWindowHandlerNode, predecessorCopy, mainInput.clone());
      network.addEdge(predecessorCopy, node, mainInput.clone());
    }
    return network;
  }
}
