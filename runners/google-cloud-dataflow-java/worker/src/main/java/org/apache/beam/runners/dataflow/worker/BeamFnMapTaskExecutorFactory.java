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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.util.Throwables;
import com.google.api.services.dataflow.model.InstructionOutput;
import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.ParDoInstruction;
import com.google.api.services.dataflow.model.ParallelInstruction;
import com.google.api.services.dataflow.model.PartialGroupByKeyInstruction;
import com.google.api.services.dataflow.model.ReadInstruction;
import com.google.api.services.dataflow.model.Source;
import com.google.api.services.dataflow.model.WriteInstruction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.ElementByteSizeObservable;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext.DataflowStepContext;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.fn.control.BeamFnMapTaskExecutor;
import org.apache.beam.runners.dataflow.worker.fn.control.ProcessRemoteBundleOperation;
import org.apache.beam.runners.dataflow.worker.fn.control.RegisterAndProcessBundleOperation;
import org.apache.beam.runners.dataflow.worker.fn.data.RemoteGrpcPortReadOperation;
import org.apache.beam.runners.dataflow.worker.fn.data.RemoteGrpcPortWriteOperation;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.MultiOutputInfoEdge;
import org.apache.beam.runners.dataflow.worker.graph.Networks;
import org.apache.beam.runners.dataflow.worker.graph.Networks.TypeSafeNodeFunction;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ExecutableStageNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.FetchAndFilterStreamingSideInputsNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.OperationNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.OutputReceiverNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.RegisterRequestNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.RemoteGrpcPortNode;
import org.apache.beam.runners.dataflow.worker.util.CloudSourceUtils;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ElementCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.FlattenOperation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Operation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OperationContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputReceiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoOperation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReadOperation;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.runners.dataflow.worker.util.common.worker.WriteOperation;
import org.apache.beam.runners.fnexecution.GrpcFnServer;
import org.apache.beam.runners.fnexecution.control.InstructionRequestHandler;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.SingleEnvironmentInstanceJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.runners.fnexecution.data.GrpcDataService;
import org.apache.beam.runners.fnexecution.environment.StaticRemoteEnvironmentFactory;
import org.apache.beam.runners.fnexecution.state.GrpcStateService;
import org.apache.beam.runners.fnexecution.state.StateDelegator;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableTable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.Network;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates a {@link DataflowMapTaskExecutor} from a {@link MapTask} definition. */
public class BeamFnMapTaskExecutorFactory implements DataflowMapTaskExecutorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(BeamFnMapTaskExecutorFactory.class);

  public static BeamFnMapTaskExecutorFactory defaultFactory() {
    return new BeamFnMapTaskExecutorFactory();
  }

  private BeamFnMapTaskExecutorFactory() {}

  /**
   * Creates a new {@link DataflowMapTaskExecutor} from the given {@link MapTask} definition using
   * the provided {@link ReaderFactory}.
   */
  @Override
  public DataflowMapTaskExecutor create(
      InstructionRequestHandler instructionRequestHandler,
      GrpcFnServer<GrpcDataService> grpcDataFnServer,
      Endpoints.ApiServiceDescriptor dataApiServiceDescriptor,
      GrpcFnServer<GrpcStateService> grpcStateFnServer,
      MutableNetwork<Node, Edge> network,
      PipelineOptions options,
      String stageName,
      ReaderFactory readerFactory,
      SinkFactory sinkFactory,
      DataflowExecutionContext<?> executionContext,
      CounterSet counterSet,
      IdGenerator idGenerator) {

    // TODO: remove this once we trust the code paths
    checkArgument(
        DataflowRunner.hasExperiment(options.as(DataflowPipelineDebugOptions.class), "beam_fn_api"),
        "%s should only be used when beam_fn_api is enabled",
        getClass().getSimpleName());

    // Swap out all the InstructionOutput nodes with OutputReceiver nodes
    Networks.replaceDirectedNetworkNodes(
        network, createOutputReceiversTransform(stageName, counterSet));

    if (DataflowRunner.hasExperiment(
        options.as(DataflowPipelineDebugOptions.class), "use_executable_stage_bundle_execution")) {
      LOG.debug("Using SingleEnvironmentInstanceJobBundleFactory");
      JobBundleFactory jobBundleFactory =
          SingleEnvironmentInstanceJobBundleFactory.create(
              StaticRemoteEnvironmentFactory.forService(instructionRequestHandler),
              grpcDataFnServer,
              grpcStateFnServer,
              idGenerator);
      // If the use_executable_stage_bundle_execution is enabled, use ExecutableStage instead.
      Networks.replaceDirectedNetworkNodes(
          network,
          createOperationTransformForExecutableStageNode(
              network, stageName, executionContext, jobBundleFactory));
    } else {
      // Swap out all the RegisterFnRequest nodes with Operation nodes
      Networks.replaceDirectedNetworkNodes(
          network,
          createOperationTransformForRegisterFnNodes(
              idGenerator,
              instructionRequestHandler,
              grpcStateFnServer.getService(),
              stageName,
              executionContext));
      // Swap out all the RemoteGrpcPort nodes with Operation nodes, note that it is expected
      // that the RegisterFnRequest nodes have already been replaced.
      Networks.replaceDirectedNetworkNodes(
          network,
          createOperationTransformForGrpcPortNodes(
              network,
              grpcDataFnServer.getService(),
              // TODO: Set NameContext properly for these operations.
              executionContext.createOperationContext(
                  NameContext.create(stageName, stageName, stageName, stageName))));
    }

    // Swap out all the FetchAndFilterStreamingSideInput nodes with operation nodes
    Networks.replaceDirectedNetworkNodes(
        network,
        createOperationTransformForFetchAndFilterStreamingSideInputNodes(
            network,
            idGenerator,
            instructionRequestHandler,
            grpcDataFnServer.getService(),
            dataApiServiceDescriptor,
            executionContext,
            stageName));

    // Swap out all the ParallelInstruction nodes with Operation nodes
    Networks.replaceDirectedNetworkNodes(
        network,
        createOperationTransformForParallelInstructionNodes(
            stageName, network, options, readerFactory, sinkFactory, executionContext));

    // Collect all the operations within the network and attach all the operations as receivers
    // to preceding output receivers.
    List<Operation> topoSortedOperations = new ArrayList<>();
    for (OperationNode node :
        Iterables.filter(Networks.topologicalOrder(network), OperationNode.class)) {
      topoSortedOperations.add(node.getOperation());

      for (Node predecessor :
          Iterables.filter(network.predecessors(node), OutputReceiverNode.class)) {
        ((OutputReceiverNode) predecessor)
            .getOutputReceiver()
            .addOutput((Receiver) node.getOperation());
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.info("Map task network: {}", Networks.toDot(network));
    }

    return BeamFnMapTaskExecutor.withSharedCounterSet(
        topoSortedOperations, counterSet, executionContext.getExecutionStateTracker());
  }

  private Function<Node, Node> createOperationTransformForFetchAndFilterStreamingSideInputNodes(
      MutableNetwork<Node, Edge> network,
      IdGenerator idGenerator,
      InstructionRequestHandler instructionRequestHandler,
      FnDataService beamFnDataService,
      Endpoints.ApiServiceDescriptor dataApiServiceDescriptor,
      DataflowExecutionContext executionContext,
      String stageName) {
    return new TypeSafeNodeFunction<FetchAndFilterStreamingSideInputsNode>(
        FetchAndFilterStreamingSideInputsNode.class) {

      @Override
      public Node typedApply(FetchAndFilterStreamingSideInputsNode input) {
        OutputReceiverNode output =
            (OutputReceiverNode) Iterables.getOnlyElement(network.successors(input));
        DataflowOperationContext operationContext =
            executionContext.createOperationContext(
                NameContext.create(
                    stageName,
                    input.getNameContext().originalName(),
                    input.getNameContext().systemName(),
                    input.getNameContext().userName()));
        return OperationNode.create(
            new FetchAndFilterStreamingSideInputsOperation<>(
                new OutputReceiver[] {output.getOutputReceiver()},
                operationContext,
                instructionRequestHandler,
                beamFnDataService,
                dataApiServiceDescriptor,
                idGenerator,
                (Coder<WindowedValue<Object>>) output.getCoder(),
                (WindowingStrategy<?, BoundedWindow>) input.getWindowingStrategy(),
                executionContext.getStepContext(operationContext),
                input.getPCollectionViewsToWindowMappingFns()));
      }
    };
  }

  private Function<Node, Node> createOperationTransformForGrpcPortNodes(
      final Network<Node, Edge> network,
      final FnDataService beamFnDataService,
      final OperationContext context) {
    return new TypeSafeNodeFunction<RemoteGrpcPortNode>(RemoteGrpcPortNode.class) {
      @Override
      public Node typedApply(RemoteGrpcPortNode input) {
        RegisterAndProcessBundleOperation registerFnOperation =
            (RegisterAndProcessBundleOperation)
                Iterables.getOnlyElement(
                        Iterables.filter(network.adjacentNodes(input), OperationNode.class))
                    .getOperation();

        // The coder comes from the one and only adjacent output node
        Coder<?> coder =
            Iterables.getOnlyElement(
                    Iterables.filter(network.adjacentNodes(input), OutputReceiverNode.class))
                .getCoder();
        // We figure out whether we are outputting some where if the output node is a
        // successor.
        Iterable<OutputReceiverNode> outputReceiverNodes =
            Iterables.filter(network.successors(input), OutputReceiverNode.class);
        Operation operation;
        if (outputReceiverNodes.iterator().hasNext()) {
          OutputReceiver[] outputReceivers =
              new OutputReceiver[] {
                Iterables.getOnlyElement(outputReceiverNodes).getOutputReceiver()
              };

          operation =
              new RemoteGrpcPortReadOperation<>(
                  beamFnDataService,
                  input.getPrimitiveTransformId(),
                  registerFnOperation::getProcessBundleInstructionId,
                  (Coder) coder,
                  outputReceivers,
                  context);
        } else {
          operation =
              new RemoteGrpcPortWriteOperation<>(
                  beamFnDataService,
                  input.getPrimitiveTransformId(),
                  registerFnOperation::getProcessBundleInstructionId,
                  (Coder) coder,
                  context);
        }
        return OperationNode.create(operation);
      }
    };
  }

  private Function<Node, Node> createOperationTransformForExecutableStageNode(
      final Network<Node, Edge> network,
      final String stageName,
      final DataflowExecutionContext<?> executionContext,
      final JobBundleFactory jobBundleFactory) {
    return new TypeSafeNodeFunction<ExecutableStageNode>(ExecutableStageNode.class) {
      @Override
      public Node typedApply(ExecutableStageNode input) {
        StageBundleFactory stageBundleFactory =
            jobBundleFactory.forStage(input.getExecutableStage());
        Iterable<OutputReceiverNode> outputReceiverNodes =
            Iterables.filter(network.successors(input), OutputReceiverNode.class);

        Map<String, OutputReceiver> outputReceiverMap = new HashMap<>();
        Lists.newArrayList(outputReceiverNodes).stream()
            .forEach(
                outputReceiverNode ->
                    outputReceiverMap.put(
                        outputReceiverNode.getPcollectionId(),
                        outputReceiverNode.getOutputReceiver()));

        ImmutableMap.Builder<String, DataflowOperationContext>
            ptransformIdToOperationContextBuilder = ImmutableMap.builder();

        for (Map.Entry<String, NameContext> entry :
            input.getPTransformIdToPartialNameContextMap().entrySet()) {
          NameContext fullNameContext =
              NameContext.create(
                  stageName,
                  entry.getValue().originalName(),
                  entry.getValue().systemName(),
                  entry.getValue().userName());

          DataflowOperationContext operationContext =
              executionContext.createOperationContext(fullNameContext);
          ptransformIdToOperationContextBuilder.put(entry.getKey(), operationContext);
        }

        ImmutableMap<String, DataflowOperationContext> ptransformIdToOperationContexts =
            ptransformIdToOperationContextBuilder.build();

        ImmutableMap<String, SideInputReader> ptransformIdToSideInputReaders =
            buildPTransformIdToSideInputReadersMap(
                executionContext, input, ptransformIdToOperationContexts);

        Map<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>>
            ptransformIdToSideInputIdToPCollectionView = buildSideInputIdToPCollectionView(input);

        return OperationNode.create(
            new ProcessRemoteBundleOperation(
                input.getExecutableStage(),
                executionContext.createOperationContext(
                    NameContext.create(stageName, stageName, stageName, stageName)),
                stageBundleFactory,
                outputReceiverMap,
                ptransformIdToSideInputReaders,
                ptransformIdToSideInputIdToPCollectionView));
      }
    };
  }

  private Function<Node, Node> createOperationTransformForRegisterFnNodes(
      final IdGenerator idGenerator,
      final InstructionRequestHandler instructionRequestHandler,
      final StateDelegator beamFnStateDelegator,
      final String stageName,
      final DataflowExecutionContext<?> executionContext) {
    return new TypeSafeNodeFunction<RegisterRequestNode>(RegisterRequestNode.class) {
      @Override
      public Node typedApply(RegisterRequestNode input) {
        ImmutableMap.Builder<String, DataflowOperationContext>
            ptransformIdToOperationContextBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, DataflowStepContext> ptransformIdToStepContext =
            ImmutableMap.builder();
        for (Map.Entry<String, NameContext> entry :
            input.getPTransformIdToPartialNameContextMap().entrySet()) {
          NameContext fullNameContext =
              NameContext.create(
                  stageName,
                  entry.getValue().originalName(),
                  entry.getValue().systemName(),
                  entry.getValue().userName());

          DataflowOperationContext operationContext =
              executionContext.createOperationContext(fullNameContext);
          ptransformIdToOperationContextBuilder.put(entry.getKey(), operationContext);
          ptransformIdToStepContext.put(
              entry.getKey(), executionContext.getStepContext(operationContext));
        }

        ImmutableMap.Builder<String, NameContext> pcollectionIdToNameContext =
            ImmutableMap.builder();
        for (Map.Entry<String, NameContext> entry :
            input.getPCollectionToPartialNameContextMap().entrySet()) {
          pcollectionIdToNameContext.put(
              entry.getKey(),
              NameContext.create(
                  stageName,
                  entry.getValue().originalName(),
                  entry.getValue().systemName(),
                  entry.getValue().userName()));
        }

        ImmutableMap<String, DataflowOperationContext> ptransformIdToOperationContexts =
            ptransformIdToOperationContextBuilder.build();

        ImmutableMap<String, SideInputReader> ptransformIdToSideInputReaders =
            buildPTransformIdToSideInputReadersMap(
                executionContext, input, ptransformIdToOperationContexts);

        ImmutableTable<String, String, PCollectionView<?>>
            ptransformIdToSideInputIdToPCollectionView =
                buildPTransformIdToSideInputIdToPCollectionView(input);

        return OperationNode.create(
            new RegisterAndProcessBundleOperation(
                idGenerator,
                instructionRequestHandler,
                beamFnStateDelegator,
                input.getRegisterRequest(),
                ptransformIdToOperationContexts,
                ptransformIdToStepContext.build(),
                ptransformIdToSideInputReaders,
                ptransformIdToSideInputIdToPCollectionView,
                pcollectionIdToNameContext.build(),
                // TODO: Set NameContext properly for these operations.
                executionContext.createOperationContext(
                    NameContext.create(stageName, stageName, stageName, stageName))));
      }
    };
  }

  /** Returns a map from PTransform id to side input reader. */
  private static ImmutableMap<String, SideInputReader> buildPTransformIdToSideInputReadersMap(
      DataflowExecutionContext executionContext,
      RegisterRequestNode registerRequestNode,
      ImmutableMap<String, DataflowOperationContext> ptransformIdToOperationContexts) {

    ImmutableMap.Builder<String, SideInputReader> ptransformIdToSideInputReaders =
        ImmutableMap.builder();
    for (Map.Entry<String, Iterable<PCollectionView<?>>> ptransformIdToPCollectionView :
        registerRequestNode.getPTransformIdToPCollectionViewMap().entrySet()) {
      try {
        ptransformIdToSideInputReaders.put(
            ptransformIdToPCollectionView.getKey(),
            executionContext.getSideInputReader(
                // Note that the side input infos will only be populated for a batch pipeline
                registerRequestNode
                    .getPTransformIdToSideInputInfoMap()
                    .get(ptransformIdToPCollectionView.getKey()),
                ptransformIdToPCollectionView.getValue(),
                ptransformIdToOperationContexts.get(ptransformIdToPCollectionView.getKey())));
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
    return ptransformIdToSideInputReaders.build();
  }

  /** Returns a map from PTransform id to side input reader. */
  private static ImmutableMap<String, SideInputReader> buildPTransformIdToSideInputReadersMap(
      DataflowExecutionContext executionContext,
      ExecutableStageNode registerRequestNode,
      ImmutableMap<String, DataflowOperationContext> ptransformIdToOperationContexts) {

    ImmutableMap.Builder<String, SideInputReader> ptransformIdToSideInputReaders =
        ImmutableMap.builder();
    for (Map.Entry<String, Iterable<PCollectionView<?>>> ptransformIdToPCollectionView :
        registerRequestNode.getPTransformIdToPCollectionViewMap().entrySet()) {
      try {
        ptransformIdToSideInputReaders.put(
            ptransformIdToPCollectionView.getKey(),
            executionContext.getSideInputReader(
                // Note that the side input infos will only be populated for a batch pipeline
                registerRequestNode
                    .getPTransformIdToSideInputInfoMap()
                    .get(ptransformIdToPCollectionView.getKey()),
                ptransformIdToPCollectionView.getValue(),
                ptransformIdToOperationContexts.get(ptransformIdToPCollectionView.getKey())));
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
    return ptransformIdToSideInputReaders.build();
  }

  /**
   * Returns a table where the row key is the PTransform id, the column key is the side input id,
   * and the value is the corresponding PCollectionView.
   */
  private static ImmutableTable<String, String, PCollectionView<?>>
      buildPTransformIdToSideInputIdToPCollectionView(RegisterRequestNode registerRequestNode) {
    ImmutableTable.Builder<String, String, PCollectionView<?>>
        ptransformIdToSideInputIdToPCollectionViewBuilder = ImmutableTable.builder();
    for (Map.Entry<String, Iterable<PCollectionView<?>>> ptransformIdToPCollectionViews :
        registerRequestNode.getPTransformIdToPCollectionViewMap().entrySet()) {
      for (PCollectionView<?> pCollectionView : ptransformIdToPCollectionViews.getValue()) {
        ptransformIdToSideInputIdToPCollectionViewBuilder.put(
            ptransformIdToPCollectionViews.getKey(),
            pCollectionView.getTagInternal().getId(),
            pCollectionView);
      }
    }

    return ptransformIdToSideInputIdToPCollectionViewBuilder.build();
  }

  /** Returns a map where key is the SideInput id, value is PCollectionView. */
  private static Map<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>>
      buildSideInputIdToPCollectionView(ExecutableStageNode executableStageNode) {
    ImmutableMap.Builder<RunnerApi.ExecutableStagePayload.SideInputId, PCollectionView<?>>
        sideInputIdToPCollectionViewMapBuilder = ImmutableMap.builder();

    for (Map.Entry<String, Iterable<PCollectionView<?>>> ptransformIdToPCollectionViews :
        executableStageNode.getPTransformIdToPCollectionViewMap().entrySet()) {
      for (PCollectionView<?> pCollectionView : ptransformIdToPCollectionViews.getValue()) {
        sideInputIdToPCollectionViewMapBuilder.put(
            RunnerApi.ExecutableStagePayload.SideInputId.newBuilder()
                .setTransformId(ptransformIdToPCollectionViews.getKey())
                .setLocalName(pCollectionView.getTagInternal().getId())
                .build(),
            pCollectionView);
      }
    }

    return sideInputIdToPCollectionViewMapBuilder.build();
  }

  /**
   * Creates an {@link Operation} from the given {@link ParallelInstruction} definition using the
   * provided {@link ReaderFactory}.
   */
  Function<Node, Node> createOperationTransformForParallelInstructionNodes(
      final String stageName,
      final Network<Node, Edge> network,
      final PipelineOptions options,
      final ReaderFactory readerFactory,
      final SinkFactory sinkFactory,
      final DataflowExecutionContext<?> executionContext) {

    return new TypeSafeNodeFunction<ParallelInstructionNode>(ParallelInstructionNode.class) {
      @Override
      public Node typedApply(ParallelInstructionNode node) {
        ParallelInstruction instruction = node.getParallelInstruction();
        NameContext nameContext =
            NameContext.create(
                stageName,
                instruction.getOriginalName(),
                instruction.getSystemName(),
                instruction.getName());
        try {
          DataflowOperationContext context = executionContext.createOperationContext(nameContext);
          if (instruction.getRead() != null) {
            return createReadOperation(
                network, node, options, readerFactory, executionContext, context);
          } else if (instruction.getWrite() != null) {
            return createWriteOperation(node, options, sinkFactory, executionContext, context);
          } else if (instruction.getParDo() != null) {
            return createParDoOperation(network, node, options, executionContext, context);
          } else if (instruction.getPartialGroupByKey() != null) {
            return createPartialGroupByKeyOperation(
                network, node, options, executionContext, context);
          } else if (instruction.getFlatten() != null) {
            return createFlattenOperation(network, node, context);
          } else {
            throw new IllegalArgumentException(
                String.format("Unexpected instruction: %s", instruction));
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  OperationNode createReadOperation(
      Network<Node, Edge> network,
      ParallelInstructionNode node,
      PipelineOptions options,
      ReaderFactory readerFactory,
      DataflowExecutionContext<?> executionContext,
      DataflowOperationContext operationContext)
      throws Exception {

    ParallelInstruction instruction = node.getParallelInstruction();
    ReadInstruction read = instruction.getRead();

    Source cloudSource = CloudSourceUtils.flattenBaseSpecs(read.getSource());
    CloudObject sourceSpec = CloudObject.fromSpec(cloudSource.getSpec());
    Coder<?> coder =
        CloudObjects.coderFromCloudObject(CloudObject.fromSpec(cloudSource.getCodec()));
    NativeReader<?> reader =
        readerFactory.create(sourceSpec, coder, options, executionContext, operationContext);
    OutputReceiver[] receivers = getOutputReceivers(network, node);
    return OperationNode.create(ReadOperation.create(reader, receivers, operationContext));
  }

  OperationNode createWriteOperation(
      ParallelInstructionNode node,
      PipelineOptions options,
      SinkFactory sinkFactory,
      DataflowExecutionContext executionContext,
      DataflowOperationContext context)
      throws Exception {

    ParallelInstruction instruction = node.getParallelInstruction();
    WriteInstruction write = instruction.getWrite();
    Coder<?> coder =
        CloudObjects.coderFromCloudObject(CloudObject.fromSpec(write.getSink().getCodec()));
    CloudObject cloudSink = CloudObject.fromSpec(write.getSink().getSpec());
    Sink<?> sink = sinkFactory.create(cloudSink, coder, options, executionContext, context);
    return OperationNode.create(WriteOperation.create(sink, EMPTY_OUTPUT_RECEIVER_ARRAY, context));
  }

  private ParDoFnFactory parDoFnFactory = new DefaultParDoFnFactory();

  private OperationNode createParDoOperation(
      Network<Node, Edge> network,
      ParallelInstructionNode node,
      PipelineOptions options,
      DataflowExecutionContext<?> executionContext,
      DataflowOperationContext operationContext)
      throws Exception {

    ParallelInstruction instruction = node.getParallelInstruction();
    ParDoInstruction parDo = instruction.getParDo();

    TupleTag<?> mainOutputTag = tupleTag(parDo.getMultiOutputInfos().get(0));
    ImmutableMap.Builder<TupleTag<?>, Integer> outputTagsToReceiverIndicesBuilder =
        ImmutableMap.builder();
    int successorOffset = 0;
    for (Node successor : network.successors(node)) {
      for (Edge edge : network.edgesConnecting(node, successor)) {
        outputTagsToReceiverIndicesBuilder.put(
            tupleTag(((MultiOutputInfoEdge) edge).getMultiOutputInfo()), successorOffset);
      }
      successorOffset += 1;
    }
    ParDoFn fn =
        parDoFnFactory.create(
            options,
            CloudObject.fromSpec(parDo.getUserFn()),
            parDo.getSideInputs(),
            mainOutputTag,
            outputTagsToReceiverIndicesBuilder.build(),
            executionContext,
            operationContext);

    OutputReceiver[] receivers = getOutputReceivers(network, node);
    return OperationNode.create(new ParDoOperation(fn, receivers, operationContext));
  }

  private static <V> TupleTag<V> tupleTag(MultiOutputInfo multiOutputInfo) {
    return new TupleTag<>(multiOutputInfo.getTag());
  }

  <K> OperationNode createPartialGroupByKeyOperation(
      Network<Node, Edge> network,
      ParallelInstructionNode node,
      PipelineOptions options,
      DataflowExecutionContext<?> executionContext,
      DataflowOperationContext operationContext)
      throws Exception {

    ParallelInstruction instruction = node.getParallelInstruction();
    PartialGroupByKeyInstruction pgbk = instruction.getPartialGroupByKey();
    OutputReceiver[] receivers = getOutputReceivers(network, node);

    Coder<?> windowedCoder =
        CloudObjects.coderFromCloudObject(CloudObject.fromSpec(pgbk.getInputElementCodec()));
    if (!(windowedCoder instanceof WindowedValueCoder)) {
      throw new IllegalArgumentException(
          String.format(
              "unexpected kind of input coder for PartialGroupByKeyOperation: %s", windowedCoder));
    }
    Coder<?> elemCoder = ((WindowedValueCoder<?>) windowedCoder).getValueCoder();
    if (!(elemCoder instanceof KvCoder)) {
      throw new IllegalArgumentException(
          String.format(
              "unexpected kind of input element coder for PartialGroupByKeyOperation: %s",
              elemCoder));
    }

    @SuppressWarnings("unchecked")
    KvCoder<K, ?> keyedElementCoder = (KvCoder<K, ?>) elemCoder;

    CloudObject cloudUserFn =
        pgbk.getValueCombiningFn() != null
            ? CloudObject.fromSpec(pgbk.getValueCombiningFn())
            : null;
    ParDoFn fn =
        PartialGroupByKeyParDoFns.create(
            options,
            keyedElementCoder,
            cloudUserFn,
            pgbk.getSideInputs(),
            Arrays.<Receiver>asList(receivers),
            executionContext,
            operationContext);

    return OperationNode.create(new ParDoOperation(fn, receivers, operationContext));
  }

  OperationNode createFlattenOperation(
      Network<Node, Edge> network, ParallelInstructionNode node, OperationContext context) {
    OutputReceiver[] receivers = getOutputReceivers(network, node);
    return OperationNode.create(new FlattenOperation(receivers, context));
  }

  /**
   * Returns a function which can convert {@link InstructionOutput}s into {@link OutputReceiver}s.
   */
  static Function<Node, Node> createOutputReceiversTransform(
      final String stageName, final CounterFactory counterFactory) {
    return new TypeSafeNodeFunction<InstructionOutputNode>(InstructionOutputNode.class) {
      @Override
      public Node typedApply(InstructionOutputNode input) {
        InstructionOutput cloudOutput = input.getInstructionOutput();
        OutputReceiver outputReceiver = new OutputReceiver();
        Coder<?> coder =
            CloudObjects.coderFromCloudObject(CloudObject.fromSpec(cloudOutput.getCodec()));

        @SuppressWarnings("unchecked")
        ElementCounter outputCounter =
            new DataflowOutputCounter(
                cloudOutput.getName(),
                new ElementByteSizeObservableCoder<>(coder),
                counterFactory,
                NameContext.create(
                    stageName,
                    cloudOutput.getOriginalName(),
                    cloudOutput.getSystemName(),
                    cloudOutput.getName()));
        outputReceiver.addOutputCounter(outputCounter);

        return OutputReceiverNode.create(outputReceiver, coder, input.getPcollectionId());
      }
    };
  }

  private static final OutputReceiver[] EMPTY_OUTPUT_RECEIVER_ARRAY = new OutputReceiver[0];

  static OutputReceiver[] getOutputReceivers(Network<Node, Edge> network, Node node) {
    int outDegree = network.outDegree(node);
    if (outDegree == 0) {
      return EMPTY_OUTPUT_RECEIVER_ARRAY;
    }

    OutputReceiver[] receivers = new OutputReceiver[outDegree];
    Iterator<Node> receiverNodes = network.successors(node).iterator();
    int i = 0;
    do {
      receivers[i] = ((OutputReceiverNode) receiverNodes.next()).getOutputReceiver();
      i += 1;
    } while (receiverNodes.hasNext());

    return receivers;
  }

  /** Adapts a Coder to the ElementByteSizeObservable interface. */
  public static class ElementByteSizeObservableCoder<T> implements ElementByteSizeObservable<T> {
    final Coder<T> coder;

    public ElementByteSizeObservableCoder(Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(T value) {
      return coder.isRegisterByteSizeObserverCheap(value);
    }

    @Override
    public void registerByteSizeObserver(T value, ElementByteSizeObserver observer)
        throws Exception {
      coder.registerByteSizeObserver(value, observer);
    }
  }
}
