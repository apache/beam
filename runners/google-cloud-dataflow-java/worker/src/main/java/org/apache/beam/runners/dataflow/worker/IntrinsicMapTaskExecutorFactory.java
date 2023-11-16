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
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.runners.core.ElementByteSizeObservable;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.Edges.MultiOutputInfoEdge;
import org.apache.beam.runners.dataflow.worker.graph.Networks;
import org.apache.beam.runners.dataflow.worker.graph.Networks.TypeSafeNodeFunction;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.OperationNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.OutputReceiverNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.MutableNetwork;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.Network;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates a {@link DataflowMapTaskExecutor} from a {@link MapTask} definition. */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class IntrinsicMapTaskExecutorFactory implements DataflowMapTaskExecutorFactory {
  private static final Logger LOG = LoggerFactory.getLogger(IntrinsicMapTaskExecutorFactory.class);

  public static IntrinsicMapTaskExecutorFactory defaultFactory() {
    return new IntrinsicMapTaskExecutorFactory();
  }

  private IntrinsicMapTaskExecutorFactory() {}

  /**
   * Creates a new {@link DataflowMapTaskExecutor} from the given {@link MapTask} definition using
   * the provided {@link ReaderFactory}.
   */
  @Override
  public DataflowMapTaskExecutor create(
      MutableNetwork<Node, Edge> network,
      PipelineOptions options,
      String stageName,
      ReaderFactory readerFactory,
      SinkFactory sinkFactory,
      DataflowExecutionContext<?> executionContext,
      CounterSet counterSet,
      IdGenerator idGenerator) {

    // Swap out all the InstructionOutput nodes with OutputReceiver nodes
    Networks.replaceDirectedNetworkNodes(
        network, createOutputReceiversTransform(stageName, counterSet));

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

    return IntrinsicMapTaskExecutor.withSharedCounterSet(
        topoSortedOperations, counterSet, executionContext.getExecutionStateTracker());
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
