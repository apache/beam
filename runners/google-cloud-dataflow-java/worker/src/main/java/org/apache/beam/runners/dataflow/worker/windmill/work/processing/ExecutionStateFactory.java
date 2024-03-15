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
package org.apache.beam.runners.dataflow.worker.windmill.work.processing;

import com.google.api.services.dataflow.model.MapTask;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.internal.CustomSources;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionContext;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.DataflowMapTaskExecutor;
import org.apache.beam.runners.dataflow.worker.DataflowMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.IntrinsicMapTaskExecutorFactory;
import org.apache.beam.runners.dataflow.worker.ReaderCache;
import org.apache.beam.runners.dataflow.worker.ReaderRegistry;
import org.apache.beam.runners.dataflow.worker.SinkRegistry;
import org.apache.beam.runners.dataflow.worker.StreamingModeExecutionContext;
import org.apache.beam.runners.dataflow.worker.WindmillKeyedWorkItem;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.MapTaskToNetworkFunction;
import org.apache.beam.runners.dataflow.worker.graph.Networks;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.InstructionOutputNode;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.ParallelInstructionNode;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutionState;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.util.common.worker.MapTaskExecutor;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputObjectAndByteCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReadOperation;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.MutableNetwork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
@Internal
public final class ExecutionStateFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutionStateFactory.class);
  private static final int CUSTOM_SOURCE_BYTES_READ_SAMPLING_PERIOD = 100;

  private final Supplier<ReaderCache> readerCache;
  private final Supplier<Map<String, String>> stateNameMap;
  private final Function<String, WindmillStateCache.ForComputation> perComputationStateCacheFactory;
  private final ReaderRegistry readerRegistry;
  private final SinkRegistry sinkRegistry;
  private final PipelineOptions options;
  private final CounterSet pendingDeltaCounters;
  private final IdGenerator idGenerator;
  private final Function<MapTask, MutableNetwork<Node, Edge>> mapTaskToNetworkFn;
  private final DataflowExecutionStateSampler sampler;
  private final DataflowMapTaskExecutorFactory internalFactory;
  private final long maxSinkBytes;

  private ExecutionStateFactory(
      Supplier<ReaderCache> readerCache,
      Supplier<Map<String, String>> stateNameMap,
      Function<String, WindmillStateCache.ForComputation> perComputationStateCacheFactory,
      ReaderRegistry readerRegistry,
      SinkRegistry sinkRegistry,
      PipelineOptions options,
      CounterSet pendingDeltaCounters,
      IdGenerator idGenerator,
      Function<MapTask, MutableNetwork<Node, Edge>> mapTaskToNetworkFn,
      DataflowExecutionStateSampler sampler,
      DataflowMapTaskExecutorFactory internalFactory,
      long maxSinkBytes) {
    this.readerCache = readerCache;
    this.stateNameMap = stateNameMap;
    this.perComputationStateCacheFactory = perComputationStateCacheFactory;
    this.readerRegistry = readerRegistry;
    this.sinkRegistry = sinkRegistry;
    this.options = options;
    this.pendingDeltaCounters = pendingDeltaCounters;
    this.idGenerator = idGenerator;
    this.mapTaskToNetworkFn = mapTaskToNetworkFn;
    this.sampler = sampler;
    this.internalFactory = internalFactory;
    this.maxSinkBytes = maxSinkBytes;
  }

  public static ExecutionStateFactory createDefault(
      Supplier<ReaderCache> readerCache,
      Supplier<Map<String, String>> stateNameMap,
      Function<String, WindmillStateCache.ForComputation> perComputationStateCacheFactory,
      PipelineOptions options,
      CounterSet pendingDeltaCounters,
      IdGenerator idGenerator,
      DataflowExecutionStateSampler sampler,
      DataflowMapTaskExecutorFactory mapTaskExecutorFactory,
      long maxSinkBytes) {
    return new ExecutionStateFactory(
        readerCache,
        stateNameMap,
        perComputationStateCacheFactory,
        ReaderRegistry.defaultRegistry(),
        SinkRegistry.defaultRegistry(),
        options,
        pendingDeltaCounters,
        idGenerator,
        new MapTaskToNetworkFunction(idGenerator),
        sampler,
        mapTaskExecutorFactory,
        maxSinkBytes);
  }

  private static ParallelInstructionNode extractReadNode(Set<Node> nodes) {
    return (ParallelInstructionNode)
        Iterables.find(
            nodes,
            node ->
                node instanceof ParallelInstructionNode
                    && ((ParallelInstructionNode) node).getParallelInstruction().getRead() != null);
  }

  /**
   * Extracts the userland key coder, if any, from the coder used in the initial read step of a
   * stage. This encodes many assumptions about how the streaming execution context works.
   */
  private static Optional<Coder<?>> extractKeyCoder(Coder<?> readCoder) {
    if (!(readCoder instanceof WindowedValue.WindowedValueCoder)) {
      throw new RuntimeException(
          String.format(
              "Expected coder for streaming read to be %s, but received %s",
              WindowedValue.WindowedValueCoder.class.getSimpleName(), readCoder));
    }

    // Note that TimerOrElementCoder is a backwards-compatibility class
    // that is really a FakeKeyedWorkItemCoder
    Coder<?> valueCoder = ((WindowedValue.WindowedValueCoder<?>) readCoder).getValueCoder();

    if (valueCoder instanceof KvCoder<?, ?>) {
      return Optional.ofNullable(((KvCoder<?, ?>) valueCoder).getKeyCoder());
    }
    if (!(valueCoder instanceof WindmillKeyedWorkItem.FakeKeyedWorkItemCoder<?, ?>)) {
      return Optional.empty();
    }

    return Optional.ofNullable(
        ((WindmillKeyedWorkItem.FakeKeyedWorkItemCoder<?, ?>) valueCoder).getKeyCoder());
  }

  private static ReadOperation extractReadOperationUnchecked(MapTaskExecutor mapTaskExecutor) {
    try {
      return mapTaskExecutor.getReadOperation();
    } catch (Exception e) {
      throw new ExecutionStateCreationException(
          "Couldn't extract read operation from MapTaskExecutor.", e);
    }
  }

  /** Creates a new {@link ExecutionState}. */
  ExecutionState createExecutionState(
      String computationId,
      MapTask mapTask,
      StageInfo stageInfo,
      String workLatencyTrackingId,
      Map<String, String> stateNameMap) {
    DataflowExecutionContext.DataflowExecutionStateTracker executionStateTracker =
        new DataflowExecutionContext.DataflowExecutionStateTracker(
            sampler,
            stageInfo
                .executionStateRegistry()
                .getState(
                    NameContext.forStage(mapTask.getStageName()),
                    "other",
                    null,
                    ScopedProfiler.INSTANCE.emptyScope()),
            stageInfo.deltaCounters(),
            options,
            workLatencyTrackingId);
    StreamingModeExecutionContext context =
        new StreamingModeExecutionContext(
            pendingDeltaCounters,
            computationId,
            readerCache.get(),
            resolveStateNameMap(stateNameMap),
            perComputationStateCacheFactory.apply(computationId),
            stageInfo.metricsContainerRegistry(),
            executionStateTracker,
            stageInfo.executionStateRegistry(),
            maxSinkBytes);
    DataflowMapTaskExecutor mapTaskExecutor = newMapTaskExecutor(mapTask, context);

    return ExecutionState.builder()
        .setWorkExecutor(mapTaskExecutor)
        .setContext(context)
        .setExecutionStateTracker(executionStateTracker)
        .setKeyCoder(extractKeyCoder(getReadCoder(mapTaskExecutor, mapTask)))
        .build();
  }

  private Map<String, String> resolveStateNameMap(Map<String, String> otherStateNameMap) {
    return otherStateNameMap.isEmpty() ? stateNameMap.get() : otherStateNameMap;
  }

  private Coder<?> getReadCoder(MapTaskExecutor mapTaskExecutor, MapTask mapTask) {
    MutableNetwork<Node, Edge> mapTaskNetwork = mapTaskToNetworkFn.apply(mapTask);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Network as Graphviz .dot: {}", Networks.toDot(mapTaskNetwork));
    }
    ParallelInstructionNode readNode = extractReadNode(mapTaskNetwork.nodes());
    InstructionOutputNode readOutputNode =
        (InstructionOutputNode) Iterables.getOnlyElement(mapTaskNetwork.successors(readNode));
    ReadOperation readOperation = extractReadOperationUnchecked(mapTaskExecutor);
    // Disable progress updates since its results are unused for streaming
    // and involves starting a thread.
    readOperation.setProgressUpdatePeriodMs(ReadOperation.DONT_UPDATE_PERIODICALLY);
    Preconditions.checkState(
        mapTaskExecutor.supportsRestart(),
        "Streaming runner requires all operations support restart.");

    Coder<?> readCoder =
        CloudObjects.coderFromCloudObject(
            CloudObject.fromSpec(readOutputNode.getInstructionOutput().getCodec()));

    if (isCustomSource(readNode)) {
      trackBytesReadForAutoscaling(
          mapTask, readOperation, readNode, readCoder, mapTaskExecutor.getOutputCounters());
    }

    return readCoder;
  }

  private boolean isCustomSource(ParallelInstructionNode readNode) {
    return CustomSources.class
        .getName()
        .equals(readNode.getParallelInstruction().getRead().getSource().getSpec().get("@type"));
  }

  private void trackBytesReadForAutoscaling(
      MapTask mapTask,
      ReadOperation readOperation,
      ParallelInstructionNode readNode,
      Coder<?> readCoder,
      CounterFactory outputCounters) {
    NameContext nameContext =
        NameContext.create(
            mapTask.getStageName(),
            readNode.getParallelInstruction().getOriginalName(),
            readNode.getParallelInstruction().getSystemName(),
            readNode.getParallelInstruction().getName());
    String counterName = StreamingWorkExecutor.getSourceBytesProcessedCounterName(mapTask);
    readOperation.receivers[0].addOutputCounter(
        counterName,
        new OutputObjectAndByteCounter(
                new IntrinsicMapTaskExecutorFactory.ElementByteSizeObservableCoder<>(readCoder),
                outputCounters,
                nameContext)
            .setSamplingPeriod(CUSTOM_SOURCE_BYTES_READ_SAMPLING_PERIOD)
            .countBytes(counterName));
  }

  private DataflowMapTaskExecutor newMapTaskExecutor(
      MapTask mapTask, StreamingModeExecutionContext context) {
    return internalFactory.create(
        mapTaskToNetworkFn.apply(mapTask),
        options,
        mapTask.getStageName(),
        readerRegistry,
        sinkRegistry,
        context,
        pendingDeltaCounters,
        idGenerator);
  }

  private static class ExecutionStateCreationException extends RuntimeException {
    private ExecutionStateCreationException(String message, Throwable source) {
      super(message, source);
    }
  }
}
