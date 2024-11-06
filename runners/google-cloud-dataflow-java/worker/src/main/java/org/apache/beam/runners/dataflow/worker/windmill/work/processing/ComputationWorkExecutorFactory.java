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

import static org.apache.beam.runners.dataflow.DataflowRunner.hasExperiment;

import com.google.api.services.dataflow.model.MapTask;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.internal.CustomSources;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
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
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.MapTaskToNetworkFunction;
import org.apache.beam.runners.dataflow.worker.graph.Networks;
import org.apache.beam.runners.dataflow.worker.graph.Nodes;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationWorkExecutor;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfigHandle;
import org.apache.beam.runners.dataflow.worker.streaming.harness.environment.Environment;
import org.apache.beam.runners.dataflow.worker.util.common.worker.MapTaskExecutor;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OutputObjectAndByteCounter;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ReadOperation;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.graph.MutableNetwork;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory class for generating {@link ComputationWorkExecutor} instances. */
final class ComputationWorkExecutorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ComputationWorkExecutorFactory.class);
  private static final String DISABLE_SINK_BYTE_LIMIT_EXPERIMENT =
      "disable_limiting_bundle_sink_bytes";
  // Whether to throw an exception when processing output that violates any of the operational
  // limits.
  private static final String THROW_EXCEPTIONS_ON_LARGE_OUTPUT_EXPERIMENT =
      "throw_exceptions_on_large_output";

  private final DataflowWorkerHarnessOptions options;
  private final DataflowMapTaskExecutorFactory mapTaskExecutorFactory;
  private final ReaderCache readerCache;
  private final Function<String, WindmillStateCache.ForComputation> stateCacheFactory;
  private final ReaderRegistry readerRegistry;
  private final SinkRegistry sinkRegistry;
  private final DataflowExecutionStateSampler sampler;
  private final CounterSet pendingDeltaCounters;

  /**
   * Function which converts map tasks to their network representation for execution.
   *
   * <ul>
   *   <li>Translate the map task to a network representation.
   *   <li>Remove flatten instructions by rewiring edges.
   * </ul>
   */
  private final Function<MapTask, MutableNetwork<Node, Edge>> mapTaskToNetwork;

  private final long maxSinkBytes;
  private final IdGenerator idGenerator;
  private final StreamingGlobalConfigHandle globalConfigHandle;
  private final boolean throwExceptionOnLargeOutput;

  ComputationWorkExecutorFactory(
      DataflowWorkerHarnessOptions options,
      DataflowMapTaskExecutorFactory mapTaskExecutorFactory,
      ReaderCache readerCache,
      Function<String, WindmillStateCache.ForComputation> stateCacheFactory,
      DataflowExecutionStateSampler sampler,
      CounterSet pendingDeltaCounters,
      IdGenerator idGenerator,
      StreamingGlobalConfigHandle globalConfigHandle) {
    this.options = options;
    this.mapTaskExecutorFactory = mapTaskExecutorFactory;
    this.readerCache = readerCache;
    this.stateCacheFactory = stateCacheFactory;
    this.idGenerator = idGenerator;
    this.globalConfigHandle = globalConfigHandle;
    this.readerRegistry = ReaderRegistry.defaultRegistry();
    this.sinkRegistry = SinkRegistry.defaultRegistry();
    this.sampler = sampler;
    this.pendingDeltaCounters = pendingDeltaCounters;
    this.mapTaskToNetwork = new MapTaskToNetworkFunction(idGenerator);
    this.maxSinkBytes =
        hasExperiment(options, DISABLE_SINK_BYTE_LIMIT_EXPERIMENT)
            ? Long.MAX_VALUE
            : Environment.maxSinkBytes();
    this.throwExceptionOnLargeOutput =
        hasExperiment(options, THROW_EXCEPTIONS_ON_LARGE_OUTPUT_EXPERIMENT);
  }

  private static Nodes.ParallelInstructionNode extractReadNode(
      MutableNetwork<Node, Edge> mapTaskNetwork) {
    return (Nodes.ParallelInstructionNode)
        Iterables.find(
            mapTaskNetwork.nodes(),
            node ->
                node instanceof Nodes.ParallelInstructionNode
                    && ((Nodes.ParallelInstructionNode) node).getParallelInstruction().getRead()
                        != null);
  }

  private static boolean isCustomSource(Nodes.ParallelInstructionNode readNode) {
    return CustomSources.class
        .getName()
        .equals(readNode.getParallelInstruction().getRead().getSource().getSpec().get("@type"));
  }

  private static void trackAutoscalingBytesRead(
      MapTask mapTask,
      Nodes.ParallelInstructionNode readNode,
      Coder<?> readCoder,
      ReadOperation readOperation,
      MapTaskExecutor mapTaskExecutor,
      String counterName) {
    NameContext nameContext =
        NameContext.create(
            mapTask.getStageName(),
            readNode.getParallelInstruction().getOriginalName(),
            readNode.getParallelInstruction().getSystemName(),
            readNode.getParallelInstruction().getName());
    readOperation.receivers[0].addOutputCounter(
        counterName,
        new OutputObjectAndByteCounter(
                new IntrinsicMapTaskExecutorFactory.ElementByteSizeObservableCoder<>(readCoder),
                mapTaskExecutor.getOutputCounters(),
                nameContext)
            .setSamplingPeriod(100)
            .countBytes(counterName));
  }

  private static ReadOperation getValidatedReadOperation(MapTaskExecutor mapTaskExecutor) {
    ReadOperation readOperation = mapTaskExecutor.getReadOperation();
    // Disable progress updates since its results are unused for streaming
    // and involves starting a thread.
    readOperation.setProgressUpdatePeriodMs(ReadOperation.DONT_UPDATE_PERIODICALLY);
    Preconditions.checkState(
        mapTaskExecutor.supportsRestart(),
        "Streaming runner requires all operations support restart.");
    return readOperation;
  }

  ComputationWorkExecutor createComputationWorkExecutor(
      StageInfo stageInfo, ComputationState computationState, String workLatencyTrackingId) {
    MapTask mapTask = computationState.getMapTask();
    MutableNetwork<Node, Edge> mapTaskNetwork = mapTaskToNetwork.apply(mapTask);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Network as Graphviz .dot: {}", Networks.toDot(mapTaskNetwork));
    }

    Nodes.ParallelInstructionNode readNode = extractReadNode(mapTaskNetwork);
    Nodes.InstructionOutputNode readOutputNode =
        (Nodes.InstructionOutputNode) Iterables.getOnlyElement(mapTaskNetwork.successors(readNode));

    DataflowExecutionContext.DataflowExecutionStateTracker executionStateTracker =
        createExecutionStateTracker(stageInfo, mapTask, workLatencyTrackingId);
    StreamingModeExecutionContext context =
        createExecutionContext(computationState, stageInfo, executionStateTracker);
    DataflowMapTaskExecutor mapTaskExecutor =
        createMapTaskExecutor(context, mapTask, mapTaskNetwork);
    ReadOperation readOperation = getValidatedReadOperation(mapTaskExecutor);

    Coder<?> readCoder =
        CloudObjects.coderFromCloudObject(
            CloudObject.fromSpec(readOutputNode.getInstructionOutput().getCodec()));
    Coder<?> keyCoder = extractKeyCoder(readCoder);

    // If using a custom source, count bytes read for autoscaling.
    if (isCustomSource(readNode)) {
      trackAutoscalingBytesRead(
          mapTask,
          readNode,
          readCoder,
          readOperation,
          mapTaskExecutor,
          computationState.sourceBytesProcessCounterName());
    }

    ComputationWorkExecutor.Builder executionStateBuilder =
        ComputationWorkExecutor.builder()
            .setWorkExecutor(mapTaskExecutor)
            .setContext(context)
            .setExecutionStateTracker(executionStateTracker);

    if (keyCoder != null) {
      executionStateBuilder.setKeyCoder(keyCoder);
    }

    return executionStateBuilder.build();
  }

  /**
   * Extracts the userland key coder, if any, from the coder used in the initial read step of a
   * stage. This encodes many assumptions about how the streaming execution context works.
   */
  private @Nullable Coder<?> extractKeyCoder(Coder<?> readCoder) {
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
      return ((KvCoder<?, ?>) valueCoder).getKeyCoder();
    }
    if (!(valueCoder instanceof WindmillKeyedWorkItem.FakeKeyedWorkItemCoder<?, ?>)) {
      return null;
    }

    return ((WindmillKeyedWorkItem.FakeKeyedWorkItemCoder<?, ?>) valueCoder).getKeyCoder();
  }

  private StreamingModeExecutionContext createExecutionContext(
      ComputationState computationState,
      StageInfo stageInfo,
      DataflowExecutionContext.DataflowExecutionStateTracker executionStateTracker) {
    String computationId = computationState.getComputationId();
    return new StreamingModeExecutionContext(
        pendingDeltaCounters,
        computationId,
        readerCache,
        computationState.getTransformUserNameToStateFamily(),
        stateCacheFactory.apply(computationId),
        stageInfo.metricsContainerRegistry(),
        executionStateTracker,
        stageInfo.executionStateRegistry(),
        globalConfigHandle,
        maxSinkBytes,
        throwExceptionOnLargeOutput);
  }

  private DataflowMapTaskExecutor createMapTaskExecutor(
      StreamingModeExecutionContext context,
      MapTask mapTask,
      MutableNetwork<Node, Edge> mapTaskNetwork) {
    return mapTaskExecutorFactory.create(
        mapTaskNetwork,
        options,
        mapTask.getStageName(),
        readerRegistry,
        sinkRegistry,
        context,
        pendingDeltaCounters,
        idGenerator);
  }

  private DataflowExecutionContext.DataflowExecutionStateTracker createExecutionStateTracker(
      StageInfo stageInfo, MapTask mapTask, String workLatencyTrackingId) {
    return new DataflowExecutionContext.DataflowExecutionStateTracker(
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
  }
}
