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
package org.apache.beam.runners.dataflow.worker.windmill.work.processing.context;

import com.google.api.services.dataflow.model.MapTask;
import java.util.Map;
import java.util.function.Function;
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
import org.apache.beam.runners.dataflow.worker.WindmillKeyedWorkItem;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.graph.Edges;
import org.apache.beam.runners.dataflow.worker.graph.Networks;
import org.apache.beam.runners.dataflow.worker.graph.Nodes;
import org.apache.beam.runners.dataflow.worker.options.StreamingDataflowWorkerOptions;
import org.apache.beam.runners.dataflow.worker.profiler.ScopedProfiler;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutionState;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
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

/** Factory for generating new {@link ExecutionState} objects for user processing. */
public final class StreamingExecutionStateFactory {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingExecutionStateFactory.class);
  // Per-key cache of active Reader objects in use by this process.
  private final DataflowMapTaskExecutorFactory mapTaskExecutorFactory;
  private final Function<MapTask, MutableNetwork<Nodes.Node, Edges.Edge>> mapTaskToNetwork;
  private final Map<String, String> stateNameMap;
  private final Function<String, WindmillStateCache.ForComputation> perComputationStateCacheFactory;
  private final ReaderCache readerCache;
  private final CounterSet counterFactory;
  private final DataflowExecutionStateSampler sampler;
  private final IdGenerator idGenerator;
  private final ReaderRegistry readerRegistry;
  private final SinkRegistry sinkRegistry;
  private final StreamingDataflowWorkerOptions options;
  private final long sinkByteLimit;

  public StreamingExecutionStateFactory(
      DataflowMapTaskExecutorFactory mapTaskExecutorFactory,
      Function<MapTask, MutableNetwork<Nodes.Node, Edges.Edge>> mapTaskToNetwork,
      Map<String, String> stateNameMap,
      Function<String, WindmillStateCache.ForComputation> perComputationStateCacheFactory,
      ReaderCache readerCache,
      CounterSet counterFactory,
      DataflowExecutionStateSampler sampler,
      IdGenerator idGenerator,
      ReaderRegistry readerRegistry,
      SinkRegistry sinkRegistry,
      StreamingDataflowWorkerOptions options,
      long sinkByteLimit) {
    this.mapTaskExecutorFactory = mapTaskExecutorFactory;
    this.mapTaskToNetwork = mapTaskToNetwork;
    this.stateNameMap = stateNameMap;
    this.perComputationStateCacheFactory = perComputationStateCacheFactory;
    this.readerCache = readerCache;
    this.counterFactory = counterFactory;
    this.sampler = sampler;
    this.idGenerator = idGenerator;
    this.readerRegistry = readerRegistry;
    this.sinkRegistry = sinkRegistry;
    this.options = options;
    this.sinkByteLimit = sinkByteLimit;
  }

  /**
   * Extracts the userland key coder, if any, from the coder used in the initial read step of a
   * stage. This encodes many assumptions about how the streaming execution context works.
   */
  private static @Nullable Coder<?> extractKeyCoder(Coder<?> readCoder) {
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
    return new StreamingModeExecutionContext(
        counterFactory,
        computationState.getComputationId(),
        readerCache,
        !computationState.getTransformUserNameToStateFamily().isEmpty()
            ? computationState.getTransformUserNameToStateFamily()
            : stateNameMap,
        perComputationStateCacheFactory.apply(computationState.getComputationId()),
        stageInfo.metricsContainerRegistry(),
        executionStateTracker,
        stageInfo.executionStateRegistry(),
        sinkByteLimit);
  }

  /**
   * Return an existing {@link ExecutionState} if one is present of generate or create a new one.
   */
  public ExecutionState getOrCreateExecutionState(
      MapTask mapTask,
      StageInfo stageInfo,
      ComputationState computationState,
      Work work,
      String counterName) {
    return computationState
        .getExecutionState()
        .orElseGet(
            () -> createExecutionState(mapTask, stageInfo, computationState, work, counterName));
  }

  private ExecutionState createExecutionState(
      MapTask mapTask,
      StageInfo stageInfo,
      ComputationState computationState,
      Work work,
      String counterName) {
    MutableNetwork<Nodes.Node, Edges.Edge> mapTaskNetwork = mapTaskToNetwork.apply(mapTask);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Network as Graphviz .dot: {}", Networks.toDot(mapTaskNetwork));
    }
    Nodes.ParallelInstructionNode readNode =
        (Nodes.ParallelInstructionNode)
            Iterables.find(
                mapTaskNetwork.nodes(),
                node ->
                    node instanceof Nodes.ParallelInstructionNode
                        && ((Nodes.ParallelInstructionNode) node).getParallelInstruction().getRead()
                            != null);
    Nodes.InstructionOutputNode readOutputNode =
        (Nodes.InstructionOutputNode) Iterables.getOnlyElement(mapTaskNetwork.successors(readNode));
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
            work.getLatencyTrackingId());
    StreamingModeExecutionContext context =
        createExecutionContext(computationState, stageInfo, executionStateTracker);
    DataflowMapTaskExecutor mapTaskExecutor =
        mapTaskExecutorFactory.create(
            mapTaskNetwork,
            options,
            mapTask.getStageName(),
            readerRegistry,
            sinkRegistry,
            context,
            counterFactory,
            idGenerator);
    ReadOperation readOperation = mapTaskExecutor.getReadOperation();
    // Disable progress updates since its results are unused  for streaming
    // and involves starting a thread.
    readOperation.setProgressUpdatePeriodMs(ReadOperation.DONT_UPDATE_PERIODICALLY);
    Preconditions.checkState(
        mapTaskExecutor.supportsRestart(),
        "Streaming runner requires all operations support restart.");

    Coder<?> readCoder =
        CloudObjects.coderFromCloudObject(
            CloudObject.fromSpec(readOutputNode.getInstructionOutput().getCodec()));

    // If using a custom source, count bytes read for autoscaling.
    if (CustomSources.class
        .getName()
        .equals(readNode.getParallelInstruction().getRead().getSource().getSpec().get("@type"))) {
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

    return ExecutionState.builder()
        .setWorkExecutor(mapTaskExecutor)
        .setContext(context)
        .setExecutionStateTracker(executionStateTracker)
        .setKeyCoder(extractKeyCoder(readCoder))
        .build();
  }
}
