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

import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.WorkItem;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.function.Function;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.RemoteGrpcPort;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.SdkHarnessRegistry.SdkWorkerHarness;
import org.apache.beam.runners.dataflow.worker.apiary.FixMultiOutputInfosOnParDoInstructions;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.graph.CloneAmbiguousFlattensFunction;
import org.apache.beam.runners.dataflow.worker.graph.CreateExecutableStageNodeFunction;
import org.apache.beam.runners.dataflow.worker.graph.CreateRegisterFnOperationFunction;
import org.apache.beam.runners.dataflow.worker.graph.DeduceFlattenLocationsFunction;
import org.apache.beam.runners.dataflow.worker.graph.DeduceNodeLocationsFunction;
import org.apache.beam.runners.dataflow.worker.graph.Edges.Edge;
import org.apache.beam.runners.dataflow.worker.graph.LengthPrefixUnknownCoders;
import org.apache.beam.runners.dataflow.worker.graph.MapTaskToNetworkFunction;
import org.apache.beam.runners.dataflow.worker.graph.Networks;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.Node;
import org.apache.beam.runners.dataflow.worker.graph.Nodes.RemoteGrpcPortNode;
import org.apache.beam.runners.dataflow.worker.graph.RegisterNodeFunction;
import org.apache.beam.runners.dataflow.worker.graph.ReplacePgbkWithPrecombineFunction;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture.Capturable;
import org.apache.beam.runners.dataflow.worker.status.WorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.sdk.util.Weighted;
import org.apache.beam.sdk.util.WeightedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.graph.MutableNetwork;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a semi-abstract harness for executing WorkItem tasks in Java workers. Concrete
 * implementations need to implement a WorkUnitClient.
 *
 * <p>DataflowWorker presents one public interface, getAndPerformWork(), which uses the
 * WorkUnitClient to get work, execute it, and update the work.
 */
public class BatchDataflowWorker implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(BatchDataflowWorker.class);

  /** A client to get and update work items. */
  private final WorkUnitClient workUnitClient;

  /**
   * Pipeline options, initially provided via the constructor and partially provided via each work
   * work unit.
   */
  private final DataflowWorkerHarnessOptions options;

  /** The factory to create {@link DataflowMapTaskExecutor DataflowMapTaskExecutors}. */
  private final DataflowMapTaskExecutorFactory mapTaskExecutorFactory;

  /** The idGenerator to generate unique id globally. */
  private static final IdGenerator idGenerator = IdGenerators.decrementingLongs();

  /**
   * Function which converts map tasks to their network representation for execution.
   *
   * <p>It will:
   *
   * <ul>
   *   <li>Fix multi output infos to exist on all ParDo instructions.
   *   <li>Translate the map task to a network representation.
   * </ul>
   */
  private static final Function<MapTask, MutableNetwork<Node, Edge>> mapTaskToBaseNetwork =
      new FixMultiOutputInfosOnParDoInstructions(idGenerator)
          .andThen(new MapTaskToNetworkFunction(idGenerator));

  /** Registry of known {@link ReaderFactory ReaderFactories}. */
  private final ReaderRegistry readerRegistry = ReaderRegistry.defaultRegistry();

  /** Registry of known {@link SinkFactory SinkFactories}. */
  private final SinkRegistry sinkRegistry = SinkRegistry.defaultRegistry();

  /** A side input cache shared between all execution contexts. */
  private final Cache<?, WeightedValue<?>> sideInputDataCache;

  /**
   * A side input cache shared between all execution contexts. This cache is meant to store values
   * as weak references. This allows for insertion of logical keys with zero weight since they will
   * only be scoped to the lifetime of the value being cached.
   */
  private final Cache<?, ?> sideInputWeakReferenceCache;

  private static final int DEFAULT_STATUS_PORT = 8081;

  /** Status pages returning health of worker. */
  private WorkerStatusPages statusPages;

  /** Periodic sender of debug information to the debug capture service. */
  private DebugCapture.Manager debugCaptureManager = null;

  /**
   * A weight in "bytes" for the overhead of a {@link Weighted} wrapper in the cache. It is just an
   * approximation so it is OK for it to be fairly arbitrary as long as it is nonzero.
   */
  private static final int OVERHEAD_WEIGHT = 8;

  private static final long MEGABYTES = 1024 * 1024;

  /**
   * Limit the number of logical references. Weak references may never be cleared if the object is
   * long lived irrespective if the user actually is interested in the key lookup anymore.
   */
  private static final int MAX_LOGICAL_REFERENCES = 1_000_000;

  /** How many concurrent write operations to a cache should we allow. */
  private static final int CACHE_CONCURRENCY_LEVEL = 4 * Runtime.getRuntime().availableProcessors();

  private final SdkHarnessRegistry sdkHarnessRegistry;
  private final Function<MapTask, MutableNetwork<Node, Edge>> mapTaskToNetwork;

  private final MemoryMonitor memoryMonitor;
  private final Thread memoryMonitorThread;

  /**
   * Returns a {@link BatchDataflowWorker} configured to execute user functions via intrinsic Java
   * execution.
   *
   * <p>This is also known as the "legacy" or "pre-portability" approach. It is not yet deprecated
   * as there is not a compatible path forward for users.
   */
  static BatchDataflowWorker forBatchIntrinsicWorkerHarness(
      WorkUnitClient workUnitClient, DataflowWorkerHarnessOptions options) {
    return new BatchDataflowWorker(
        null,
        SdkHarnessRegistries.emptySdkHarnessRegistry(),
        workUnitClient,
        IntrinsicMapTaskExecutorFactory.defaultFactory(),
        options);
  }

  /**
   * Returns a {@link BatchDataflowWorker} configured to execute user functions via the Beam "Fn
   * API".
   *
   * <p>This is also known as the "portable" or "Beam model" approach.
   */
  static BatchDataflowWorker forBatchFnWorkerHarness(
      RunnerApi.@Nullable Pipeline pipeline,
      SdkHarnessRegistry sdkHarnessRegistry,
      WorkUnitClient workUnitClient,
      DataflowWorkerHarnessOptions options) {
    return new BatchDataflowWorker(
        pipeline,
        sdkHarnessRegistry,
        workUnitClient,
        BeamFnMapTaskExecutorFactory.defaultFactory(),
        options);
  }

  protected BatchDataflowWorker(
      RunnerApi.@Nullable Pipeline pipeline,
      SdkHarnessRegistry sdkHarnessRegistry,
      WorkUnitClient workUnitClient,
      DataflowMapTaskExecutorFactory mapTaskExecutorFactory,
      DataflowWorkerHarnessOptions options) {
    this.mapTaskExecutorFactory = mapTaskExecutorFactory;
    this.sdkHarnessRegistry = sdkHarnessRegistry;
    this.workUnitClient = workUnitClient;
    this.options = options;

    this.sideInputDataCache =
        CacheBuilder.newBuilder()
            .maximumWeight(options.getWorkerCacheMb() * MEGABYTES) // weights are in bytes
            .weigher(Weighers.fixedWeightKeys(OVERHEAD_WEIGHT))
            .softValues()
            .concurrencyLevel(CACHE_CONCURRENCY_LEVEL)
            .build();

    this.sideInputWeakReferenceCache =
        CacheBuilder.newBuilder()
            .maximumSize(MAX_LOGICAL_REFERENCES)
            .weakValues()
            .concurrencyLevel(CACHE_CONCURRENCY_LEVEL)
            .build();

    this.memoryMonitor = MemoryMonitor.fromOptions(options);
    this.statusPages =
        WorkerStatusPages.create(
            DEFAULT_STATUS_PORT, this.memoryMonitor, sdkHarnessRegistry::sdkHarnessesAreHealthy);

    if (!DataflowRunner.hasExperiment(options, "disable_debug_capture")) {
      this.debugCaptureManager =
          initializeAndStartDebugCaptureManager(options, statusPages.getDebugCapturePages());
    }

    // TODO: this conditional -> two implementations of common interface, or
    // param/injection
    if (DataflowRunner.hasExperiment(options, "beam_fn_api")) {
      Function<MutableNetwork<Node, Edge>, MutableNetwork<Node, Edge>> transformToRunnerNetwork;
      Function<MutableNetwork<Node, Edge>, Node> sdkFusedStage;
      Function<MutableNetwork<Node, Edge>, MutableNetwork<Node, Edge>> lengthPrefixUnknownCoders =
          LengthPrefixUnknownCoders::forSdkNetwork;
      if (DataflowRunner.hasExperiment(options, "use_executable_stage_bundle_execution")) {
        sdkFusedStage = new CreateExecutableStageNodeFunction(pipeline, idGenerator);
        transformToRunnerNetwork =
            new CreateRegisterFnOperationFunction(
                idGenerator,
                this::createPortNode,
                lengthPrefixUnknownCoders.andThen(sdkFusedStage),
                true);
      } else {
        sdkFusedStage =
            pipeline == null
                ? RegisterNodeFunction.withoutPipeline(
                    idGenerator,
                    sdkHarnessRegistry.beamFnStateApiServiceDescriptor(),
                    sdkHarnessRegistry.beamFnDataApiServiceDescriptor())
                : RegisterNodeFunction.forPipeline(
                    pipeline,
                    idGenerator,
                    sdkHarnessRegistry.beamFnStateApiServiceDescriptor(),
                    sdkHarnessRegistry.beamFnDataApiServiceDescriptor());
        transformToRunnerNetwork =
            new CreateRegisterFnOperationFunction(
                idGenerator,
                this::createPortNode,
                lengthPrefixUnknownCoders.andThen(sdkFusedStage),
                false);
      }
      mapTaskToNetwork =
          mapTaskToBaseNetwork
              .andThen(new ReplacePgbkWithPrecombineFunction())
              .andThen(new DeduceNodeLocationsFunction())
              .andThen(new DeduceFlattenLocationsFunction())
              .andThen(new CloneAmbiguousFlattensFunction())
              .andThen(transformToRunnerNetwork)
              .andThen(LengthPrefixUnknownCoders::andReplaceForRunnerNetwork);
    } else {
      mapTaskToNetwork = mapTaskToBaseNetwork;
    }

    this.memoryMonitorThread = startMemoryMonitorThread(memoryMonitor);

    ExecutionStateSampler.instance().start();
  }

  private static DebugCapture.Manager initializeAndStartDebugCaptureManager(
      DataflowWorkerHarnessOptions options, Collection<Capturable> debugCapturePages) {
    DebugCapture.Manager result = new DebugCapture.Manager(options, debugCapturePages);
    result.start();
    return result;
  }

  private static Thread startMemoryMonitorThread(MemoryMonitor memoryMonitor) {
    Thread result = new Thread(memoryMonitor);
    result.setDaemon(true);
    result.setPriority(Thread.MIN_PRIORITY);
    result.setName("MemoryMonitor");
    result.start();
    return result;
  }

  private Node createPortNode() {
    return RemoteGrpcPortNode.create(
        RemoteGrpcPort.newBuilder()
            .setApiServiceDescriptor(sdkHarnessRegistry.beamFnDataApiServiceDescriptor())
            .build(),
        idGenerator.getId());
  }

  /**
   * Gets WorkItem and performs it; returns true if work succeeded otherwise returns false. This
   * method will continuously attempt to get work until at least one work item is returned or the
   * service returns an error. We purposely do this because the service intelligently uses hanging
   * gets to be able to assign work as quickly as possible without the need for an exponential
   * backoff strategy for the work request calls.
   *
   * <p>getAndPerformWork may throw if there is a failure of the WorkUnitClient.
   */
  public boolean getAndPerformWork() throws IOException {
    while (true) {
      Optional<WorkItem> work = workUnitClient.getWorkItem();
      if (work.isPresent()) {
        WorkItemStatusClient statusProvider = new WorkItemStatusClient(workUnitClient, work.get());
        return doWork(work.get(), statusProvider);
      }
    }
  }

  /**
   * Performs the given work; returns true if successful.
   *
   * @throws IOException Only if the WorkUnitClient fails.
   */
  @VisibleForTesting
  boolean doWork(WorkItem workItem, WorkItemStatusClient workItemStatusClient) throws IOException {
    LOG.debug("Executing: {}", workItem);

    DataflowWorkExecutor worker = null;
    SdkWorkerHarness sdkWorkerHarness = sdkHarnessRegistry.getAvailableWorkerAndAssignWork();
    try {
      // Populate PipelineOptions with data from work unit.
      options.setProject(workItem.getProjectId());

      final String stageName;
      if (workItem.getMapTask() != null) {
        stageName = workItem.getMapTask().getStageName();
      } else if (workItem.getSourceOperationTask() != null) {
        stageName = workItem.getSourceOperationTask().getStageName();
      } else {
        throw new RuntimeException("Unknown kind of work item: " + workItem.toString());
      }

      CounterSet counterSet = new CounterSet();
      BatchModeExecutionContext executionContext =
          BatchModeExecutionContext.create(
              counterSet,
              sideInputDataCache,
              sideInputWeakReferenceCache,
              readerRegistry,
              options,
              stageName,
              String.valueOf(workItem.getId()));

      if (workItem.getMapTask() != null) {
        MutableNetwork<Node, Edge> network = mapTaskToNetwork.apply(workItem.getMapTask());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Network as Graphviz .dot: {}", Networks.toDot(network));
        }

        worker =
            mapTaskExecutorFactory.create(
                sdkWorkerHarness.getControlClientHandler(),
                sdkWorkerHarness.getGrpcDataFnServer(),
                sdkHarnessRegistry.beamFnDataApiServiceDescriptor(),
                sdkWorkerHarness.getGrpcStateFnServer(),
                network,
                options,
                stageName,
                readerRegistry,
                sinkRegistry,
                executionContext,
                counterSet,
                idGenerator);
      } else if (workItem.getSourceOperationTask() != null) {
        worker =
            SourceOperationExecutorFactory.create(
                options,
                workItem.getSourceOperationTask(),
                counterSet,
                executionContext,
                stageName);
      } else {
        throw new IllegalStateException("Work Item was neither a MapTask nor a SourceOperation");
      }
      workItemStatusClient.setWorker(worker, executionContext);

      DataflowWorkProgressUpdater progressUpdater =
          new DataflowWorkProgressUpdater(workItemStatusClient, workItem, worker);
      executeWork(worker, progressUpdater);
      workItemStatusClient.reportSuccess();
      return true;

    } catch (Throwable e) {
      workItemStatusClient.reportError(e);
      return false;

    } finally {
      if (worker != null) {
        try {
          worker.close();
        } catch (Exception exn) {
          LOG.warn(
              "Uncaught exception while closing worker. All work has already committed or "
                  + "been marked for retry.",
              exn);
        }
      }
      if (sdkWorkerHarness != null) {
        sdkHarnessRegistry.completeWork(sdkWorkerHarness);
      }
    }
  }

  /** Executes the work and report progress. For testing only. */
  void executeWork(DataflowWorkExecutor worker, DataflowWorkProgressUpdater progressUpdater)
      throws Exception {
    progressUpdater.startReportingProgress();
    // Blocks while executing the work.
    try {
      worker.execute();
    } finally {
      // stopReportingProgress can throw an exception if the final progress
      // update fails. For correctness, the task must then be marked as failed.
      progressUpdater.stopReportingProgress();
    }
  }

  /** Runs the status server to report worker health on the specified port. */
  public void startStatusServer() {
    statusPages.start();
  }

  /** Cleanup allocated resources. */
  @Override
  public void close() {
    // TODO: Implement proper cleanup logic.
    if (this.debugCaptureManager != null) {
      this.debugCaptureManager.stop();
    }

    this.memoryMonitor.stop();
    this.statusPages.stop();
    ExecutionStateSampler.instance().stop();

    long timeoutMilliSec = 5 * 1000;
    try {
      this.memoryMonitorThread.join(timeoutMilliSec);
    } catch (InterruptedException ex) {
      LOG.warn("Failed to wait for monitor thread to exit.", ex);
    }
    if (this.memoryMonitorThread.isAlive()) {
      LOG.warn("memoryMonitorThread didn't exit. Please, check for potential memory leaks.");
    }
  }
}
