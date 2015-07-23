/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.api.services.dataflow.model.MapTask;
import com.google.cloud.dataflow.sdk.io.UnboundedSource;
import com.google.cloud.dataflow.sdk.options.DataflowWorkerHarnessOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.worker.logging.DataflowWorkerLoggingInitializer;
import com.google.cloud.dataflow.sdk.runners.worker.logging.DataflowWorkerLoggingMDC;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.WindmillServerStub;
import com.google.cloud.dataflow.sdk.util.BoundedQueueExecutor;
import com.google.cloud.dataflow.sdk.util.StateFetcher;
import com.google.cloud.dataflow.sdk.util.StreamingModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.UserCodeException;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind;
import com.google.cloud.dataflow.sdk.util.common.Counter.CounterMean;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.MapTaskExecutor;
import com.google.cloud.dataflow.sdk.util.common.worker.ReadOperation;
import com.google.cloud.dataflow.sdk.util.state.WindmillStateReader;
import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Implements a Streaming Dataflow worker.
 */
public class StreamingDataflowWorker {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingDataflowWorker.class);
  // Maximum number of threads for processing.  Currently each thread processes one key at a time.
  static final int MAX_PROCESSING_THREADS = 300;
  static final long THREAD_EXPIRATION_TIME_SEC = 60;
  // Maximum work units retrieved from Windmill and queued before processing. Limiting this delays
  // retrieving extra work from Windmill without working on it, leading to better
  // prioritization / utilization.
  static final int MAX_WORK_UNITS_QUEUED = 100;
  static final long MAX_COMMIT_BYTES = 32 << 20;
  static final int DEFAULT_STATUS_PORT = 8081;
  // Memory threshold over which no new work will be processed.
  // Set to a value >= 1 to disable pushback.
  static final double PUSHBACK_THRESHOLD_RATIO = 0.9;
  static final String DEFAULT_WINDMILL_SERVER_CLASS_NAME =
      "com.google.cloud.dataflow.sdk.runners.worker.windmill.WindmillServer";

  /**
   * Indicates that the key token was invalid when data was attempted to be fetched.
   */
  public static class KeyTokenInvalidException extends RuntimeException {
    private static final long serialVersionUID = 0;

    public KeyTokenInvalidException(String key) {
      super("Unable to fetch data due to token mismatch for key " + key);
    }
  }

  /**
   * Returns whether an exception was caused by a {@link KeyTokenInvalidException}.
   */
  public static boolean isKeyTokenInvalidException(Throwable t) {
    while (t != null) {
      if (t instanceof KeyTokenInvalidException) {
        return true;
      }
      t = t.getCause();
    }
    return false;
  }

  static MapTask parseMapTask(String input) throws IOException {
    return Transport.getJsonFactory()
        .fromString(input, MapTask.class);
  }

  public static void main(String[] args) throws Exception {
    Thread.setDefaultUncaughtExceptionHandler(
        DataflowWorkerHarness.WorkerUncaughtExceptionHandler.INSTANCE);

    DataflowWorkerLoggingInitializer.initialize();
    DataflowWorkerHarnessOptions options =
        PipelineOptionsFactory.createFromSystemPropertiesInternal();
    // TODO: Remove setting these options once we have migrated to passing
    // through the pipeline options.
    options.setAppName("StreamingWorkerHarness");
    options.setStreaming(true);

    DataflowWorkerLoggingInitializer.configure(options);
    String hostport = System.getProperty("windmill.hostport");
    if (hostport == null) {
      throw new Exception("-Dwindmill.hostport must be set to the location of the windmill server");
    }

    int statusPort = DEFAULT_STATUS_PORT;
    if (System.getProperties().containsKey("status_port")) {
      statusPort = Integer.parseInt(System.getProperty("status_port"));
    }

    String windmillServerClassName = DEFAULT_WINDMILL_SERVER_CLASS_NAME;
    if (System.getProperties().containsKey("windmill.serverclassname")) {
      windmillServerClassName = System.getProperty("windmill.serverclassname");
    }

    ArrayList<MapTask> mapTasks = new ArrayList<>();
    for (String arg : args) {
      mapTasks.add(parseMapTask(arg));
    }

    WindmillServerStub windmillServer =
        (WindmillServerStub) Class.forName(windmillServerClassName)
        .getDeclaredConstructor(String.class).newInstance(hostport);

    StreamingDataflowWorker worker =
        new StreamingDataflowWorker(mapTasks, windmillServer, options);
    worker.start();

    worker.runStatusServer(statusPort);
  }

  // Maps from computation ids to per-computation state.
  private final ConcurrentMap<String, MapTask> instructionMap;
  private final ConcurrentMap<String, ConcurrentLinkedQueue<Windmill.WorkItemCommitRequest>>
      outputMap;
  private final ConcurrentMap<String, ConcurrentLinkedQueue<WorkerAndContext>> mapTaskExecutors;
  private final ConcurrentMap<String, ActiveWorkForComputation> activeWorkMap;
  // Per computation cache of active readers, keyed by split ID.
  private final ConcurrentMap<String, ConcurrentMap<ByteString, UnboundedSource.UnboundedReader<?>>>
      readerCache;

  // Map of tokens to commit callbacks.
  private ConcurrentMap<Long, Runnable> commitCallbacks;

  // Map of user state names to system state names.
  private ConcurrentMap<String, String> stateNameMap;
  private ConcurrentMap<String, String> systemNameToComputationIdMap;

  private ThreadFactory threadFactory;
  private BoundedQueueExecutor workUnitExecutor;
  private ExecutorService commitExecutor;
  private WindmillServerStub windmillServer;
  private Thread dispatchThread;
  private AtomicBoolean running;
  private StateFetcher stateFetcher;
  private DataflowWorkerHarnessOptions options;
  private long clientId;
  private Server statusServer;
  private final AtomicReference<Throwable> lastException;
  private final MetricTrackingWindmillServerStub metricTrackingWindmillServer;
  private Timer globalCountersUpdatesTimer;

  public StreamingDataflowWorker(
      List<MapTask> mapTasks, WindmillServerStub server, DataflowWorkerHarnessOptions options) {
    this.options = options;
    this.instructionMap = new ConcurrentHashMap<>();
    this.outputMap = new ConcurrentHashMap<>();
    this.mapTaskExecutors = new ConcurrentHashMap<>();
    this.activeWorkMap = new ConcurrentHashMap<>();
    this.readerCache = new ConcurrentHashMap<>();
    this.commitCallbacks = new ConcurrentHashMap<>();
    this.stateNameMap = new ConcurrentHashMap<>();
    this.systemNameToComputationIdMap = new ConcurrentHashMap<>();
    this.threadFactory = new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread t = new Thread(r);
          t.setDaemon(true);
          return t;
        }
      };
    this.workUnitExecutor = new BoundedQueueExecutor(
        MAX_PROCESSING_THREADS, THREAD_EXPIRATION_TIME_SEC, TimeUnit.SECONDS,
        MAX_WORK_UNITS_QUEUED, threadFactory);
    this.commitExecutor =
        new ThreadPoolExecutor(
            1,
            1,
            Long.MAX_VALUE,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(2),
            new ThreadFactory() {
              @Override
              public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setPriority(Thread.MAX_PRIORITY);
                t.setName("CommitThread");
                return t;
              }
            },
            new ThreadPoolExecutor.DiscardPolicy());
    this.windmillServer = server;
    this.metricTrackingWindmillServer = new MetricTrackingWindmillServerStub(server);
    this.running = new AtomicBoolean();
    this.stateFetcher = new StateFetcher(metricTrackingWindmillServer);
    this.clientId = new Random().nextLong();
    this.lastException = new AtomicReference<>();

    for (MapTask mapTask : mapTasks) {
      addComputation(mapTask);
    }

    DataflowWorkerLoggingMDC.setJobId(options.getJobId());
    DataflowWorkerLoggingMDC.setWorkerId(options.getWorkerId());
  }

  void addStateNameMappings(Map<String, String> nameMap) {
    stateNameMap.putAll(nameMap);
  }

  public void start() {
    running.set(true);
    dispatchThread = threadFactory.newThread(new Runnable() {
      @Override
      public void run() {
        dispatchLoop();
      }
    });
    dispatchThread.setPriority(Thread.MIN_PRIORITY);
    dispatchThread.setName("DispatchThread");
    dispatchThread.start();
    globalCountersUpdatesTimer = new Timer("GlobalCountersUpdates");
    //  Report counters update every second.
    globalCountersUpdatesTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        reportPeriodicStats();
      }
    }, 1000, 1000);
    reportHarnessStartup();
  }

  public void stop() {
    try {
      if (globalCountersUpdatesTimer != null) {
        globalCountersUpdatesTimer.cancel();
      }
      if (statusServer != null) {
        statusServer.stop();
      }
      running.set(false);
      dispatchThread.join();
      workUnitExecutor.shutdown();
      if (!workUnitExecutor.awaitTermination(5, TimeUnit.MINUTES)) {
        throw new RuntimeException("Work executor did not terminate within 5 minutes");
      }
      for (ConcurrentLinkedQueue<WorkerAndContext> queue : mapTaskExecutors.values()) {
        WorkerAndContext workerAndContext;
        while ((workerAndContext = queue.poll()) != null) {
          workerAndContext.getWorker().close();
        }
      }
      commitExecutor.shutdown();
      if (!commitExecutor.awaitTermination(5, TimeUnit.MINUTES)) {
        throw new RuntimeException("Commit executor did not terminate within 5 minutes");
      }
    } catch (Exception e) {
      LOG.warn("Exception while shutting down: ", e);
    }
  }

  public void runStatusServer(int statusPort) {
    statusServer = new Server(statusPort);
    statusServer.setHandler(new StatusHandler());
    try {
      statusServer.start();
      LOG.info("Status server started on port {}", statusPort);
      statusServer.join();
    } catch (Exception e) {
      LOG.warn("Status server failed to start: ", e);
    }
  }

  private void addComputation(MapTask mapTask) {
    String computationId =
        systemNameToComputationIdMap.containsKey(mapTask.getSystemName())
            ? systemNameToComputationIdMap.get(mapTask.getSystemName())
            : mapTask.getSystemName();
    if (!instructionMap.containsKey(computationId)) {
      LOG.info("Adding config for {}: {}", computationId, mapTask);
      outputMap.put(computationId, new ConcurrentLinkedQueue<Windmill.WorkItemCommitRequest>());
      instructionMap.put(computationId, mapTask);
      mapTaskExecutors.put(computationId, new ConcurrentLinkedQueue<WorkerAndContext>());
      activeWorkMap.put(computationId, new ActiveWorkForComputation(workUnitExecutor));
      readerCache.put(
          computationId, new ConcurrentHashMap<ByteString, UnboundedSource.UnboundedReader<?>>());
    }
  }

  private static void sleep(int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      // NOLINT
    }
  }

  private static long lastPushbackLog = 0;

  protected static boolean inPushback(Runtime rt) {
    // If free memory is less than a percentage of total memory, block
    // until current work drains and memory is released.
    // Also force a GC to try to get under the memory threshold if possible.
    long currentMemorySize = rt.totalMemory();
    long memoryUsed = currentMemorySize - rt.freeMemory();
    long maxMemory = rt.maxMemory();

    if (memoryUsed <= maxMemory * PUSHBACK_THRESHOLD_RATIO) {
      return false;
    }

    if (lastPushbackLog < System.currentTimeMillis() - 60 * 1000) {
      LOG.warn(
          "In pushback, not accepting new work. Using {}MB / {}MB ({}MB currently used by JVM)",
          memoryUsed >> 20, maxMemory >> 20, currentMemorySize >> 20);
      lastPushbackLog = System.currentTimeMillis();
    }

    return true;
  }

  private void dispatchLoop() {
    LOG.info("Dispatch starting");
    Runtime rt = Runtime.getRuntime();
    while (running.get()) {
      if (inPushback(rt)) {
        System.gc();
        while (inPushback(rt)) {
          sleep(10);
        }
      }

      int backoff = 1;
      Windmill.GetWorkResponse workResponse;
      do {
        workResponse = getWork();
        if (workResponse.getWorkCount() > 0) {
          break;
        }
        sleep(backoff);
        backoff = Math.min(1000, backoff * 2);
      } while (running.get());
      for (final Windmill.ComputationWorkItems computationWork : workResponse.getWorkList()) {
        final String computation = computationWork.getComputationId();
        if (!instructionMap.containsKey(computation)) {
          getConfig(computation);
        }
        final MapTask mapTask = instructionMap.get(computation);
        if (mapTask == null) {
          LOG.warn(
              "Received work for unknown computation: {}. Known computations are {}",
              computation, instructionMap.keySet());
          continue;
        }

        long watermarkMicros = computationWork.getInputDataWatermark();
        final Instant inputDataWatermark = new Instant(watermarkMicros / 1000);
        ActiveWorkForComputation activeWork = activeWorkMap.get(computation);
        for (final Windmill.WorkItem workItem : computationWork.getWorkList()) {
          Work work = new Work(workItem.getWorkToken()) {
              @Override
              public void run() {
                process(computation, mapTask, inputDataWatermark, workItem);
              }
            };
          if (activeWork.activateWork(workItem.getKey(), work)) {
            workUnitExecutor.execute(work);
          }
        }
      }
    }
    LOG.info("Dispatch done");
  }

  abstract static class Work implements Runnable {
    private final long workToken;
    public Work(long workToken) {
      this.workToken = workToken;
    }
    public long getWorkToken() {
      return workToken;
    }
  }

  private void process(
      final String computation,
      final MapTask mapTask,
      final Instant inputDataWatermark,
      final Windmill.WorkItem work) {
    LOG.debug("Starting processing for {}:\n{}", computation, work);

    Windmill.WorkItemCommitRequest.Builder outputBuilder =
        Windmill.WorkItemCommitRequest.newBuilder()
        .setKey(work.getKey())
        .setWorkToken(work.getWorkToken());

    StreamingModeExecutionContext context = null;
    MapTaskExecutor worker = null;

    try {
      DataflowWorkerLoggingMDC.setWorkId(
          work.getKey().toStringUtf8() + "-" + Long.toString(work.getWorkToken()));
      DataflowWorkerLoggingMDC.setStageName(computation);
      WorkerAndContext workerAndContext = mapTaskExecutors.get(computation).poll();
      if (workerAndContext == null) {
        context = new StreamingModeExecutionContext(
            stateFetcher, readerCache.get(computation), stateNameMap);
        worker = MapTaskExecutorFactory.create(options, mapTask, context);
        ReadOperation readOperation = worker.getReadOperation();
        // Disable progress updates since its results are unused for streaming
        // and involves starting a thread.
        readOperation.setProgressUpdatePeriodMs(0);
        Preconditions.checkState(
            worker.supportsRestart(), "Streaming runner requires all operations support restart.");
      } else {
        worker = workerAndContext.getWorker();
        context = workerAndContext.getContext();
      }

      WindmillStateReader stateReader = new WindmillStateReader(
          metricTrackingWindmillServer, computation, work.getKey(), work.getWorkToken());
      context.start(work, inputDataWatermark, stateReader, outputBuilder);

      for (Long callbackId : context.getReadyCommitCallbackIds()) {
        final Runnable callback = commitCallbacks.remove(callbackId);
        if (callback != null) {
          workUnitExecutor.forceExecute(new Runnable() {
              @Override
              public void run() {
                try {
                  callback.run();
                } catch (Throwable t) {
                  LOG.error("Source checkpoint finalization failed:", t);
                }
              }
            });
        }
      }

      // Blocks while executing work.
      worker.execute();

      buildCounters(worker.getOutputCounters(), outputBuilder);

      commitCallbacks.putAll(context.flushState());

      mapTaskExecutors.get(computation).offer(new WorkerAndContext(worker, context));
      worker = null;
      context = null;

      Windmill.WorkItemCommitRequest output = outputBuilder.build();
      outputMap.get(computation).add(output);
      scheduleCommit();

      LOG.debug("Processing done for work token: {}", work.getWorkToken());
    } catch (Throwable t) {
      if (worker != null) {
        try {
          worker.close();
        } catch (Exception e) {
          LOG.warn("Failed to close worker: ", e);
        }
      }

      t = t instanceof UserCodeException ? t.getCause() : t;

      if (isKeyTokenInvalidException(t)) {
        LOG.debug("Execution of work for {} for key {} failed due to token expiration, "
            + "will not retry locally.",
            computation, work.getKey().toStringUtf8());
      } else {
        LOG.error(
            "Execution of work for {} for key {} failed, retrying.",
            computation,
            work.getKey().toStringUtf8());
        LOG.error("\nError: ", t);
        lastException.set(t);
        LOG.debug("Failed work: {}", work);
        if (reportFailure(computation, work, t)) {
          // Try again, after some delay and at the end of the queue to avoid a tight loop.
          sleep(10000);
          workUnitExecutor.forceExecute(
              new Runnable() {
                @Override
                public void run() {
                  process(computation, mapTask, inputDataWatermark, work);
                }
              });
        } else {
          // If we failed to report the error, the item is invalid and should
          // not be retried internally.  It will be retried at the higher level.
          LOG.debug("Aborting processing due to exception reporting failure");
        }
      }
    } finally {
      DataflowWorkerLoggingMDC.setWorkId(null);
      DataflowWorkerLoggingMDC.setStageName(null);
    }
  }

  private void scheduleCommit() {
    commitExecutor.execute(new Commit());
  }

  private class Commit implements Runnable {
    @Override
    public void run() {
      while (true) {
        Windmill.CommitWorkRequest.Builder commitRequestBuilder =
            Windmill.CommitWorkRequest.newBuilder();
        long remainingCommitBytes = MAX_COMMIT_BYTES;
        for (Map.Entry<String, ConcurrentLinkedQueue<Windmill.WorkItemCommitRequest>> entry :
                 outputMap.entrySet()) {
          Windmill.ComputationCommitWorkRequest.Builder computationRequestBuilder =
              Windmill.ComputationCommitWorkRequest.newBuilder();
          ConcurrentLinkedQueue<Windmill.WorkItemCommitRequest> queue = entry.getValue();
          while (remainingCommitBytes > 0) {
            Windmill.WorkItemCommitRequest request = queue.poll();
            if (request == null) {
              break;
            }
            remainingCommitBytes -= request.getSerializedSize();
            computationRequestBuilder.addRequests(request);
          }
          if (computationRequestBuilder.getRequestsCount() > 0) {
            computationRequestBuilder.setComputationId(entry.getKey());
            commitRequestBuilder.addRequests(computationRequestBuilder);
          }
        }
        if (commitRequestBuilder.getRequestsCount() > 0) {
          Windmill.CommitWorkRequest commitRequest = commitRequestBuilder.build();
          LOG.trace("Commit: {}", commitRequest);
          commitWork(commitRequest);
          for (Windmill.ComputationCommitWorkRequest computationRequest :
              commitRequest.getRequestsList()) {
            ActiveWorkForComputation activeWork =
                activeWorkMap.get(computationRequest.getComputationId());
            for (Windmill.WorkItemCommitRequest workRequest :
                computationRequest.getRequestsList()) {
              activeWork.completeWork(workRequest.getKey());
            }
          }
        } else {
          break;
        }
      }
    }
  }

  private Windmill.GetWorkResponse getWork() {
    return windmillServer.getWork(
        Windmill.GetWorkRequest.newBuilder()
        .setClientId(clientId)
        .setMaxItems(100)
        .build());
  }

  private void commitWork(Windmill.CommitWorkRequest request) {
    windmillServer.commitWork(request);
  }

  private void getConfig(String computation) {
    Windmill.GetConfigRequest request =
        Windmill.GetConfigRequest.newBuilder().addComputations(computation).build();

    Windmill.GetConfigResponse response = windmillServer.getConfig(request);
    for (Windmill.GetConfigResponse.SystemNameToComputationIdMapEntry entry :
        response.getSystemNameToComputationIdMapList()) {
      systemNameToComputationIdMap.put(entry.getSystemName(), entry.getComputationId());
    }
    for (String serializedMapTask : response.getCloudWorksList()) {
      try {
        addComputation(parseMapTask(serializedMapTask));
      } catch (IOException e) {
        LOG.warn("Parsing MapTask failed: {}", serializedMapTask);
        LOG.warn("Error: ", e);
      }
    }
    for (Windmill.GetConfigResponse.NameMapEntry entry : response.getNameMapList()) {
      stateNameMap.put(entry.getUserName(), entry.getSystemName());
    }
  }

  private void buildCounters(CounterSet counterSet,
                             Windmill.WorkItemCommitRequest.Builder builder) {
    for (Counter<?> counter : counterSet) {
      Windmill.Counter.Builder counterBuilder = Windmill.Counter.newBuilder();
      Windmill.Counter.Kind kind;
      Object aggregateObj = null;
      switch (counter.getKind()) {
        case SUM: kind = Windmill.Counter.Kind.SUM; break;
        case MAX: kind = Windmill.Counter.Kind.MAX; break;
        case MIN: kind = Windmill.Counter.Kind.MIN; break;
        case MEAN:
          kind = Windmill.Counter.Kind.MEAN;
          CounterMean<?> mean = counter.getAndResetMeanDelta();
          long count = mean.getCount();
          aggregateObj = mean.getAggregate();
          if (count <= 0) {
            continue;
          }
          counterBuilder.setMeanCount(count);
          break;
        default:
          LOG.debug("Unhandled counter type: {}", counter.getKind());
          continue;
      }
      if (counter.getKind() != AggregationKind.MEAN) {
        aggregateObj = counter.getAndResetDelta();
      }
      if (addKnownTypeToCounterBuilder(aggregateObj, counterBuilder)) {
        counterBuilder.setName(counter.getName()).setKind(kind);
        builder.addCounterUpdates(counterBuilder);
      }
    }
  }

  private boolean addKnownTypeToCounterBuilder(Object aggregateObj,
      Windmill.Counter.Builder counterBuilder) {
    if (aggregateObj instanceof Double) {
      double aggregate = (Double) aggregateObj;
      if (aggregate != 0) {
        counterBuilder.setDoubleScalar(aggregate);
      }
    } else if (aggregateObj instanceof Long) {
      long aggregate = (Long) aggregateObj;
      if (aggregate != 0) {
        counterBuilder.setIntScalar(aggregate);
      }
    } else if (aggregateObj instanceof Integer) {
      long aggregate = ((Integer) aggregateObj).longValue();
      if (aggregate != 0) {
        counterBuilder.setIntScalar(aggregate);
      }
    } else {
      LOG.debug("Unhandled aggregate class: {}", aggregateObj.getClass());
      return false;
    }
    return true;
  }

  private Windmill.Exception buildExceptionReport(Throwable t) {
    Windmill.Exception.Builder builder = Windmill.Exception.newBuilder();

    builder.addStackFrames(t.toString());
    for (StackTraceElement frame : t.getStackTrace()) {
      builder.addStackFrames(frame.toString());
    }
    if (t.getCause() != null) {
      builder.setCause(buildExceptionReport(t.getCause()));
    }

    return builder.build();
  }

  // Returns true if reporting the exception is successful and the work should be retried.
  private boolean reportFailure(String computation, Windmill.WorkItem work, Throwable t) {
    Windmill.ReportStatsResponse response =
        windmillServer.reportStats(Windmill.ReportStatsRequest.newBuilder()
            .setComputationId(computation)
            .setKey(work.getKey())
            .setWorkToken(work.getWorkToken())
            .addExceptions(buildExceptionReport(t))
            .build());
    return !response.getFailed();
  }

  private void reportHarnessStartup() {
    Windmill.Counter.Builder counterBuilder = Windmill.Counter.newBuilder();
    counterBuilder =
        counterBuilder.setName("dataflow_java_harness_restarts")
            .setKind(Windmill.Counter.Kind.SUM)
            .setIntScalar(1);
    Windmill.ReportStatsResponse response = windmillServer.reportStats(
        Windmill.ReportStatsRequest.newBuilder().addCounterUpdates(counterBuilder).build());
    if (response.getFailed()) {
      LOG.warn("Failed to notify windmill on harness startup. dataflow_java_harness_restarts will "
          + " not be incremented.");
    }
  }

  private void reportPeriodicStats() {
    Runtime rt = Runtime.getRuntime();
    long usedMemory = rt.totalMemory() - rt.freeMemory();
    long maxMemory =  rt.maxMemory();
    Windmill.Counter.Builder counterBuilder = Windmill.Counter.newBuilder();
    counterBuilder =
        counterBuilder.setName("dataflow_java_harness_memory_utilization")
            .setKind(Windmill.Counter.Kind.MEAN)
            .setCumulative(true)
            .setIntScalar(usedMemory)
            .setMeanCount(maxMemory);
    Windmill.ReportStatsResponse response = windmillServer.reportStats(
        Windmill.ReportStatsRequest.newBuilder()
            .addCounterUpdates(counterBuilder)
            .build());
    if (response.getFailed()) {
      LOG.warn("Failed to send periodic counters to windmill.");
    }
  }

  /**
   * Class representing the state of active work for a computation.
   *
   * <p> This class is synchronized, but only used from the dispatch and commit threads, so should
   * not be heavily contended.  Still, blocking work should not be done by it.
   */
  static class ActiveWorkForComputation {
    private Map<ByteString, Queue<Work>> activeWork = new HashMap<>();
    private BoundedQueueExecutor executor;

    ActiveWorkForComputation(BoundedQueueExecutor executor) {
      this.executor = executor;
    }

    /**
     * Mark the given key and work as active.  Returns whether the work is ready to be run
     * immediately.
     */
    public synchronized boolean activateWork(ByteString key, Work work) {
      Queue<Work> queue = activeWork.get(key);
      if (queue == null) {
        queue = new LinkedList<>();
        activeWork.put(key, queue);
        queue.add(work);
        return true;
      }
      if (queue.peek().getWorkToken() != work.getWorkToken()) {
        queue.add(work);
      }
      return false;
    }

    /**
     * Marks the work for a the given key as complete.  Schedules queued work for the key if any.
     */
    public synchronized void completeWork(ByteString key) {
      Queue<Work> queue = activeWork.get(key);
      queue.poll();
      if (queue.peek() != null) {
        executor.forceExecute(queue.peek());
      } else {
        activeWork.remove(key);
      }
    }

    public synchronized void printActiveWork(PrintWriter writer) {
      writer.println("<ul>");
      for (Map.Entry<ByteString, Queue<Work>> entry : activeWork.entrySet()) {
        Queue<Work> queue = entry.getValue();
        writer.print("<li>Key: ");
        writer.print(entry.getKey().toStringUtf8());
        writer.print(" Token: ");
        writer.print(queue.peek().getWorkToken());
        if (queue.size() > 1) {
          writer.print("(");
          writer.print(queue.size() - 1);
          writer.print(" queued)");
        }
        writer.println("</li>");
      }
      writer.println("</ul>");
    }
  }

  private static class WorkerAndContext {
    public MapTaskExecutor worker;
    public StreamingModeExecutionContext context;

    public WorkerAndContext(MapTaskExecutor worker, StreamingModeExecutionContext context) {
      this.worker = worker;
      this.context = context;
    }

    public MapTaskExecutor getWorker() {
      return worker;
    }

    public StreamingModeExecutionContext getContext() {
      return context;
    }
  }

  private class StatusHandler extends AbstractHandler {
    @Override
    public void handle(
        String target, Request baseRequest,
        HttpServletRequest request, HttpServletResponse response)
        throws IOException, ServletException {

      response.setContentType("text/html;charset=utf-8");
      response.setStatus(HttpServletResponse.SC_OK);
      baseRequest.setHandled(true);

      PrintWriter responseWriter = response.getWriter();

      responseWriter.println("<html><body>");

      if (target.equals("/healthz")) {
        responseWriter.println("ok");
      } else if (target.equals("/threadz")) {
        printThreads(responseWriter);
      } else {
        printHeader(responseWriter);
        printMetrics(responseWriter);
        printResources(responseWriter);
        printLastException(responseWriter);
        printSpecs(responseWriter);
      }

      responseWriter.println("</body></html>");
    }
  }

  private void printHeader(PrintWriter response) {
    response.println("<h1>Streaming Worker Harness</h1>");
    response.println("Running: " + running.get() + "<br>");
    response.println("ID: " + clientId + "<br>");
  }

  private void printMetrics(PrintWriter response) {
    response.println("<h2>Metrics</h2>");
    response.println("Worker Threads: " + workUnitExecutor.getPoolSize()
        + "/" + MAX_PROCESSING_THREADS + "<br>");
    response.println("Active Threads: " + workUnitExecutor.getActiveCount() + "<br>");
    response.println("Work Queue Size: " + workUnitExecutor.getQueue().size()
        + "/" + MAX_WORK_UNITS_QUEUED + "<br>");
    response.println("Commit Queues: <ul>");
    for (Map.Entry<String, ConcurrentLinkedQueue<Windmill.WorkItemCommitRequest>> entry
             : outputMap.entrySet()) {
      response.print("<li>");
      response.print(entry.getKey());
      response.print(": ");
      response.print(entry.getValue().size());
      response.println("</li>");
    }
    response.println("</ul>");
    response.println("Active Keys: <ul>");
    for (Map.Entry<String, ActiveWorkForComputation> computationEntry
             : activeWorkMap.entrySet()) {
      response.print("<li>");
      response.print(computationEntry.getKey());
      response.print(":");
      computationEntry.getValue().printActiveWork(response);
      response.println("</li>");
    }
    response.println("</ul>");
    metricTrackingWindmillServer.printHtml(response);
  }

  private void printResources(PrintWriter response) {
    Runtime rt = Runtime.getRuntime();
    response.append("<h2>Resources</h2>\n");
    response.append("Total Memory: " + (rt.totalMemory() >> 20) + "MB<br>\n");
    response.append("Used Memory: " + ((rt.totalMemory() - rt.freeMemory()) >> 20) + "MB<br>\n");
    response.append("Max Memory: " + (rt.maxMemory() >> 20) + "MB<br>\n");
  }

  private void printSpecs(PrintWriter response) {
    response.append("<h2>Specs</h2>\n");
    for (Map.Entry<String, MapTask> entry : instructionMap.entrySet()) {
      response.println("<h3>" + entry.getKey() + "</h3>");
      response.print("<script>document.write(JSON.stringify(");
      response.print(entry.getValue().toString());
      response.println(", null, \"&nbsp&nbsp\").replace(/\\n/g, \"<br>\"))</script>");
    }
  }

  private void printLastException(PrintWriter response) {
    Throwable t = lastException.get();
    if (t != null) {
      response.println("<h2>Last Exception</h2>");
      StringWriter writer = new StringWriter();
      t.printStackTrace(new PrintWriter(writer));
      response.println(writer.toString().replace("\t", "&nbsp&nbsp").replace("\n", "<br>"));
    }
  }

  private void printThreads(PrintWriter response) {
    Map<Thread, StackTraceElement[]> stacks = Thread.getAllStackTraces();
    for (Map.Entry<Thread,  StackTraceElement[]> entry : stacks.entrySet()) {
      Thread thread = entry.getKey();
      response.println("Thread: " + thread + " State: " + thread.getState() + "<br>");
      for (StackTraceElement element : entry.getValue()) {
        response.println("&nbsp&nbsp" + element + "<br>");
      }
      response.println("<br>");
    }
  }
}
