/*
 * Copyright (C) 2014 Google Inc.
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
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.WindmillServerStub;
import com.google.cloud.dataflow.sdk.util.BoundedQueueExecutor;
import com.google.cloud.dataflow.sdk.util.CloudCounterUtils;
import com.google.cloud.dataflow.sdk.util.StateFetcher;
import com.google.cloud.dataflow.sdk.util.StreamingModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.util.UserCodeException;
import com.google.cloud.dataflow.sdk.util.Values;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.MapTaskExecutor;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Implements a Streaming Dataflow worker.
 */
public class StreamingDataflowWorker {
  private static final Logger LOG = Logger.getLogger(StreamingDataflowWorker.class.getName());
  static final int MAX_THREAD_POOL_SIZE = 100;
  static final long THREAD_EXPIRATION_TIME_SEC = 60;
  static final int MAX_THREAD_POOL_QUEUE_SIZE = 100;
  static final long MAX_COMMIT_BYTES = 32 << 20;
  static final int DEFAULT_STATUS_PORT = 8081;
  // Memory threshold under which no new work will be processed.  Set to 0 to disable pushback.
  static final double PUSHBACK_THRESHOLD = 0.1;
  static final String WINDMILL_SERVER_CLASS_NAME =
      "com.google.cloud.dataflow.sdk.runners.worker.windmill.WindmillServer";

  /**
   * Indicates that the key token was invalid when data was attempted to be fetched.
   */
  public static class KeyTokenInvalidException extends RuntimeException {
    public KeyTokenInvalidException(String key) {
      super("Unable to fetch data due to token mismatch for key " + key);
    }
  }

  static MapTask parseMapTask(String input) throws IOException {
    return Transport.getJsonFactory()
        .fromString(input, MapTask.class);
  }

  public static void main(String[] args) throws Exception {
    LOG.setLevel(Level.INFO);
    String hostport = System.getProperty("windmill.hostport");
    if (hostport == null) {
      throw new Exception("-Dwindmill.hostport must be set to the location of the windmill server");
    }

    int statusPort = DEFAULT_STATUS_PORT;
    if (System.getProperties().containsKey("status_port")) {
      statusPort = Integer.parseInt(System.getProperty("status_port"));
    }

    ArrayList<MapTask> mapTasks = new ArrayList<>();
    for (int i = 0; i < args.length; i++) {
      mapTasks.add(parseMapTask(args[i]));
    }

    WindmillServerStub windmillServer =
        (WindmillServerStub) Class.forName(WINDMILL_SERVER_CLASS_NAME)
        .getDeclaredConstructor(String.class).newInstance(hostport);

    StreamingDataflowWorker worker =
        new StreamingDataflowWorker(mapTasks, windmillServer);
    worker.start();

    worker.runStatusServer(statusPort);
  }

  private ConcurrentMap<String, MapTask> instructionMap;
  private ConcurrentMap<String, ConcurrentLinkedQueue<Windmill.WorkItemCommitRequest>> outputMap;
  private ConcurrentMap<String, ConcurrentLinkedQueue<WorkerAndContext>> mapTaskExecutors;
  private ThreadFactory threadFactory;
  private BoundedQueueExecutor executor;
  private WindmillServerStub windmillServer;
  private Thread dispatchThread;
  private Thread commitThread;
  private AtomicBoolean running;
  private StateFetcher stateFetcher;
  private DataflowPipelineOptions options;
  private long clientId;
  private Server statusServer;
  private AtomicReference<Throwable> lastException;

  /** Regular constructor. */
  public StreamingDataflowWorker(
      List<MapTask> mapTasks, WindmillServerStub server) {
    initialize(mapTasks, server);
    options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setAppName("StreamingWorkerHarness");
    options.setStreaming(true);

    if (System.getProperties().containsKey("path_validator_class")) {
      try {
        options.setPathValidatorClass((Class) Class.forName(
            System.getProperty("path_validator_class")));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Unable to find validator class", e);
      }
    }
    if (System.getProperties().containsKey("credential_factory_class")) {
      try {
        options.setCredentialFactoryClass((Class) Class.forName(
            System.getProperty("credential_factory_class")));
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Unable to find credential factory class", e);
      }
    }
  }

  /** The constructor that takes PipelineOptions.  Should be used only by unit tests. */
  StreamingDataflowWorker(
      List<MapTask> mapTasks, WindmillServerStub server, DataflowPipelineOptions options) {
    initialize(mapTasks, server);
    this.options = options;
  }

  public void start() {
    running.set(true);
    dispatchThread = threadFactory.newThread(new Runnable() {
        public void run() {
          dispatchLoop();
        }
      });
    dispatchThread.setPriority(Thread.MIN_PRIORITY);
    dispatchThread.setName("DispatchThread");
    dispatchThread.start();

    commitThread = threadFactory.newThread(new Runnable() {
        public void run() {
          commitLoop();
        }
      });
    commitThread.setPriority(Thread.MAX_PRIORITY);
    commitThread.setName("CommitThread");
    commitThread.start();
  }

  public void stop() {
    try {
      if (statusServer != null) {
        statusServer.stop();
      }
      running.set(false);
      dispatchThread.join();
      executor.shutdown();
      if (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
        throw new RuntimeException("Process did not terminate within 5 minutes");
      }
      for (ConcurrentLinkedQueue<WorkerAndContext> queue : mapTaskExecutors.values()) {
        WorkerAndContext workerAndContext;
        while ((workerAndContext = queue.poll()) != null) {
          workerAndContext.getWorker().close();
        }
      }
      commitThread.join();
    } catch (Exception e) {
      LOG.warning("Exception while shutting down: " + e);
      e.printStackTrace();
    }
  }

  /** Initializes the execution harness. */
  private void initialize(List<MapTask> mapTasks, WindmillServerStub server) {
    this.instructionMap = new ConcurrentHashMap<>();
    this.outputMap = new ConcurrentHashMap<>();
    this.mapTaskExecutors = new ConcurrentHashMap<>();
    for (MapTask mapTask : mapTasks) {
      addComputation(mapTask);
    }
    this.threadFactory = new ThreadFactory() {
        private final Thread.UncaughtExceptionHandler handler =
            new Thread.UncaughtExceptionHandler() {
              public void uncaughtException(Thread thread, Throwable e) {
                LOG.severe("Uncaught exception: " + e);
                e.printStackTrace();
                System.exit(1);
              }
            };
        public Thread newThread(Runnable r) {
          Thread t = new Thread(r);
          t.setUncaughtExceptionHandler(handler);
          t.setDaemon(true);
          return t;
        }
      };
    this.executor = new BoundedQueueExecutor(
        MAX_THREAD_POOL_SIZE, THREAD_EXPIRATION_TIME_SEC, TimeUnit.SECONDS,
        MAX_THREAD_POOL_QUEUE_SIZE, threadFactory);
    this.windmillServer = server;
    this.running = new AtomicBoolean();
    this.stateFetcher = new StateFetcher(server);
    this.clientId = new Random().nextLong();
    this.lastException = new AtomicReference<>();
  }

  public void runStatusServer(int statusPort) {
    statusServer = new Server(statusPort);
    statusServer.setHandler(new StatusHandler());
    try {
      statusServer.start();
      LOG.info("Status server started on port " + statusPort);
      statusServer.join();
    } catch (Exception e) {
      LOG.warning("Status server failed to start: " + e);
    }
  }

  private void addComputation(MapTask mapTask) {
    String computation = mapTask.getSystemName();
    if (!instructionMap.containsKey(computation)) {
      LOG.info("Adding config for " + computation + ": " + mapTask);
      outputMap.put(computation, new ConcurrentLinkedQueue<Windmill.WorkItemCommitRequest>());
      instructionMap.put(computation, mapTask);
      mapTaskExecutors.put(
          computation,
          new ConcurrentLinkedQueue<WorkerAndContext>());
    }
  }

  private static void sleep(int millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      // NOLINT
    }
  }

  private void dispatchLoop() {
    LOG.info("Dispatch starting");
    Runtime rt = Runtime.getRuntime();
    long lastPushbackLog = 0;
    while (running.get()) {

      // If free memory is less than a percentage of total memory, block
      // until current work drains and memory is released.
      // Also force a GC to try to get under the memory threshold if possible.
      while (rt.freeMemory() < rt.totalMemory() * PUSHBACK_THRESHOLD) {
        if (lastPushbackLog < (lastPushbackLog = System.currentTimeMillis()) - 60 * 1000) {
          LOG.warning("In pushback, not accepting new work. Free Memory: "
              + rt.freeMemory() + "MB / " + rt.totalMemory() + "MB");
          System.gc();
        }
        sleep(10);
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
        for (final Windmill.WorkItem work : computationWork.getWorkList()) {
          final String computation = computationWork.getComputationId();
          if (!instructionMap.containsKey(computation)) {
            getConfig(computation);
          }
          executor.execute(new Runnable() {
              public void run() {
                process(computation, work);
              }
            });
        }
      }
    }
    LOG.info("Dispatch done");
  }

  private void process(
      final String computation, final Windmill.WorkItem work) {
    LOG.log(Level.FINE, "Starting processing for " + computation + ":\n{0}", work);

    MapTask mapTask = instructionMap.get(computation);
    if (mapTask == null) {
      LOG.info("Received work for unknown computation: " + computation
          + ". Known computations are " + instructionMap.keySet());
      return;
    }

    Windmill.WorkItemCommitRequest.Builder outputBuilder =
        Windmill.WorkItemCommitRequest.newBuilder()
        .setKey(work.getKey())
        .setWorkToken(work.getWorkToken());

    StreamingModeExecutionContext context = null;
    MapTaskExecutor worker = null;

    try {
      WorkerAndContext workerAndContext = mapTaskExecutors.get(computation).poll();
      if (workerAndContext == null) {
        context = new StreamingModeExecutionContext(computation, stateFetcher);
        worker = MapTaskExecutorFactory.create(options, mapTask, context);
      } else {
        worker = workerAndContext.getWorker();
        context = workerAndContext.getContext();
      }

      context.start(work, outputBuilder);

      // Blocks while executing work.
      worker.execute();

      buildCounters(worker.getOutputCounters(), outputBuilder);

      context.flushState();

      mapTaskExecutors.get(computation).offer(new WorkerAndContext(worker, context));
      worker = null;
      context = null;
    } catch (Throwable t) {
      if (worker != null) {
        try {
          worker.close();
        } catch (Exception e) {
          LOG.warning("Failed to close worker: " + e.getMessage());
          e.printStackTrace();
        }
      }

      t = t instanceof UserCodeException ? t.getCause() : t;

      if (t instanceof KeyTokenInvalidException) {
        LOG.fine("Execution of work for " + computation + " for key " + work.getKey().toStringUtf8()
            + " failed due to token expiration, will not retry locally.");
      } else {
        LOG.warning("Execution of work for " + computation + " for key "
            + work.getKey().toStringUtf8() + " failed, retrying."
            + "\nError: " + t.getMessage());
        t.printStackTrace();
        lastException.set(t);
        LOG.fine("Failed work: " + work);
        reportFailure(computation, work, t);
        // Try again, but go to the end of the queue to avoid a tight loop.
        sleep(60000);
        executor.forceExecute(new Runnable() {
            public void run() {
              process(computation, work);
            }
          });
      }
      return;
    }

    Windmill.WorkItemCommitRequest output = outputBuilder.build();
    outputMap.get(computation).add(output);
    LOG.fine("Processing done for work token: " + work.getWorkToken());
  }

  private void commitLoop() {
    while (running.get()) {
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
        LOG.log(Level.FINE, "Commit: {0}", commitRequest);
        commitWork(commitRequest);
      }
      if (remainingCommitBytes > 0) {
        sleep(100);
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
    for (String serializedMapTask : windmillServer.getConfig(request).getCloudWorksList()) {
      try {
        addComputation(parseMapTask(serializedMapTask));
      } catch (IOException e) {
        LOG.warning("Parsing MapTask failed: " + serializedMapTask);
        e.printStackTrace();
      }
    }
  }

  private void buildCounters(CounterSet counterSet,
                             Windmill.WorkItemCommitRequest.Builder builder) {
    for (MetricUpdate metricUpdate :
             CloudCounterUtils.extractCounters(counterSet, true /* delta */)) {
      Windmill.Counter.Kind kind;
      String cloudKind = metricUpdate.getKind();
      if (cloudKind.equals(Counter.AggregationKind.SUM.name())) {
        kind = Windmill.Counter.Kind.SUM;
      } else if (cloudKind.equals(Counter.AggregationKind.MEAN.name())) {
        kind = Windmill.Counter.Kind.MEAN;
      } else if (cloudKind.equals(Counter.AggregationKind.MAX.name())) {
        kind = Windmill.Counter.Kind.MAX;
      } else if (cloudKind.equals(Counter.AggregationKind.MIN.name())) {
        kind = Windmill.Counter.Kind.MIN;
      } else {
        LOG.log(Level.FINE, "Unhandled counter type: " + metricUpdate.getKind());
        return;
      }
      Windmill.Counter.Builder counterBuilder = builder.addCounterUpdatesBuilder();
      counterBuilder.setName(metricUpdate.getName().getName()).setKind(kind);
      Object element = null;
      if (kind == Windmill.Counter.Kind.MEAN) {
        Object meanCount = metricUpdate.getMeanCount();
        if (meanCount != null) {
          try {
            Long longValue = Values.asLong(meanCount);
            if (longValue != 0) {
              counterBuilder.setMeanCount(longValue);
            }
          } catch (ClassCastException e) {
            // Nothing to do.
          }
        }
        element = metricUpdate.getMeanSum();
      } else {
        element = metricUpdate.getScalar();
      }
      if (element != null) {
        try {
          Double doubleValue = Values.asDouble(element);
          if (doubleValue != 0) {
            counterBuilder.setDoubleScalar(doubleValue);
          }
        } catch (ClassCastException e) {
          // Nothing to do.
        }
        try {
          Long longValue = Values.asLong(element);
          if (longValue != 0) {
            counterBuilder.setIntScalar(longValue);
          }
        } catch (ClassCastException e) {
          // Nothing to do.
        }
      }
    }
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

  private void reportFailure(String computation, Windmill.WorkItem work, Throwable t) {
    windmillServer.reportStats(Windmill.ReportStatsRequest.newBuilder()
        .setComputationId(computation)
        .setKey(work.getKey())
        .setWorkToken(work.getWorkToken())
        .addExceptions(buildExceptionReport(t))
        .build());
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

      printHeader(responseWriter);

      printMetrics(responseWriter);

      printResources(responseWriter);

      printLastException(responseWriter);

      printSpecs(responseWriter);

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
    response.println("Worker Threads: " + executor.getPoolSize()
        + "/" + MAX_THREAD_POOL_QUEUE_SIZE + "<br>");
    response.println("Active Threads: " + executor.getActiveCount() + "<br>");
    response.println("Work Queue Size: " + executor.getQueue().size() + "<br>");
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
  }

  private void printResources(PrintWriter response) {
    Runtime rt = Runtime.getRuntime();
    int mb = 1024 * 1024;

    response.append("<h2>Resources</h2>\n");
    response.append("Total Memory: " + rt.totalMemory() / mb + "MB<br>\n");
    response.append("Used Memory: " + (rt.totalMemory() - rt.freeMemory()) / mb + "MB<br>\n");
    response.append("Max Memory: " + rt.maxMemory() / mb + "MB<br>\n");
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
}
