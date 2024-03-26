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
package org.apache.beam.runners.dataflow.worker.streaming.harness;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.beam.runners.dataflow.worker.status.BaseStatusServlet;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture;
import org.apache.beam.runners.dataflow.worker.status.LastExceptionDataProvider;
import org.apache.beam.runners.dataflow.worker.status.WorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.streaming.computations.ComputationStateCache;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.ChannelzServlet;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.StreamingEngineClient;
import org.apache.beam.runners.dataflow.worker.windmill.state.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow status pages for debugging current worker and processing state.
 *
 * @implNote Class member state should only be accessed, not modified.
 */
final class StreamingWorkerStatusPages {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingWorkerStatusPages.class);
  private static final String DUMP_STATUS_PAGES_EXECUTOR = "DumpStatusPages";

  private final Supplier<Instant> clock;
  private final long clientId;
  private final AtomicBoolean isRunning;
  private final WorkerStatusPages statusPages;
  private final DebugCapture.Manager debugCapture;
  private final ChannelzServlet channelzServlet;
  private final WindmillStateCache stateCache;
  private final ComputationStateCache computationStateCache;
  private final StreamingEngineClient streamingEngineClient;
  private final GrpcWindmillStreamFactory windmillStreamFactory;
  private final GetDataClient stateReadMetricsTracker;
  private final BoundedQueueExecutor workUnitExecutor;
  private final ScheduledExecutorService statusPageDumper;

  private StreamingWorkerStatusPages(
      Supplier<Instant> clock,
      long clientId,
      AtomicBoolean isRunning,
      WorkerStatusPages statusPages,
      DebugCapture.Manager debugCapture,
      ChannelzServlet channelzServlet,
      WindmillStateCache stateCache,
      ComputationStateCache computationStateCache,
      StreamingEngineClient streamingEngineClient,
      GrpcWindmillStreamFactory windmillStreamFactory,
      GetDataClient stateReadMetricsTracker,
      BoundedQueueExecutor workUnitExecutor,
      ScheduledExecutorService statusPageDumper) {
    this.clock = clock;
    this.clientId = clientId;
    this.isRunning = isRunning;
    this.statusPages = statusPages;
    this.debugCapture = debugCapture;
    this.channelzServlet = channelzServlet;
    this.stateCache = stateCache;
    this.computationStateCache = computationStateCache;
    this.streamingEngineClient = streamingEngineClient;
    this.windmillStreamFactory = windmillStreamFactory;
    this.stateReadMetricsTracker = stateReadMetricsTracker;
    this.workUnitExecutor = workUnitExecutor;
    this.statusPageDumper = statusPageDumper;
  }

  static StreamingWorkerStatusPages create(
      Supplier<Instant> clock,
      long clientId,
      AtomicBoolean isRunning,
      WorkerStatusPages statusPages,
      DebugCapture.Manager debugCapture,
      ChannelzServlet channelzServlet,
      WindmillStateCache stateCache,
      ComputationStateCache computationStateCache,
      StreamingEngineClient streamingEngineClient,
      GrpcWindmillStreamFactory windmillStreamFactory,
      GetDataClient stateReadMetricsTracker,
      BoundedQueueExecutor workUnitExecutor) {
    return new StreamingWorkerStatusPages(
        clock,
        clientId,
        isRunning,
        statusPages,
        debugCapture,
        channelzServlet,
        stateCache,
        computationStateCache,
        streamingEngineClient,
        windmillStreamFactory,
        stateReadMetricsTracker,
        workUnitExecutor,
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setNameFormat(DUMP_STATUS_PAGES_EXECUTOR).build()));
  }

  @VisibleForTesting
  static StreamingWorkerStatusPages forTesting(
      Supplier<Instant> clock,
      long clientId,
      AtomicBoolean isRunning,
      WorkerStatusPages statusPages,
      DebugCapture.Manager debugCapture,
      ChannelzServlet channelzServlet,
      WindmillStateCache stateCache,
      ComputationStateCache computationStateCache,
      StreamingEngineClient streamingEngineClient,
      GrpcWindmillStreamFactory windmillStreamFactory,
      GetDataClient stateReadMetricsTracker,
      BoundedQueueExecutor workUnitExecutor,
      ScheduledExecutorService statusPageDumper) {
    return new StreamingWorkerStatusPages(
        clock,
        clientId,
        isRunning,
        statusPages,
        debugCapture,
        channelzServlet,
        stateCache,
        computationStateCache,
        streamingEngineClient,
        windmillStreamFactory,
        stateReadMetricsTracker,
        workUnitExecutor,
        statusPageDumper);
  }

  void start() {
    debugCapture.start();
    statusPages.addServlet(channelzServlet);
    statusPages.addCapturePage(channelzServlet);

    statusPages.addServlet(stateCache.statusServlet());
    statusPages.addServlet(newSpecServlet());
    statusPages.addStatusDataProvider(
        "harness",
        "Harness",
        writer -> {
          writer.println("Running: " + isRunning.get() + "<br>");
          writer.println("ID: " + clientId + "<br>");
        });

    statusPages.addStatusDataProvider(
        "metrics",
        "Metrics",
        new MetricsDataProvider(
            workUnitExecutor,
            streamingEngineClient::currentActiveCommitBytes,
            stateReadMetricsTracker,
            computationStateCache::getAllComputations));
    statusPages.addStatusDataProvider(
        "exception", "Last Exception", new LastExceptionDataProvider());
    statusPages.addStatusDataProvider("cache", "State Cache", stateCache);
    statusPages.addStatusDataProvider("streaming", "Streaming RPCs", windmillStreamFactory);

    statusPages.start();
  }

  void stop() {
    statusPages.stop();
    debugCapture.stop();
    statusPageDumper.shutdown();
    try {
      statusPageDumper.awaitTermination(300, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Error occurred shutting down periodic status page dumper", e);
    }
    statusPageDumper.shutdownNow();
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  void scheduleStatusPageDump(
      String getPeriodicStatusPageOutputDirectory, String workerId, long delay) {
    statusPageDumper.scheduleWithFixedDelay(
        () -> {
          Collection<DebugCapture.Capturable> pages = statusPages.getDebugCapturePages();
          if (pages.isEmpty()) {
            LOG.warn("No captured status pages.");
          }
          long timestamp = clock.get().getMillis();
          for (DebugCapture.Capturable page : pages) {
            PrintWriter writer = null;
            try {
              File outputFile =
                  new File(
                      getPeriodicStatusPageOutputDirectory,
                      ("StreamingDataflowWorker"
                              + workerId
                              + "_"
                              + page.pageName()
                              + timestamp
                              + ".html")
                          .replaceAll("/", "_"));
              writer = new PrintWriter(outputFile, UTF_8.name());
              page.captureData(writer);
            } catch (IOException e) {
              LOG.warn("Error dumping status page.", e);
            } finally {
              if (writer != null) {
                writer.close();
              }
            }
          }
        },
        60,
        delay,
        TimeUnit.SECONDS);
  }

  private BaseStatusServlet newSpecServlet() {
    return new BaseStatusServlet("/spec") {
      @Override
      protected void doGet(HttpServletRequest request, HttpServletResponse response)
          throws IOException {
        PrintWriter writer = response.getWriter();
        computationStateCache.appendSummaryHtml(writer);
      }
    };
  }
}
