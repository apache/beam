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

import com.google.auto.value.AutoBuilder;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.runners.dataflow.worker.status.BaseStatusServlet;
import org.apache.beam.runners.dataflow.worker.status.DebugCapture;
import org.apache.beam.runners.dataflow.worker.status.LastExceptionDataProvider;
import org.apache.beam.runners.dataflow.worker.status.WorkerStatusPages;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationStateCache;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfig;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingGlobalConfigHandle;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.TerminatingExecutors;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.ChannelzServlet;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow status pages for debugging current worker and processing state.
 *
 * @implNote Class member state should only be accessed, not modified.
 */
@Internal
public final class StreamingWorkerStatusPages {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingWorkerStatusPages.class);
  private static final String DUMP_STATUS_PAGES_EXECUTOR = "DumpStatusPages";
  private static final long DEFAULT_STATUS_PAGE_DUMP_PERIOD_SECONDS = 60;

  private final Supplier<Instant> clock;
  private final long clientId;
  private final AtomicBoolean isRunning;
  private final WorkerStatusPages statusPages;
  private final WindmillStateCache stateCache;
  private final ComputationStateCache computationStateCache;
  private final Supplier<Long> currentActiveCommitBytes;
  private final Consumer<PrintWriter> getDataStatusProvider;
  private final BoundedQueueExecutor workUnitExecutor;
  private final ScheduledExecutorService statusPageDumper;

  // StreamingEngine status providers.
  private final @Nullable GrpcWindmillStreamFactory windmillStreamFactory;
  private final DebugCapture.@Nullable Manager debugCapture;
  private final @Nullable ChannelzServlet channelzServlet;

  private final AtomicReference<StreamingGlobalConfig> globalConfig = new AtomicReference<>();

  StreamingWorkerStatusPages(
      Supplier<Instant> clock,
      long clientId,
      AtomicBoolean isRunning,
      WorkerStatusPages statusPages,
      DebugCapture.@Nullable Manager debugCapture,
      @Nullable ChannelzServlet channelzServlet,
      WindmillStateCache stateCache,
      ComputationStateCache computationStateCache,
      Supplier<Long> currentActiveCommitBytes,
      @Nullable GrpcWindmillStreamFactory windmillStreamFactory,
      Consumer<PrintWriter> getDataStatusProvider,
      BoundedQueueExecutor workUnitExecutor,
      ScheduledExecutorService statusPageDumper,
      StreamingGlobalConfigHandle globalConfigHandle) {
    this.clock = clock;
    this.clientId = clientId;
    this.isRunning = isRunning;
    this.statusPages = statusPages;
    this.debugCapture = debugCapture;
    this.channelzServlet = channelzServlet;
    this.stateCache = stateCache;
    this.computationStateCache = computationStateCache;
    this.currentActiveCommitBytes = currentActiveCommitBytes;
    this.windmillStreamFactory = windmillStreamFactory;
    this.getDataStatusProvider = getDataStatusProvider;
    this.workUnitExecutor = workUnitExecutor;
    this.statusPageDumper = statusPageDumper;
    globalConfigHandle.registerConfigObserver(globalConfig::set);
  }

  public static StreamingWorkerStatusPages.Builder builder() {
    return new AutoBuilder_StreamingWorkerStatusPages_Builder()
        .setStatusPageDumper(
            TerminatingExecutors.newSingleThreadedScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat(DUMP_STATUS_PAGES_EXECUTOR), LOG));
  }

  public void start(DataflowWorkerHarnessOptions options) {
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
            currentActiveCommitBytes,
            getDataStatusProvider,
            computationStateCache::getAllPresentComputations));
    statusPages.addStatusDataProvider(
        "exception", "Last Exception", new LastExceptionDataProvider());
    statusPages.addStatusDataProvider("cache", "State Cache", stateCache);

    if (isStreamingEngine()) {
      addStreamingEngineStatusPages();
    }

    statusPages.start();
    scheduleStatusPageDump(options);
  }

  private void addStreamingEngineStatusPages() {
    Preconditions.checkNotNull(debugCapture).start();
    statusPages.addServlet(Preconditions.checkNotNull(channelzServlet));
    statusPages.addCapturePage(Preconditions.checkNotNull(channelzServlet));
    statusPages.addStatusDataProvider(
        "streaming", "Streaming RPCs", Preconditions.checkNotNull(windmillStreamFactory));
    statusPages.addStatusDataProvider(
        "jobSettings",
        "User Worker Job Settings",
        writer -> {
          @Nullable StreamingGlobalConfig config = globalConfig.get();
          if (config == null) {
            writer.println("Job Settings not loaded.");
            return;
          }
          writer.println(config.userWorkerJobSettings().toString());
        });
  }

  private boolean isStreamingEngine() {
    return debugCapture != null && channelzServlet != null && windmillStreamFactory != null;
  }

  public void stop() {
    statusPages.stop();
    if (debugCapture != null) {
      debugCapture.stop();
    }
    statusPageDumper.shutdown();
    try {
      statusPageDumper.awaitTermination(300, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Error occurred shutting down periodic status page dumper", e);
    }
    statusPageDumper.shutdownNow();
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void scheduleStatusPageDump(DataflowWorkerHarnessOptions options) {
    if (options.getPeriodicStatusPageOutputDirectory() != null) {
      LOG.info(
          "Scheduling status page dump every {} seconds", DEFAULT_STATUS_PAGE_DUMP_PERIOD_SECONDS);
      statusPageDumper.scheduleWithFixedDelay(
          () ->
              dumpStatusPages(
                  options.getPeriodicStatusPageOutputDirectory(), options.getWorkerId()),
          DEFAULT_STATUS_PAGE_DUMP_PERIOD_SECONDS,
          DEFAULT_STATUS_PAGE_DUMP_PERIOD_SECONDS,
          TimeUnit.SECONDS);
    } else {
      LOG.info(
          "Status page output directory was not set, "
              + "status pages will not be periodically dumped. "
              + "If this was not intended check pipeline options.");
    }
  }

  private void dumpStatusPages(String getPeriodicStatusPageOutputDirectory, String workerId) {
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
                ("StreamingDataflowWorker" + workerId + "_" + page.pageName() + timestamp + ".html")
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

  @Internal
  @AutoBuilder(ofClass = StreamingWorkerStatusPages.class)
  public interface Builder {
    Builder setClock(Supplier<Instant> clock);

    Builder setClientId(long clientId);

    Builder setIsRunning(AtomicBoolean isRunning);

    Builder setStatusPages(WorkerStatusPages statusPages);

    Builder setDebugCapture(DebugCapture.Manager debugCapture);

    Builder setChannelzServlet(ChannelzServlet channelzServlet);

    Builder setStateCache(WindmillStateCache stateCache);

    Builder setComputationStateCache(ComputationStateCache computationStateCache);

    Builder setCurrentActiveCommitBytes(Supplier<Long> currentActiveCommitBytes);

    Builder setWindmillStreamFactory(GrpcWindmillStreamFactory windmillStreamFactory);

    Builder setGetDataStatusProvider(Consumer<PrintWriter> getDataStatusProvider);

    Builder setWorkUnitExecutor(BoundedQueueExecutor workUnitExecutor);

    Builder setStatusPageDumper(ScheduledExecutorService statusPageDumper);

    Builder setGlobalConfigHandle(StreamingGlobalConfigHandle globalConfigHandle);

    StreamingWorkerStatusPages build();
  }
}
