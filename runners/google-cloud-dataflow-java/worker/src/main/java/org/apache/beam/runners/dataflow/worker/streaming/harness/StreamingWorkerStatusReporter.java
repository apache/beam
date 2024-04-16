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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.PerStepNamespaceMetrics;
import com.google.api.services.dataflow.model.PerWorkerMetrics;
import com.google.api.services.dataflow.model.StreamingScalingReport;
import com.google.api.services.dataflow.model.StreamingScalingReportResponse;
import com.google.api.services.dataflow.model.WorkItemStatus;
import com.google.api.services.dataflow.model.WorkerMessage;
import com.google.api.services.dataflow.model.WorkerMessageResponse;
import java.io.IOException;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.DataflowSystemMetrics;
import org.apache.beam.runners.dataflow.worker.StreamingStepMetricsContainer;
import org.apache.beam.runners.dataflow.worker.WorkUnitClient;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.logging.DataflowWorkerLoggingMDC;
import org.apache.beam.runners.dataflow.worker.streaming.StageInfo;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures.FailureTracker;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.MultimapBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.math.LongMath;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reports the status of the worker to Dataflow Service. */
@Internal
@ThreadSafe
public final class StreamingWorkerStatusReporter {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingWorkerStatusReporter.class);

  // Reserved ID for counter updates.
  // Matches kWindmillCounterUpdate in workflow_worker_service_multi_hubs.cc.
  private static final String WINDMILL_COUNTER_UPDATE_WORK_ID = "3";
  private static final int COUNTER_UPDATES_SIZE = 128;
  private static final String WORKER_MESSAGE_REPORTER_THREAD = "ReportWorkerMessage";
  private static final String GLOBAL_WORKER_UPDATE_REPORTER_THREAD = "GlobalWorkerUpdates";

  private final boolean publishCounters;
  private final int initialMaxThreadCount;
  private final int initialMaxBundlesOutstanding;
  private final WorkUnitClient dataflowServiceClient;
  private final Supplier<Long> windmillQuotaThrottleTime;
  private final Supplier<Collection<StageInfo>> allStageInfo;
  private final FailureTracker failureTracker;
  private final StreamingCounters streamingCounters;
  private final MemoryMonitor memoryMonitor;
  private final BoundedQueueExecutor workExecutor;
  private final AtomicLong previousTimeAtMaxThreads;
  private final AtomicInteger maxThreadCountOverride;
  private final ScheduledExecutorService globalWorkerUpdateReporter;
  private final ScheduledExecutorService workerMessageReporter;

  // Reporting period for periodic status updates.
  private final long windmillHarnessUpdateReportingPeriodMillis;
  // PerWorkerMetrics are sent on the WorkerMessages channel, and are sent one in every
  // perWorkerMetricsUpdateFrequency RPC call. If 0, PerWorkerMetrics are not reported.
  private final long perWorkerMetricsUpdateFrequency;
  // Used to track the number of WorkerMessages that have been sent without PerWorkerMetrics.
  private final AtomicLong workerMessagesIndex;

  private StreamingWorkerStatusReporter(
      boolean publishCounters,
      WorkUnitClient dataflowServiceClient,
      Supplier<Long> windmillQuotaThrottleTime,
      Supplier<Collection<StageInfo>> allStageInfo,
      FailureTracker failureTracker,
      StreamingCounters streamingCounters,
      MemoryMonitor memoryMonitor,
      BoundedQueueExecutor workExecutor,
      Function<String, ScheduledExecutorService> executorFactory,
      long windmillHarnessUpdateReportingPeriodMillis,
      long perWorkerMetricsUpdateReportingPeriodMillis) {
    this.publishCounters = publishCounters;
    this.dataflowServiceClient = dataflowServiceClient;
    this.windmillQuotaThrottleTime = windmillQuotaThrottleTime;
    this.allStageInfo = allStageInfo;
    this.failureTracker = failureTracker;
    this.streamingCounters = streamingCounters;
    this.memoryMonitor = memoryMonitor;
    this.workExecutor = workExecutor;
    this.initialMaxThreadCount = workExecutor.getMaximumPoolSize();
    this.initialMaxBundlesOutstanding = workExecutor.maximumElementsOutstanding();
    this.previousTimeAtMaxThreads = new AtomicLong();
    this.maxThreadCountOverride = new AtomicInteger();
    this.globalWorkerUpdateReporter = executorFactory.apply(GLOBAL_WORKER_UPDATE_REPORTER_THREAD);
    this.workerMessageReporter = executorFactory.apply(WORKER_MESSAGE_REPORTER_THREAD);
    this.windmillHarnessUpdateReportingPeriodMillis = windmillHarnessUpdateReportingPeriodMillis;
    this.perWorkerMetricsUpdateFrequency =
        getPerWorkerMetricsUpdateFrequency(
            windmillHarnessUpdateReportingPeriodMillis,
            perWorkerMetricsUpdateReportingPeriodMillis);
    this.workerMessagesIndex = new AtomicLong();
  }

  public static StreamingWorkerStatusReporter create(
      WorkUnitClient workUnitClient,
      Supplier<Long> windmillQuotaThrottleTime,
      Supplier<Collection<StageInfo>> allStageInfo,
      FailureTracker failureTracker,
      StreamingCounters streamingCounters,
      MemoryMonitor memoryMonitor,
      BoundedQueueExecutor workExecutor,
      long windmillHarnessUpdateReportingPeriodMillis,
      long perWorkerMetricsUpdateReportingPeriodMillis) {
    return new StreamingWorkerStatusReporter(
        /* publishCounters= */ true,
        workUnitClient,
        windmillQuotaThrottleTime,
        allStageInfo,
        failureTracker,
        streamingCounters,
        memoryMonitor,
        workExecutor,
        threadName ->
            Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat(threadName).build()),
        windmillHarnessUpdateReportingPeriodMillis,
        perWorkerMetricsUpdateReportingPeriodMillis);
  }

  @VisibleForTesting
  public static StreamingWorkerStatusReporter forTesting(
      boolean publishCounters,
      WorkUnitClient workUnitClient,
      Supplier<Long> windmillQuotaThrottleTime,
      Supplier<Collection<StageInfo>> allStageInfo,
      FailureTracker failureTracker,
      StreamingCounters streamingCounters,
      MemoryMonitor memoryMonitor,
      BoundedQueueExecutor workExecutor,
      Function<String, ScheduledExecutorService> executorFactory,
      long windmillHarnessUpdateReportingPeriodMillis,
      long perWorkerMetricsUpdateReportingPeriodMillis) {
    return new StreamingWorkerStatusReporter(
        publishCounters,
        workUnitClient,
        windmillQuotaThrottleTime,
        allStageInfo,
        failureTracker,
        streamingCounters,
        memoryMonitor,
        workExecutor,
        executorFactory,
        windmillHarnessUpdateReportingPeriodMillis,
        perWorkerMetricsUpdateReportingPeriodMillis);
  }

  /**
   * Returns key for a counter update. It is a String in case of legacy counter and
   * CounterStructuredName in the case of a structured counter.
   */
  private static Object getCounterUpdateKey(CounterUpdate counterUpdate) {
    Object key = null;
    if (counterUpdate.getNameAndKind() != null) {
      key = counterUpdate.getNameAndKind().getName();
    } else if (counterUpdate.getStructuredNameAndMetadata() != null) {
      key = counterUpdate.getStructuredNameAndMetadata().getName();
    }
    return checkNotNull(key, "Could not find name for CounterUpdate: %s", counterUpdate);
  }

  /**
   * Clears counterUpdates and enqueues unique counters from counterMultimap. If a counter appears
   * more than once, one of them is extracted leaving the remaining in the map.
   */
  private static void extractUniqueCounters(
      List<CounterUpdate> counterUpdates, ListMultimap<Object, CounterUpdate> counterMultimap) {
    counterUpdates.clear();
    for (Iterator<Object> iter = counterMultimap.keySet().iterator(); iter.hasNext(); ) {
      List<CounterUpdate> counters = counterMultimap.get(iter.next());
      counterUpdates.add(counters.get(0));
      if (counters.size() == 1) {
        // There is single value. Remove the entry through the iterator.
        iter.remove();
      } else {
        // Otherwise remove the first value.
        counters.remove(0);
      }
    }
  }

  private static void shutdownExecutor(ScheduledExecutorService executor) {
    executor.shutdown();
    try {
      executor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Error occurred trying to gracefully shutdown executor={}", executor, e);
      executor.shutdownNow();
    }
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  public void start() {
    reportHarnessStartup();
    if (windmillHarnessUpdateReportingPeriodMillis > 0) {
      LOG.info(
          "Starting periodic worker status reporters. Reporting period is every {} millis.",
          windmillHarnessUpdateReportingPeriodMillis);
      // Periodically report workers counters and other updates.
      globalWorkerUpdateReporter.scheduleWithFixedDelay(
          this::reportPeriodicWorkerUpdates,
          0,
          windmillHarnessUpdateReportingPeriodMillis,
          TimeUnit.MILLISECONDS);

      workerMessageReporter.scheduleWithFixedDelay(
          this::reportPeriodicWorkerMessage,
          0,
          windmillHarnessUpdateReportingPeriodMillis,
          TimeUnit.MILLISECONDS);
    } else {
      LOG.info("Periodic worker status reporting is disabled.");
    }
  }

  public void stop() {
    shutdownExecutor(globalWorkerUpdateReporter);
    shutdownExecutor(workerMessageReporter);
    // one last send
    reportPeriodicWorkerUpdates();
    this.workerMessagesIndex.set(this.perWorkerMetricsUpdateFrequency);
    reportPeriodicWorkerMessage();
  }

  private void reportHarnessStartup() {
    DataflowWorkerLoggingMDC.setStageName("startup");
    CounterSet restartCounter = new CounterSet();
    restartCounter
        .longSum(
            DataflowSystemMetrics.StreamingSystemCounterNames.JAVA_HARNESS_RESTARTS.counterName())
        .addValue(1L);
    try {
      // Sending a one time update. Use empty counter set for cumulativeCounters (2nd arg).
      sendWorkerUpdatesToDataflowService(restartCounter, new CounterSet());
    } catch (IOException e) {
      LOG.warn("Failed to send harness startup counter", e);
    }
  }

  // Calculates the PerWorkerMetrics reporting frequency, ensuring alignment with the
  // WorkerMessages RPC schedule. The desired reporting period
  // (perWorkerMetricsUpdateReportingPeriodMillis) is adjusted to the nearest multiple
  // of the RPC interval (windmillHarnessUpdateReportingPeriodMillis).
  private static long getPerWorkerMetricsUpdateFrequency(
      long windmillHarnessUpdateReportingPeriodMillis,
      long perWorkerMetricsUpdateReportingPeriodMillis) {
    if (windmillHarnessUpdateReportingPeriodMillis == 0) {
      return 0;
    }
    return LongMath.divide(
        perWorkerMetricsUpdateReportingPeriodMillis,
        windmillHarnessUpdateReportingPeriodMillis,
        RoundingMode.CEILING);
  }

  /** Sends counter updates to Dataflow backend. */
  private void sendWorkerUpdatesToDataflowService(
      CounterSet deltaCounters, CounterSet cumulativeCounters) throws IOException {
    // Throttle time is tracked by the windmillServer but is reported to DFE here.
    streamingCounters.windmillQuotaThrottling().addValue(windmillQuotaThrottleTime.get());
    if (memoryMonitor.isThrashing()) {
      streamingCounters.memoryThrashing().addValue(1);
    }

    List<CounterUpdate> counterUpdates = new ArrayList<>(COUNTER_UPDATES_SIZE);

    if (publishCounters) {
      allStageInfo.get().forEach(s -> counterUpdates.addAll(s.extractCounterUpdates()));
      counterUpdates.addAll(
          cumulativeCounters.extractUpdates(false, DataflowCounterUpdateExtractor.INSTANCE));
      counterUpdates.addAll(
          deltaCounters.extractModifiedDeltaUpdates(DataflowCounterUpdateExtractor.INSTANCE));
    }

    // Handle duplicate counters from different stages. Store all the counters in a multimap and
    // send the counters that appear multiple times in separate RPCs. Same logical counter could
    // appear in multiple stages if a step runs in multiple stages (as with flatten-unzipped stages)
    // especially if the counter definition does not set execution_step_name.
    ListMultimap<Object, CounterUpdate> counterMultimap =
        MultimapBuilder.hashKeys(counterUpdates.size()).linkedListValues().build();
    boolean hasDuplicates = false;

    for (CounterUpdate c : counterUpdates) {
      Object key = getCounterUpdateKey(c);
      if (counterMultimap.containsKey(key)) {
        hasDuplicates = true;
      }
      counterMultimap.put(key, c);
    }

    if (hasDuplicates) {
      extractUniqueCounters(counterUpdates, counterMultimap);
    } else { // Common case: no duplicates. We can just send counterUpdates, empty the multimap.
      counterMultimap.clear();
    }

    WorkItemStatus workItemStatus =
        new WorkItemStatus()
            .setWorkItemId(WINDMILL_COUNTER_UPDATE_WORK_ID)
            .setErrors(failureTracker.drainPendingFailuresToReport())
            .setCounterUpdates(counterUpdates);

    dataflowServiceClient.reportWorkItemStatus(workItemStatus);

    // Send any counters appearing more than once in subsequent RPCs:
    while (!counterMultimap.isEmpty()) {
      extractUniqueCounters(counterUpdates, counterMultimap);
      dataflowServiceClient.reportWorkItemStatus(
          new WorkItemStatus()
              .setWorkItemId(WINDMILL_COUNTER_UPDATE_WORK_ID)
              .setCounterUpdates(counterUpdates));
    }
  }

  @VisibleForTesting
  public void reportPeriodicWorkerMessage() {
    try {
      List<WorkerMessageResponse> workerMessageResponses =
          dataflowServiceClient.reportWorkerMessage(createWorkerMessage());
      readAndSaveWorkerMessageResponseForStreamingScalingReportResponse(workerMessageResponses);
    } catch (IOException e) {
      LOG.warn("Failed to send worker messages", e);
    } catch (Exception e) {
      LOG.error("Unexpected exception while trying to send worker messages", e);
    }
  }

  private List<WorkerMessage> createWorkerMessage() {
    List<WorkerMessage> workerMessages = new ArrayList<>(2);
    workerMessages.add(createWorkerMessageForStreamingScalingReport());

    createWorkerMessageForPerWorkerMetrics().ifPresent(metrics -> workerMessages.add(metrics));
    return workerMessages;
  }

  private WorkerMessage createWorkerMessageForStreamingScalingReport() {
    StreamingScalingReport activeThreadsReport =
        new StreamingScalingReport()
            .setActiveThreadCount(workExecutor.activeCount())
            .setActiveBundleCount(workExecutor.elementsOutstanding())
            .setOutstandingBytes(workExecutor.bytesOutstanding())
            .setMaximumThreadCount(workExecutor.getMaximumPoolSize())
            .setMaximumBundleCount(workExecutor.maximumElementsOutstanding())
            .setMaximumBytes(workExecutor.maximumBytesOutstanding());
    return dataflowServiceClient.createWorkerMessageFromStreamingScalingReport(activeThreadsReport);
  }

  private Optional<WorkerMessage> createWorkerMessageForPerWorkerMetrics() {
    if (!StreamingStepMetricsContainer.getEnablePerWorkerMetrics()
        || perWorkerMetricsUpdateFrequency == 0) {
      return Optional.empty();
    }

    if (workerMessagesIndex.incrementAndGet() < perWorkerMetricsUpdateFrequency) {
      return Optional.empty();
    } else {
      workerMessagesIndex.set(0L);
    }

    List<PerStepNamespaceMetrics> metrics = new ArrayList<>();
    allStageInfo.get().forEach(s -> metrics.addAll(s.extractPerWorkerMetricValues()));

    if (metrics.isEmpty()) {
      return Optional.empty();
    }

    PerWorkerMetrics perWorkerMetrics = new PerWorkerMetrics().setPerStepNamespaceMetrics(metrics);
    return Optional.of(
        dataflowServiceClient.createWorkerMessageFromPerWorkerMetrics(perWorkerMetrics));
  }

  private void readAndSaveWorkerMessageResponseForStreamingScalingReportResponse(
      List<WorkerMessageResponse> responses) {
    Optional<StreamingScalingReportResponse> streamingScalingReportResponse = Optional.empty();
    for (WorkerMessageResponse response : responses) {
      if (response.getStreamingScalingReportResponse() != null) {
        streamingScalingReportResponse = Optional.of(response.getStreamingScalingReportResponse());
      }
    }
    if (streamingScalingReportResponse.isPresent()) {
      int oldMaximumThreadCount = getMaxThreads();
      maxThreadCountOverride.set(streamingScalingReportResponse.get().getMaximumThreadCount());
      int newMaximumThreadCount = getMaxThreads();
      if (newMaximumThreadCount != oldMaximumThreadCount) {
        LOG.info(
            "Setting maximum thread count to {}, old value is {}",
            newMaximumThreadCount,
            oldMaximumThreadCount);
        workExecutor.setMaximumPoolSize(newMaximumThreadCount, getMaxBundlesOutstanding());
      }
    }
  }

  private int getMaxThreads() {
    int currentMaxThreadCountOverride = maxThreadCountOverride.get();
    if (currentMaxThreadCountOverride != 0) {
      return currentMaxThreadCountOverride;
    }
    return initialMaxThreadCount;
  }

  private int getMaxBundlesOutstanding() {
    int currentMaxThreadCountOverride = maxThreadCountOverride.get();
    if (currentMaxThreadCountOverride != 0) {
      return currentMaxThreadCountOverride + 100;
    }
    if (initialMaxBundlesOutstanding > 0) {
      return initialMaxBundlesOutstanding;
    }
    return getMaxThreads() + 100;
  }

  @VisibleForTesting
  public void reportPeriodicWorkerUpdates() {
    updateVMMetrics();
    updateThreadMetrics();
    try {
      sendWorkerUpdatesToDataflowService(
          streamingCounters.pendingDeltaCounters(), streamingCounters.pendingCumulativeCounters());
    } catch (IOException e) {
      LOG.warn("Failed to send periodic counter updates", e);
    } catch (Exception e) {
      LOG.error("Unexpected exception while trying to send counter updates", e);
    }
  }

  private void updateVMMetrics() {
    Runtime rt = Runtime.getRuntime();
    long usedMemory = rt.totalMemory() - rt.freeMemory();
    long maxMemory = rt.maxMemory();

    streamingCounters.javaHarnessUsedMemory().getAndReset();
    streamingCounters.javaHarnessUsedMemory().addValue(usedMemory);
    streamingCounters.javaHarnessMaxMemory().getAndReset();
    streamingCounters.javaHarnessMaxMemory().addValue(maxMemory);
  }

  private void updateThreadMetrics() {
    streamingCounters.timeAtMaxActiveThreads().getAndReset();
    long allThreadsActiveTime = workExecutor.allThreadsActiveTime();
    streamingCounters
        .timeAtMaxActiveThreads()
        .addValue(allThreadsActiveTime - previousTimeAtMaxThreads.get());
    previousTimeAtMaxThreads.set(allThreadsActiveTime);
    streamingCounters.activeThreads().getAndReset();
    streamingCounters.activeThreads().addValue(workExecutor.activeCount());
    streamingCounters.totalAllocatedThreads().getAndReset();
    streamingCounters.totalAllocatedThreads().addValue(workExecutor.getMaximumPoolSize());
    streamingCounters.outstandingBytes().getAndReset();
    streamingCounters.outstandingBytes().addValue(workExecutor.bytesOutstanding());
    streamingCounters.maxOutstandingBytes().getAndReset();
    streamingCounters.maxOutstandingBytes().addValue(workExecutor.maximumBytesOutstanding());
    streamingCounters.outstandingBundles().getAndReset();
    streamingCounters.outstandingBundles().addValue((long) workExecutor.elementsOutstanding());
    streamingCounters.maxOutstandingBundles().getAndReset();
    streamingCounters
        .maxOutstandingBundles()
        .addValue((long) workExecutor.maximumElementsOutstanding());
  }
}
