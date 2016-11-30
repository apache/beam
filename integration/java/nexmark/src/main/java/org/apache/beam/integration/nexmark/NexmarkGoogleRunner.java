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
package org.apache.beam.integration.nexmark;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;

import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.Aggregator;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Run a singe Nexmark query using a given configuration on Google Dataflow.
 */
class NexmarkGoogleRunner extends NexmarkRunner<NexmarkGoogleDriver.NexmarkGoogleOptions> {
  /**
   * How long to let streaming pipeline run after all events have been generated and we've
   * seen no activity.
   */
  private static final Duration DONE_DELAY = Duration.standardMinutes(1);

  /**
   * How long to allow no activity without warning.
   */
  private static final Duration STUCK_WARNING_DELAY = Duration.standardMinutes(10);

  /**
   * How long to let streaming pipeline run after we've
   * seen no activity, even if all events have not been generated.
   */
  private static final Duration STUCK_TERMINATE_DELAY = Duration.standardDays(3);

  /**
   * Delay between perf samples.
   */
  private static final Duration PERF_DELAY = Duration.standardSeconds(15);

  /**
   * Minimum number of samples needed for 'stead-state' rate calculation.
   */
  private static final int MIN_SAMPLES = 9;

  /**
   * Minimum length of time over which to consider samples for 'steady-state' rate calculation.
   */
  private static final Duration MIN_WINDOW = Duration.standardMinutes(2);

  public NexmarkGoogleRunner(NexmarkGoogleDriver.NexmarkGoogleOptions options) {
    super(options);
  }

  @Override
  protected boolean isStreaming() {
    return options.isStreaming();
  }

  @Override
  protected int coresPerWorker() {
    String machineType = options.getWorkerMachineType();
    if (machineType == null || machineType.isEmpty()) {
      return 1;
    }
    String[] split = machineType.split("-");
    if (split.length != 3) {
      return 1;
    }
    try {
      return Integer.parseInt(split[2]);
    } catch (NumberFormatException ex) {
      return 1;
    }
  }

  @Override
  protected int maxNumWorkers() {
    return Math.max(options.getNumWorkers(), options.getMaxNumWorkers());
  }

  @Override
  protected boolean canMonitor() {
    return true;
  }

  @Override
  protected void invokeBuilderForPublishOnlyPipeline(PipelineBuilder builder) {
    String jobName = options.getJobName();
    String appName = options.getAppName();
    options.setJobName("p-" + jobName);
    options.setAppName("p-" + appName);
    int coresPerWorker = coresPerWorker();
    int eventGeneratorWorkers = (configuration.numEventGenerators + coresPerWorker - 1)
                                / coresPerWorker;
    options.setMaxNumWorkers(Math.min(options.getMaxNumWorkers(), eventGeneratorWorkers));
    options.setNumWorkers(Math.min(options.getNumWorkers(), eventGeneratorWorkers));
    publisherMonitor = new Monitor<Event>(queryName, "publisher");
    try {
      builder.build(options);
    } finally {
      options.setJobName(jobName);
      options.setAppName(appName);
      options.setMaxNumWorkers(options.getMaxNumWorkers());
      options.setNumWorkers(options.getNumWorkers());
    }
  }

  /**
   * Monitor the progress of the publisher job. Return when it has been generating events for
   * at least {@code configuration.preloadSeconds}.
   */
  @Override
  protected void waitForPublisherPreload() {
    checkNotNull(publisherMonitor);
    checkNotNull(publisherResult);
    if (!options.getMonitorJobs()) {
      return;
    }
    if (!(publisherResult instanceof DataflowPipelineJob)) {
      return;
    }
    if (configuration.preloadSeconds <= 0) {
      return;
    }

    NexmarkUtils.console("waiting for publisher to pre-load");

    DataflowPipelineJob job = (DataflowPipelineJob) publisherResult;

    long numEvents = 0;
    long startMsSinceEpoch = -1;
    long endMsSinceEpoch = -1;
    while (true) {
      PipelineResult.State state = job.getState();
      switch (state) {
        case UNKNOWN:
          // Keep waiting.
          NexmarkUtils.console("%s publisher (%d events)", state, numEvents);
          break;
        case STOPPED:
        case DONE:
        case CANCELLED:
        case FAILED:
        case UPDATED:
          NexmarkUtils.console("%s publisher (%d events)", state, numEvents);
          return;
        case RUNNING:
          numEvents = getLong(job, publisherMonitor.getElementCounter());
          if (startMsSinceEpoch < 0 && numEvents > 0) {
            startMsSinceEpoch = System.currentTimeMillis();
            endMsSinceEpoch = startMsSinceEpoch
                              + Duration.standardSeconds(configuration.preloadSeconds).getMillis();
          }
          if (endMsSinceEpoch < 0) {
            NexmarkUtils.console("%s publisher (%d events)", state, numEvents);
          } else {
            long remainMs = endMsSinceEpoch - System.currentTimeMillis();
            if (remainMs > 0) {
              NexmarkUtils.console("%s publisher (%d events, waiting for %ds)", state, numEvents,
                  remainMs / 1000);
            } else {
              NexmarkUtils.console("publisher preloaded %d events", numEvents);
              return;
            }
          }
          break;
      }

      try {
        Thread.sleep(PERF_DELAY.getMillis());
      } catch (InterruptedException e) {
        Thread.interrupted();
        throw new RuntimeException("Interrupted: publisher still running.");
      }
    }
  }

  /**
   * Monitor the performance and progress of a running job. Return final performance if
   * it was measured.
   */
  @Override
  @Nullable
  protected NexmarkPerf monitor(NexmarkQuery query) {
    if (!options.getMonitorJobs()) {
      return null;
    }
    if (!(mainResult instanceof DataflowPipelineJob)) {
      return null;
    }

    if (configuration.debug) {
      NexmarkUtils.console("Waiting for main pipeline to 'finish'");
    } else {
      NexmarkUtils.console("--debug=false, so job will not self-cancel");
    }

    DataflowPipelineJob job = (DataflowPipelineJob) mainResult;
    DataflowPipelineJob publisherJob = (DataflowPipelineJob) publisherResult;
    List<NexmarkPerf.ProgressSnapshot> snapshots = new ArrayList<>();
    long startMsSinceEpoch = System.currentTimeMillis();
    long endMsSinceEpoch = -1;
    if (options.getRunningTimeMinutes() != null) {
      endMsSinceEpoch = startMsSinceEpoch
                        + Duration.standardMinutes(options.getRunningTimeMinutes()).getMillis()
                        - Duration.standardSeconds(configuration.preloadSeconds).getMillis();
    }
    long lastActivityMsSinceEpoch = -1;
    NexmarkPerf perf = null;
    boolean waitingForShutdown = false;
    boolean publisherCancelled = false;
    List<String> errors = new ArrayList<>();

    while (true) {
      long now = System.currentTimeMillis();
      if (endMsSinceEpoch >= 0 && now > endMsSinceEpoch && !waitingForShutdown) {
        NexmarkUtils.console("Reached end of test, cancelling job");
        try {
          job.cancel();
        } catch (IOException e) {
          throw new RuntimeException("Unable to cancel main job: ", e);
        }
        if (publisherResult != null) {
          try {
            publisherJob.cancel();
          } catch (IOException e) {
            throw new RuntimeException("Unable to cancel publisher job: ", e);
          }
          publisherCancelled = true;
        }
        waitingForShutdown = true;
      }

      PipelineResult.State state = job.getState();
      NexmarkUtils.console("%s %s%s", state, queryName,
          waitingForShutdown ? " (waiting for shutdown)" : "");

      NexmarkPerf currPerf;
      if (configuration.debug) {
        currPerf = currentPerf(startMsSinceEpoch, now, job, snapshots,
                               query.eventMonitor, query.resultMonitor);
      } else {
        currPerf = null;
      }

      if (perf == null || perf.anyActivity(currPerf)) {
        lastActivityMsSinceEpoch = now;
      }

      if (options.isStreaming() && !waitingForShutdown) {
        Duration quietFor = new Duration(lastActivityMsSinceEpoch, now);
        if (query.getFatalCount() != null && getLong(job, query.getFatalCount()) > 0) {
          NexmarkUtils.console("job has fatal errors, cancelling.");
          errors.add(String.format("Pipeline reported %s fatal errors", query.getFatalCount()));
          waitingForShutdown = true;
        } else if (configuration.debug && configuration.numEvents > 0
                   && currPerf.numEvents == configuration.numEvents
                   && currPerf.numResults >= 0 && quietFor.isLongerThan(DONE_DELAY)) {
          NexmarkUtils.console("streaming query appears to have finished, cancelling job.");
          waitingForShutdown = true;
        } else if (quietFor.isLongerThan(STUCK_TERMINATE_DELAY)) {
          NexmarkUtils.console("streaming query appears to have gotten stuck, cancelling job.");
          errors.add("Streaming job was cancelled since appeared stuck");
          waitingForShutdown = true;
        } else if (quietFor.isLongerThan(STUCK_WARNING_DELAY)) {
          NexmarkUtils.console("WARNING: streaming query appears to have been stuck for %d min.",
              quietFor.getStandardMinutes());
          errors.add(
              String.format("Streaming query was stuck for %d min", quietFor.getStandardMinutes()));
        }

        errors.addAll(checkWatermarks(job, startMsSinceEpoch));

        if (waitingForShutdown) {
          try {
            job.cancel();
          } catch (IOException e) {
            throw new RuntimeException("Unable to cancel main job: ", e);
          }
        }
      }

      perf = currPerf;

      boolean running = true;
      switch (state) {
        case UNKNOWN:
        case STOPPED:
        case RUNNING:
          // Keep going.
          break;
        case DONE:
          // All done.
          running = false;
          break;
        case CANCELLED:
          running = false;
          if (!waitingForShutdown) {
            errors.add("Job was unexpectedly cancelled");
          }
          break;
        case FAILED:
        case UPDATED:
          // Abnormal termination.
          running = false;
          errors.add("Job was unexpectedly updated");
          break;
      }

      if (!running) {
        break;
      }

      if (lastActivityMsSinceEpoch == now) {
        NexmarkUtils.console("new perf %s", perf);
      } else {
        NexmarkUtils.console("no activity");
      }

      try {
        Thread.sleep(PERF_DELAY.getMillis());
      } catch (InterruptedException e) {
        Thread.interrupted();
        NexmarkUtils.console("Interrupted: pipeline is still running");
      }
    }

    perf.errors = errors;
    perf.snapshots = snapshots;

    if (publisherResult != null) {
      NexmarkUtils.console("Shutting down publisher pipeline.");
      try {
        if (!publisherCancelled) {
          publisherJob.cancel();
        }
        publisherJob.waitUntilFinish(Duration.standardMinutes(5));
      } catch (IOException e) {
        throw new RuntimeException("Unable to cancel publisher job: ", e);
      } //TODO Ismael
//      catch (InterruptedException e) {
//        Thread.interrupted();
//        throw new RuntimeException("Interrupted: publish job still running.", e);
//      }
    }

    return perf;
  }

  enum MetricType {
    SYSTEM_WATERMARK,
    DATA_WATERMARK,
    OTHER
  }

  private MetricType getMetricType(MetricUpdate metric) {
    String metricName = metric.getName().getName();
    if (metricName.endsWith("windmill-system-watermark")) {
      return MetricType.SYSTEM_WATERMARK;
    } else if (metricName.endsWith("windmill-data-watermark")) {
      return MetricType.DATA_WATERMARK;
    } else {
      return MetricType.OTHER;
    }
  }

  /**
   * Check that watermarks are not too far behind.
   *
   * <p>Returns a list of errors detected.
   */
  private List<String> checkWatermarks(DataflowPipelineJob job, long startMsSinceEpoch) {
    long now = System.currentTimeMillis();
    List<String> errors = new ArrayList<>();
//    try {
      //TODO Ismael Ask google
//      JobMetrics metricResponse = job.getDataflowClient()
//                                     .projects()
//                                     .jobs()
//                                     .getMetrics(job.getProjectId(), job.getJobId())
//                                     .execute();
      List<MetricUpdate> metrics = null; // metricResponse.getMetrics();
      if (metrics != null) {
        boolean foundWatermarks = false;
        for (MetricUpdate metric : metrics) {
          MetricType type = getMetricType(metric);
          if (type == MetricType.OTHER) {
            continue;
          }
          foundWatermarks = true;
          @SuppressWarnings("unchecked")
          BigDecimal scalar = (BigDecimal) metric.getScalar();
          if (scalar.signum() < 0) {
            continue;
          }
          Instant value =
                  new Instant(scalar.divideToIntegralValue(new BigDecimal(1000)).longValueExact());
          Instant updateTime = Instant.parse(metric.getUpdateTime());

          if (options.getWatermarkValidationDelaySeconds() == null
                  || now > startMsSinceEpoch
                  + Duration.standardSeconds(options.getWatermarkValidationDelaySeconds())
                  .getMillis()) {
            Duration threshold = null;
            if (type == MetricType.SYSTEM_WATERMARK && options.getMaxSystemLagSeconds() != null) {
              threshold = Duration.standardSeconds(options.getMaxSystemLagSeconds());
            } else if (type == MetricType.DATA_WATERMARK
                    && options.getMaxDataLagSeconds() != null) {
              threshold = Duration.standardSeconds(options.getMaxDataLagSeconds());
            }

            if (threshold != null && value.isBefore(updateTime.minus(threshold))) {
              String msg = String.format("High lag for %s: %s vs %s (allowed lag of %s)",
                      metric.getName().getName(), value, updateTime, threshold);
              errors.add(msg);
              NexmarkUtils.console(msg);
            }
          }
        }
        if (!foundWatermarks) {
          NexmarkUtils.console("No known watermarks in update: " + metrics);
          if (now > startMsSinceEpoch + Duration.standardMinutes(5).getMillis()) {
            errors.add("No known watermarks found.  Metrics were " + metrics);
          }
        }
      }
//    } catch (IOException e) {
//      NexmarkUtils.console("Warning: failed to get JobMetrics: " + e);
//    }

    return errors;
  }

  /**
   * Return the current performance given {@code eventMonitor} and {@code resultMonitor}.
   */
  private NexmarkPerf currentPerf(
      long startMsSinceEpoch, long now, DataflowPipelineJob job,
      List<NexmarkPerf.ProgressSnapshot> snapshots, Monitor<?> eventMonitor,
      Monitor<?> resultMonitor) {
    NexmarkPerf perf = new NexmarkPerf();

    long numEvents = getLong(job, eventMonitor.getElementCounter());
    long numEventBytes = getLong(job, eventMonitor.getBytesCounter());
    long eventStart = getTimestamp(now, job, eventMonitor.getStartTime());
    long eventEnd = getTimestamp(now, job, eventMonitor.getEndTime());
    long numResults = getLong(job, resultMonitor.getElementCounter());
    long numResultBytes = getLong(job, resultMonitor.getBytesCounter());
    long resultStart = getTimestamp(now, job, resultMonitor.getStartTime());
    long resultEnd = getTimestamp(now, job, resultMonitor.getEndTime());
    long timestampStart = getTimestamp(now, job, resultMonitor.getStartTimestamp());
    long timestampEnd = getTimestamp(now, job, resultMonitor.getEndTimestamp());

    long effectiveEnd = -1;
    if (eventEnd >= 0 && resultEnd >= 0) {
      // It is possible for events to be generated after the last result was emitted.
      // (Eg Query 2, which only yields results for a small prefix of the event stream.)
      // So use the max of last event and last result times.
      effectiveEnd = Math.max(eventEnd, resultEnd);
    } else if (resultEnd >= 0) {
      effectiveEnd = resultEnd;
    } else if (eventEnd >= 0) {
      // During startup we may have no result yet, but we would still like to track how
      // long the pipeline has been running.
      effectiveEnd = eventEnd;
    }

    if (effectiveEnd >= 0 && eventStart >= 0 && effectiveEnd >= eventStart) {
      perf.runtimeSec = (effectiveEnd - eventStart) / 1000.0;
    }

    if (numEvents >= 0) {
      perf.numEvents = numEvents;
    }

    if (numEvents >= 0 && perf.runtimeSec > 0.0) {
      // For streaming we may later replace this with a 'steady-state' value calculated
      // from the progress snapshots.
      perf.eventsPerSec = numEvents / perf.runtimeSec;
    }

    if (numEventBytes >= 0 && perf.runtimeSec > 0.0) {
      perf.eventBytesPerSec = numEventBytes / perf.runtimeSec;
    }

    if (numResults >= 0) {
      perf.numResults = numResults;
    }

    if (numResults >= 0 && perf.runtimeSec > 0.0) {
      perf.resultsPerSec = numResults / perf.runtimeSec;
    }

    if (numResultBytes >= 0 && perf.runtimeSec > 0.0) {
      perf.resultBytesPerSec = numResultBytes / perf.runtimeSec;
    }

    if (eventStart >= 0) {
      perf.startupDelaySec = (eventStart - startMsSinceEpoch) / 1000.0;
    }

    if (resultStart >= 0 && eventStart >= 0 && resultStart >= eventStart) {
      perf.processingDelaySec = (resultStart - eventStart) / 1000.0;
    }

    if (timestampStart >= 0 && timestampEnd >= 0 && perf.runtimeSec > 0.0) {
      double eventRuntimeSec = (timestampEnd - timestampStart) / 1000.0;
      perf.timeDilation = eventRuntimeSec / perf.runtimeSec;
    }

    if (resultEnd >= 0) {
      // Fill in the shutdown delay assuming the job has now finished.
      perf.shutdownDelaySec = (now - resultEnd) / 1000.0;
    }

    perf.jobId = job.getJobId();
    // As soon as available, try to capture cumulative cost at this point too.

    NexmarkPerf.ProgressSnapshot snapshot = new NexmarkPerf.ProgressSnapshot();
    snapshot.secSinceStart = (now - startMsSinceEpoch) / 1000.0;
    snapshot.runtimeSec = perf.runtimeSec;
    snapshot.numEvents = numEvents;
    snapshot.numResults = numResults;
    snapshots.add(snapshot);

    captureSteadyState(perf, snapshots);

    return perf;
  }

  /**
   * Find a 'steady state' events/sec from {@code snapshots} and
   * store it in {@code perf} if found.
   */
  private void captureSteadyState(NexmarkPerf perf, List<NexmarkPerf.ProgressSnapshot> snapshots) {
    if (!options.isStreaming()) {
      return;
    }

    // Find the first sample with actual event and result counts.
    int dataStart = 0;
    for (; dataStart < snapshots.size(); dataStart++) {
      if (snapshots.get(dataStart).numEvents >= 0 && snapshots.get(dataStart).numResults >= 0) {
        break;
      }
    }

    // Find the last sample which demonstrated progress.
    int dataEnd = snapshots.size() - 1;
    for (; dataEnd > dataStart; dataEnd--) {
      if (snapshots.get(dataEnd).anyActivity(snapshots.get(dataEnd - 1))) {
        break;
      }
    }

    int numSamples = dataEnd - dataStart + 1;
    if (numSamples < MIN_SAMPLES) {
      // Not enough samples.
      NexmarkUtils.console("%d samples not enough to calculate steady-state event rate",
          numSamples);
      return;
    }

    // We'll look at only the middle third samples.
    int sampleStart = dataStart + numSamples / 3;
    int sampleEnd = dataEnd - numSamples / 3;

    double sampleSec =
        snapshots.get(sampleEnd).secSinceStart - snapshots.get(sampleStart).secSinceStart;
    if (sampleSec < MIN_WINDOW.getStandardSeconds()) {
      // Not sampled over enough time.
      NexmarkUtils.console(
          "sample of %.1f sec not long enough to calculate steady-state event rate",
          sampleSec);
      return;
    }

    // Find rate with least squares error.
    double sumxx = 0.0;
    double sumxy = 0.0;
    long prevNumEvents = -1;
    for (int i = sampleStart; i <= sampleEnd; i++) {
      if (prevNumEvents == snapshots.get(i).numEvents) {
        // Skip samples with no change in number of events since they contribute no data.
        continue;
      }
      // Use the effective runtime instead of wallclock time so we can
      // insulate ourselves from delays and stutters in the query manager.
      double x = snapshots.get(i).runtimeSec;
      prevNumEvents = snapshots.get(i).numEvents;
      double y = prevNumEvents;
      sumxx += x * x;
      sumxy += x * y;
    }
    double eventsPerSec = sumxy / sumxx;
    NexmarkUtils.console("revising events/sec from %.1f to %.1f", perf.eventsPerSec, eventsPerSec);
    perf.eventsPerSec = eventsPerSec;
  }

  /**
   * Return the current value for a long counter, or -1 if can't be retrieved.
   */
  private long getLong(DataflowPipelineJob job, Aggregator<Long, Long> aggregator) {
    try {
      Collection<Long> values = job.getAggregatorValues(aggregator).getValues();
      if (values.size() != 1) {
        return -1;
      }
      return Iterables.getOnlyElement(values);
    } catch (AggregatorRetrievalException e) {
      return -1;
    }
  }

  /**
   * Return the current value for a time counter, or -1 if can't be retrieved.
   */
  private long getTimestamp(
      long now, DataflowPipelineJob job, Aggregator<Long, Long> aggregator) {
    try {
      Collection<Long> values = job.getAggregatorValues(aggregator).getValues();
      if (values.size() != 1) {
        return -1;
      }
      long value = Iterables.getOnlyElement(values);
      if (Math.abs(value - now) > Duration.standardDays(10000).getMillis()) {
        return -1;
      }
      return value;
    } catch (AggregatorRetrievalException e) {
      return -1;
    }
  }
}
