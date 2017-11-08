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

package org.apache.beam.sdk.nexmark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.DistributionResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.nexmark.queries.NexmarkQuery;

import org.joda.time.Duration;
import org.slf4j.LoggerFactory;

/**
 * NexmarkPerfAnalyzer.
 */
public class NexmarkPerfAnalyzer {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(NexmarkPerfAnalyzer.class);
  /**
   * Minimum number of samples needed for 'stead-state' rate calculation.
   */
  private static final int MIN_SAMPLES = 9;
  /**
   * Minimum length of time over which to consider samples for 'steady-state' rate calculation.
   */
  private static final Duration MIN_WINDOW = Duration.standardMinutes(2);
  /**
   * Delay between perf samples.
   */
  private static final Duration PERF_DELAY = Duration.standardSeconds(15);
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
   * Monitor the performance and progress of a running job. Return final performance if
   * it was measured.
   */

  public static NexmarkPerf monitorQuery(
      NexmarkConfiguration configuration,
      NexmarkOptions options,
      NexmarkQuery query,
      PipelineResult mainPipelineResult,
      PipelineResult publisherPipelineResult) {
    if (!options.getMonitorJobs()) {
      return null;
    }

    if (configuration.debug) {
      NexmarkUtils.console("Waiting for main pipeline to 'finish'");
    } else {
      NexmarkUtils.console("--debug=false, so job will not self-cancel");
    }

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
          mainPipelineResult.cancel();
        } catch (IOException e) {
          throw new RuntimeException("Unable to cancel main job: ", e);
        }
        if (publisherPipelineResult != null) {
          try {
            publisherPipelineResult.cancel();
          } catch (IOException e) {
            throw new RuntimeException("Unable to cancel publisher job: ", e);
          }
          publisherCancelled = true;
        }
        waitingForShutdown = true;
      }

      PipelineResult.State state = mainPipelineResult.getState();
      NexmarkUtils.console("%s %s%s",
          state,
          query.getName(),
          waitingForShutdown ? " (waiting for shutdown)" : "");

      NexmarkPerf currPerf;
      if (configuration.debug) {
        currPerf = currentPerf(
            startMsSinceEpoch,
            now,
            mainPipelineResult,
            snapshots,
            query.eventMonitor,
            query.resultMonitor,
            options.isStreaming());
      } else {
        currPerf = null;
      }

      if (perf == null || perf.anyActivity(currPerf)) {
        lastActivityMsSinceEpoch = now;
      }

      if (options.isStreaming() && !waitingForShutdown) {
        Duration quietFor = new Duration(lastActivityMsSinceEpoch, now);
        long fatalCount = getCounterMetric(
            mainPipelineResult,
            query.getName(),
            "fatal",
            0);

        if (fatalCount > 0) {
          NexmarkUtils.console("job has fatal errors, cancelling.");
          errors.add(String.format("Pipeline reported %s fatal errors", fatalCount));
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

        if (waitingForShutdown) {
          try {
            mainPipelineResult.cancel();
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

    if (publisherPipelineResult != null) {
      NexmarkUtils.console("Shutting down publisher pipeline.");
      try {
        if (!publisherCancelled) {
          publisherPipelineResult.cancel();
        }
        publisherPipelineResult.waitUntilFinish(Duration.standardMinutes(5));
      } catch (IOException e) {
        throw new RuntimeException("Unable to cancel publisher job: ", e);
      }
    }

    return perf;
  }

  /**
   * Return the current performance given {@code eventMonitor} and {@code resultMonitor}.
   */
  private static NexmarkPerf currentPerf(
      long startMsSinceEpoch,
      long now,
      PipelineResult result,
      List<NexmarkPerf.ProgressSnapshot> snapshots,
      Monitor<?> eventMonitor,
      Monitor<?> resultMonitor,
      boolean isStreaming) {
    NexmarkPerf perf = new NexmarkPerf();

    long numEvents =
        getCounterMetric(result, eventMonitor.name, eventMonitor.prefix + ".elements", -1);
    long numEventBytes =
        getCounterMetric(result, eventMonitor.name, eventMonitor.prefix + ".bytes", -1);
    long eventStart =
        getTimestampMetric(now,
            getDistributionMetric(result, eventMonitor.name, eventMonitor.prefix + ".startTime",
                DistributionType.MIN, -1));
    long eventEnd =
        getTimestampMetric(now,
            getDistributionMetric(result, eventMonitor.name, eventMonitor.prefix + ".endTime",
                DistributionType.MAX, -1));

    long numResults =
        getCounterMetric(result, resultMonitor.name, resultMonitor.prefix + ".elements", -1);
    long numResultBytes =
        getCounterMetric(result, resultMonitor.name, resultMonitor.prefix + ".bytes", -1);
    long resultStart =
        getTimestampMetric(now,
            getDistributionMetric(result, resultMonitor.name, resultMonitor.prefix + ".startTime",
                DistributionType.MIN, -1));
    long resultEnd =
        getTimestampMetric(now,
            getDistributionMetric(result, resultMonitor.name, resultMonitor.prefix + ".endTime",
                DistributionType.MAX, -1));
    long timestampStart =
        getTimestampMetric(now,
            getDistributionMetric(result,
                resultMonitor.name, resultMonitor.prefix + ".startTimestamp",
                DistributionType.MIN, -1));
    long timestampEnd =
        getTimestampMetric(now,
            getDistributionMetric(result,
                resultMonitor.name, resultMonitor.prefix + ".endTimestamp",
                DistributionType.MAX, -1));

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

    // As soon as available, try to capture cumulative cost at this point too.

    NexmarkPerf.ProgressSnapshot snapshot = new NexmarkPerf.ProgressSnapshot();
    snapshot.secSinceStart = (now - startMsSinceEpoch) / 1000.0;
    snapshot.runtimeSec = perf.runtimeSec;
    snapshot.numEvents = numEvents;
    snapshot.numResults = numResults;
    snapshots.add(snapshot);

    if (isStreaming) {
      captureSteadyState(perf, snapshots);
    }

    return perf;
  }


  /**
   * Find a 'steady state' events/sec from {@code snapshots} and
   * store it in {@code perf} if found.
   */
  private static void captureSteadyState(
      NexmarkPerf perf,
      List<NexmarkPerf.ProgressSnapshot> snapshots) {

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
   * Return the current value for a time counter, or -1 if can't be retrieved.
   */
  private static long getTimestampMetric(long now, long value) {
    // timestamp metrics are used to monitor time of execution of transforms.
    // If result timestamp metric is too far from now, consider that metric is erroneous

    if (Math.abs(value - now) > Duration.standardDays(10000).getMillis()) {
      return -1;
    }
    return value;
  }



  /**
   * Return the current value for a long counter, or a default value if can't be retrieved.
   * Note this uses only attempted metrics because some runners don't support committed metrics.
   */
  private static long getCounterMetric(PipelineResult result, String namespace, String name,
                                       long defaultValue) {
    MetricQueryResults metrics = result.metrics().queryMetrics(
        MetricsFilter.builder().addNameFilter(MetricNameFilter.named(namespace, name)).build());
    Iterable<MetricResult<Long>> counters = metrics.counters();
    try {
      MetricResult<Long> metricResult = counters.iterator().next();
      return metricResult.attempted();
    } catch (NoSuchElementException e) {
      LOG.error("Failed to get metric {}, from namespace {}", name, namespace);
    }
    return defaultValue;
  }



  /**
   * Return the current value for a long counter, or a default value if can't be retrieved.
   * Note this uses only attempted metrics because some runners don't support committed metrics.
   */
  private static long getDistributionMetric(PipelineResult result, String namespace, String name,
                                            DistributionType distType, long defaultValue) {
    MetricQueryResults metrics = result.metrics().queryMetrics(
        MetricsFilter.builder().addNameFilter(MetricNameFilter.named(namespace, name)).build());
    Iterable<MetricResult<DistributionResult>> distributions = metrics.distributions();
    try {
      MetricResult<DistributionResult> distributionResult = distributions.iterator().next();
      switch (distType) {
        case MIN:
          return distributionResult.attempted().min();
        case MAX:
          return distributionResult.attempted().max();
        default:
          return defaultValue;
      }
    } catch (NoSuchElementException e) {
      LOG.error(
          "Failed to get distribution metric {} for namespace {}",
          name,
          namespace);
    }
    return defaultValue;
  }

  private enum DistributionType {MIN, MAX}



}
