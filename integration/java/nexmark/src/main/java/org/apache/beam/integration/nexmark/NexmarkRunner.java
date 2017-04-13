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

import static com.google.common.base.Preconditions.checkState;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.integration.nexmark.io.PubsubHelper;
import org.apache.beam.integration.nexmark.model.Auction;
import org.apache.beam.integration.nexmark.model.Bid;
import org.apache.beam.integration.nexmark.model.Event;
import org.apache.beam.integration.nexmark.model.KnownSize;
import org.apache.beam.integration.nexmark.model.Person;
import org.apache.beam.integration.nexmark.queries.Query0;
import org.apache.beam.integration.nexmark.queries.Query0Model;
import org.apache.beam.integration.nexmark.queries.Query1;
import org.apache.beam.integration.nexmark.queries.Query10;
import org.apache.beam.integration.nexmark.queries.Query11;
import org.apache.beam.integration.nexmark.queries.Query12;
import org.apache.beam.integration.nexmark.queries.Query1Model;
import org.apache.beam.integration.nexmark.queries.Query2;
import org.apache.beam.integration.nexmark.queries.Query2Model;
import org.apache.beam.integration.nexmark.queries.Query3;
import org.apache.beam.integration.nexmark.queries.Query3Model;
import org.apache.beam.integration.nexmark.queries.Query4;
import org.apache.beam.integration.nexmark.queries.Query4Model;
import org.apache.beam.integration.nexmark.queries.Query5;
import org.apache.beam.integration.nexmark.queries.Query5Model;
import org.apache.beam.integration.nexmark.queries.Query6;
import org.apache.beam.integration.nexmark.queries.Query6Model;
import org.apache.beam.integration.nexmark.queries.Query7;
import org.apache.beam.integration.nexmark.queries.Query7Model;
import org.apache.beam.integration.nexmark.queries.Query8;
import org.apache.beam.integration.nexmark.queries.Query8Model;
import org.apache.beam.integration.nexmark.queries.Query9;
import org.apache.beam.integration.nexmark.queries.Query9Model;
import org.apache.beam.sdk.AggregatorRetrievalException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;

/**
 * Run a single Nexmark query using a given configuration.
 */
public abstract class NexmarkRunner<OptionT extends NexmarkOptions> {
  /**
   * Minimum number of samples needed for 'stead-state' rate calculation.
   */
  protected static final int MIN_SAMPLES = 9;
  /**
   * Minimum length of time over which to consider samples for 'steady-state' rate calculation.
   */
  protected static final Duration MIN_WINDOW = Duration.standardMinutes(2);
  /**
   * Delay between perf samples.
   */
  protected static final Duration PERF_DELAY = Duration.standardSeconds(15);
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
   * NexmarkOptions shared by all runs.
   */
  protected final OptionT options;

  /**
   * Which configuration we are running.
   */
  @Nullable
  protected NexmarkConfiguration configuration;

  /**
   * Accumulate the pub/sub subscriptions etc which should be cleaned up on end of run.
   */
  @Nullable
  protected PubsubHelper pubsub;

  /**
   * If in --pubsubMode=COMBINED, the event monitor for the publisher pipeline. Otherwise null.
   */
  @Nullable
  protected Monitor<Event> publisherMonitor;

  /**
   * If in --pubsubMode=COMBINED, the pipeline result for the publisher pipeline. Otherwise null.
   */
  @Nullable
  protected PipelineResult publisherResult;

  /**
   * Result for the main pipeline.
   */
  @Nullable
  protected PipelineResult mainResult;

  /**
   * Query name we are running.
   */
  @Nullable
  protected String queryName;

  public NexmarkRunner(OptionT options) {
    this.options = options;
  }

  /**
   * Return a Pubsub helper.
   */
  private PubsubHelper getPubsub() {
    if (pubsub == null) {
      pubsub = PubsubHelper.create(options);
    }
    return pubsub;
  }

  // ================================================================================
  // Overridden by each runner.
  // ================================================================================

  /**
   * Is this query running in streaming mode?
   */
  protected abstract boolean isStreaming();

  /**
   * Return number of cores per worker.
   */
  protected abstract int coresPerWorker();

  /**
   * Return maximum number of workers.
   */
  protected abstract int maxNumWorkers();

  /**
   * Return the current value for a long counter, or -1 if can't be retrieved.
   */
  protected long getLong(PipelineResult job, Aggregator<Long, Long> aggregator) {
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
  protected long getTimestamp(
    long now, PipelineResult job, Aggregator<Long, Long> aggregator) {
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

  /**
   * Find a 'steady state' events/sec from {@code snapshots} and
   * store it in {@code perf} if found.
   */
  protected void captureSteadyState(NexmarkPerf perf,
                                    List<NexmarkPerf.ProgressSnapshot> snapshots) {
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
   * Return the current performance given {@code eventMonitor} and {@code resultMonitor}.
   */
  private NexmarkPerf currentPerf(
      long startMsSinceEpoch, long now, PipelineResult job,
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

    perf.jobId = getJobId(job);
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

  protected String getJobId(PipelineResult job) {
    return "";
  }

  // TODO specific to dataflow, see if we can find an equivalent
/*
  protected MetricType getMetricType(MetricUpdate metric) {
    String metricName = metric.getKey().metricName().name();
    if (metricName.endsWith("windmill-system-watermark")) {
      return MetricType.SYSTEM_WATERMARK;
    } else if (metricName.endsWith("windmill-data-watermark")) {
      return MetricType.DATA_WATERMARK;
    } else {
      return MetricType.OTHER;
    }
  }
*/

  /**
   * Check that watermarks are not too far behind.
   *
   * <p>Returns a list of errors detected.
   */
  // TODO specific to dataflow, see if we can find an equivalent
  /*
  private List<String> checkWatermarks(DataflowPipelineJob job, long startMsSinceEpoch) {
    long now = System.currentTimeMillis();
    List<String> errors = new ArrayList<>();
    try {
      JobMetrics metricResponse = job.getDataflowClient()
                                     .projects()
                                     .jobs()
                                     .getMetrics(job.getProjectId(), job.getJobId())
                                     .execute();
          List<MetricUpdate> metrics = metricResponse.getMetrics();



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
                      metric.getKey().metricName().name(), value, updateTime, threshold);
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
    } catch (IOException e) {
      NexmarkUtils.console("Warning: failed to get JobMetrics: " + e);
    }

    return errors;
  }
*/

  // TODO specific to dataflow, see if we can find an equivalent
/*
  enum MetricType {
    SYSTEM_WATERMARK,
    DATA_WATERMARK,
    OTHER
  }
*/

  /**
   * Build and run a pipeline using specified options.
   */
  protected interface PipelineBuilder<OptionT extends NexmarkOptions> {
    void build(OptionT publishOnlyOptions);
  }

  /**
   * Invoke the builder with options suitable for running a publish-only child pipeline.
   */
  protected abstract void invokeBuilderForPublishOnlyPipeline(PipelineBuilder builder);

  /**
   * If monitoring, wait until the publisher pipeline has run long enough to establish
   * a backlog on the Pubsub topic. Otherwise, return immediately.
   */
  protected abstract void waitForPublisherPreload();

  /**
   * Monitor the performance and progress of a running job. Return final performance if
   * it was measured.
   */
  @Nullable
  protected NexmarkPerf monitor(NexmarkQuery query) {
    if (!options.getMonitorJobs()) {
      return null;
    }

    if (configuration.debug) {
      NexmarkUtils.console("Waiting for main pipeline to 'finish'");
    } else {
      NexmarkUtils.console("--debug=false, so job will not self-cancel");
    }

    PipelineResult job = mainResult;
    PipelineResult publisherJob = publisherResult;
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

        // TODO specific to dataflow, see if we can find an equivalent
//        errors.addAll(checkWatermarks(job, startMsSinceEpoch));

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

  // ================================================================================
  // Basic sources and sinks
  // ================================================================================

  /**
   * Return a topic name.
   */
  private String shortTopic(long now) {
    String baseTopic = options.getPubsubTopic();
    if (Strings.isNullOrEmpty(baseTopic)) {
      throw new RuntimeException("Missing --pubsubTopic");
    }
    switch (options.getResourceNameMode()) {
      case VERBATIM:
        return baseTopic;
      case QUERY:
        return String.format("%s_%s_source", baseTopic, queryName);
      case QUERY_AND_SALT:
        return String.format("%s_%s_%d_source", baseTopic, queryName, now);
    }
    throw new RuntimeException("Unrecognized enum " + options.getResourceNameMode());
  }

  /**
   * Return a subscription name.
   */
  private String shortSubscription(long now) {
    String baseSubscription = options.getPubsubSubscription();
    if (Strings.isNullOrEmpty(baseSubscription)) {
      throw new RuntimeException("Missing --pubsubSubscription");
    }
    switch (options.getResourceNameMode()) {
      case VERBATIM:
        return baseSubscription;
      case QUERY:
        return String.format("%s_%s_source", baseSubscription, queryName);
      case QUERY_AND_SALT:
        return String.format("%s_%s_%d_source", baseSubscription, queryName, now);
    }
    throw new RuntimeException("Unrecognized enum " + options.getResourceNameMode());
  }

  /**
   * Return a file name for plain text.
   */
  private String textFilename(long now) {
    String baseFilename = options.getOutputPath();
    if (Strings.isNullOrEmpty(baseFilename)) {
      throw new RuntimeException("Missing --outputPath");
    }
    switch (options.getResourceNameMode()) {
      case VERBATIM:
        return baseFilename;
      case QUERY:
        return String.format("%s/nexmark_%s.txt", baseFilename, queryName);
      case QUERY_AND_SALT:
        return String.format("%s/nexmark_%s_%d.txt", baseFilename, queryName, now);
    }
    throw new RuntimeException("Unrecognized enum " + options.getResourceNameMode());
  }

  /**
   * Return a BigQuery table spec.
   */
  private String tableSpec(long now, String version) {
    String baseTableName = options.getBigQueryTable();
    if (Strings.isNullOrEmpty(baseTableName)) {
      throw new RuntimeException("Missing --bigQueryTable");
    }
    switch (options.getResourceNameMode()) {
      case VERBATIM:
        return String.format("%s:nexmark.%s_%s",
                             options.getProject(), baseTableName, version);
      case QUERY:
        return String.format("%s:nexmark.%s_%s_%s",
                             options.getProject(), baseTableName, queryName, version);
      case QUERY_AND_SALT:
        return String.format("%s:nexmark.%s_%s_%s_%d",
                             options.getProject(), baseTableName, queryName, version, now);
    }
    throw new RuntimeException("Unrecognized enum " + options.getResourceNameMode());
  }

  /**
   * Return a directory for logs.
   */
  private String logsDir(long now) {
    String baseFilename = options.getOutputPath();
    if (Strings.isNullOrEmpty(baseFilename)) {
      throw new RuntimeException("Missing --outputPath");
    }
    switch (options.getResourceNameMode()) {
      case VERBATIM:
        return baseFilename;
      case QUERY:
        return String.format("%s/logs_%s", baseFilename, queryName);
      case QUERY_AND_SALT:
        return String.format("%s/logs_%s_%d", baseFilename, queryName, now);
    }
    throw new RuntimeException("Unrecognized enum " + options.getResourceNameMode());
  }

  /**
   * Return a source of synthetic events.
   */
  private PCollection<Event> sourceEventsFromSynthetic(Pipeline p) {
    if (isStreaming()) {
      NexmarkUtils.console("Generating %d events in streaming mode", configuration.numEvents);
      return p.apply(queryName + ".ReadUnbounded", NexmarkUtils.streamEventsSource(configuration));
    } else {
      NexmarkUtils.console("Generating %d events in batch mode", configuration.numEvents);
      return p.apply(queryName + ".ReadBounded", NexmarkUtils.batchEventsSource(configuration));
    }
  }

  /**
   * Return source of events from Pubsub.
   */
  private PCollection<Event> sourceEventsFromPubsub(Pipeline p, long now) {
    String shortTopic = shortTopic(now);
    String shortSubscription = shortSubscription(now);

    // Create/confirm the subscription.
    String subscription = null;
    if (!options.getManageResources()) {
      // The subscription should already have been created by the user.
      subscription = getPubsub().reuseSubscription(shortTopic, shortSubscription).getPath();
    } else {
      subscription = getPubsub().createSubscription(shortTopic, shortSubscription).getPath();
    }
    NexmarkUtils.console("Reading events from Pubsub %s", subscription);
    PubsubIO.Read<Event> io =
        PubsubIO.<Event>read().subscription(subscription)
            .idLabel(NexmarkUtils.PUBSUB_ID)
            .withCoder(Event.CODER);
    if (!configuration.usePubsubPublishTime) {
      io = io.timestampLabel(NexmarkUtils.PUBSUB_TIMESTAMP);
    }
    return p.apply(queryName + ".ReadPubsubEvents", io);
  }

  /**
   * Return Avro source of events from {@code options.getInputFilePrefix}.
   */
  private PCollection<Event> sourceEventsFromAvro(Pipeline p) {
    String filename = options.getInputPath();
    if (Strings.isNullOrEmpty(filename)) {
      throw new RuntimeException("Missing --inputPath");
    }
    NexmarkUtils.console("Reading events from Avro files at %s", filename);
    return p
        .apply(queryName + ".ReadAvroEvents", AvroIO.Read
                          .from(filename + "*.avro")
                          .withSchema(Event.class))
        .apply("OutputWithTimestamp", NexmarkQuery.EVENT_TIMESTAMP_FROM_DATA);
  }

  /**
   * Send {@code events} to Pubsub.
   */
  private void sinkEventsToPubsub(PCollection<Event> events, long now) {
    String shortTopic = shortTopic(now);

    // Create/confirm the topic.
    String topic;
    if (!options.getManageResources()
        || configuration.pubSubMode == NexmarkUtils.PubSubMode.SUBSCRIBE_ONLY) {
      // The topic should already have been created by the user or
      // a companion 'PUBLISH_ONLY' process.
      topic = getPubsub().reuseTopic(shortTopic).getPath();
    } else {
      // Create a fresh topic to loopback via. It will be destroyed when the
      // (necessarily blocking) job is done.
      topic = getPubsub().createTopic(shortTopic).getPath();
    }
    NexmarkUtils.console("Writing events to Pubsub %s", topic);
    PubsubIO.Write<Event> io =
        PubsubIO.<Event>write().topic(topic)
                      .idLabel(NexmarkUtils.PUBSUB_ID)
                      .withCoder(Event.CODER);
    if (!configuration.usePubsubPublishTime) {
      io = io.timestampLabel(NexmarkUtils.PUBSUB_TIMESTAMP);
    }
    events.apply(queryName + ".WritePubsubEvents", io);
  }

  /**
   * Send {@code formattedResults} to Pubsub.
   */
  private void sinkResultsToPubsub(PCollection<String> formattedResults, long now) {
    String shortTopic = shortTopic(now);
    String topic;
    if (!options.getManageResources()) {
      topic = getPubsub().reuseTopic(shortTopic).getPath();
    } else {
      topic = getPubsub().createTopic(shortTopic).getPath();
    }
    NexmarkUtils.console("Writing results to Pubsub %s", topic);
    PubsubIO.Write<String> io =
        PubsubIO.<String>write().topic(topic)
            .idLabel(NexmarkUtils.PUBSUB_ID);
    if (!configuration.usePubsubPublishTime) {
      io = io.timestampLabel(NexmarkUtils.PUBSUB_TIMESTAMP);
    }
    formattedResults.apply(queryName + ".WritePubsubResults", io);
  }

  /**
   * Sink all raw Events in {@code source} to {@code options.getOutputPath}.
   * This will configure the job to write the following files:
   * <ul>
   * <li>{@code $outputPath/event*.avro} All Event entities.
   * <li>{@code $outputPath/auction*.avro} Auction entities.
   * <li>{@code $outputPath/bid*.avro} Bid entities.
   * <li>{@code $outputPath/person*.avro} Person entities.
   * </ul>
   *
   * @param source A PCollection of events.
   */
  private void sinkEventsToAvro(PCollection<Event> source) {
    String filename = options.getOutputPath();
    if (Strings.isNullOrEmpty(filename)) {
      throw new RuntimeException("Missing --outputPath");
    }
    NexmarkUtils.console("Writing events to Avro files at %s", filename);
    source.apply(queryName + ".WriteAvroEvents",
            AvroIO.Write.to(filename + "/event").withSuffix(".avro").withSchema(Event.class));
    source.apply(NexmarkQuery.JUST_BIDS)
          .apply(queryName + ".WriteAvroBids",
            AvroIO.Write.to(filename + "/bid").withSuffix(".avro").withSchema(Bid.class));
    source.apply(NexmarkQuery.JUST_NEW_AUCTIONS)
          .apply(queryName + ".WriteAvroAuctions",
                  AvroIO.Write.to(filename + "/auction").withSuffix(".avro")
                          .withSchema(Auction.class));
    source.apply(NexmarkQuery.JUST_NEW_PERSONS)
          .apply(queryName + ".WriteAvroPeople",
                  AvroIO.Write.to(filename + "/person").withSuffix(".avro")
                             .withSchema(Person.class));
  }

  /**
   * Send {@code formattedResults} to text files.
   */
  private void sinkResultsToText(PCollection<String> formattedResults, long now) {
    String filename = textFilename(now);
    NexmarkUtils.console("Writing results to text files at %s", filename);
    formattedResults.apply(queryName + ".WriteTextResults",
        TextIO.Write.to(filename));
  }

  private static class StringToTableRow extends DoFn<String, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      int n = ThreadLocalRandom.current().nextInt(10);
      List<TableRow> records = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        records.add(new TableRow().set("index", i).set("value", Integer.toString(i)));
      }
      c.output(new TableRow().set("result", c.element()).set("records", records));
    }
  }

  /**
   * Send {@code formattedResults} to BigQuery.
   */
  private void sinkResultsToBigQuery(
      PCollection<String> formattedResults, long now,
      String version) {
    String tableSpec = tableSpec(now, version);
    TableSchema tableSchema =
        new TableSchema().setFields(ImmutableList.of(
            new TableFieldSchema().setName("result").setType("STRING"),
            new TableFieldSchema().setName("records").setMode("REPEATED").setType("RECORD")
                                  .setFields(ImmutableList.of(
                                      new TableFieldSchema().setName("index").setType("INTEGER"),
                                      new TableFieldSchema().setName("value").setType("STRING")))));
    NexmarkUtils.console("Writing results to BigQuery table %s", tableSpec);
    BigQueryIO.Write io =
        BigQueryIO.write().to(tableSpec)
                        .withSchema(tableSchema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND);
    formattedResults
        .apply(queryName + ".StringToTableRow", ParDo.of(new StringToTableRow()))
        .apply(queryName + ".WriteBigQueryResults", io);
  }

  // ================================================================================
  // Construct overall pipeline
  // ================================================================================

  /**
   * Return source of events for this run, or null if we are simply publishing events
   * to Pubsub.
   */
  private PCollection<Event> createSource(Pipeline p, final long now) {
    PCollection<Event> source = null;
    switch (configuration.sourceType) {
      case DIRECT:
        source = sourceEventsFromSynthetic(p);
        break;
      case AVRO:
        source = sourceEventsFromAvro(p);
        break;
      case PUBSUB:
        // Setup the sink for the publisher.
        switch (configuration.pubSubMode) {
          case SUBSCRIBE_ONLY:
            // Nothing to publish.
            break;
          case PUBLISH_ONLY:
            // Send synthesized events to Pubsub in this job.
            sinkEventsToPubsub(sourceEventsFromSynthetic(p).apply(queryName + ".Snoop",
                    NexmarkUtils.snoop(queryName)), now);
            break;
          case COMBINED:
            // Send synthesized events to Pubsub in separate publisher job.
            // We won't start the main pipeline until the publisher has sent the pre-load events.
            // We'll shutdown the publisher job when we notice the main job has finished.
            invokeBuilderForPublishOnlyPipeline(new PipelineBuilder() {
              @Override
              public void build(NexmarkOptions publishOnlyOptions) {
                Pipeline sp = Pipeline.create(options);
                NexmarkUtils.setupPipeline(configuration.coderStrategy, sp);
                publisherMonitor = new Monitor<Event>(queryName, "publisher");
                sinkEventsToPubsub(
                    sourceEventsFromSynthetic(sp)
                            .apply(queryName + ".Monitor", publisherMonitor.getTransform()),
                    now);
                publisherResult = sp.run();
              }
            });
            break;
        }

        // Setup the source for the consumer.
        switch (configuration.pubSubMode) {
          case PUBLISH_ONLY:
            // Nothing to consume. Leave source null.
            break;
          case SUBSCRIBE_ONLY:
          case COMBINED:
            // Read events from pubsub.
            source = sourceEventsFromPubsub(p, now);
            break;
        }
        break;
    }
    return source;
  }

  private static final TupleTag<String> MAIN = new TupleTag<String>(){};
  private static final TupleTag<String> SIDE = new TupleTag<String>(){};

  private static class PartitionDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.element().hashCode() % 2 == 0) {
        c.output(c.element());
      } else {
        c.sideOutput(SIDE, c.element());
      }
    }
  }

  /**
   * Consume {@code results}.
   */
  private void sink(PCollection<TimestampedValue<KnownSize>> results, long now) {
    if (configuration.sinkType == NexmarkUtils.SinkType.COUNT_ONLY) {
      // Avoid the cost of formatting the results.
      results.apply(queryName + ".DevNull", NexmarkUtils.devNull(queryName));
      return;
    }

    PCollection<String> formattedResults =
      results.apply(queryName + ".Format", NexmarkUtils.format(queryName));
    if (options.getLogResults()) {
      formattedResults = formattedResults.apply(queryName + ".Results.Log",
              NexmarkUtils.<String>log(queryName + ".Results"));
    }

    switch (configuration.sinkType) {
      case DEVNULL:
        // Discard all results
        formattedResults.apply(queryName + ".DevNull", NexmarkUtils.devNull(queryName));
        break;
      case PUBSUB:
        sinkResultsToPubsub(formattedResults, now);
        break;
      case TEXT:
        sinkResultsToText(formattedResults, now);
        break;
      case AVRO:
        NexmarkUtils.console(
            "WARNING: with --sinkType=AVRO, actual query results will be discarded.");
        break;
      case BIGQUERY:
        // Multiple BigQuery backends to mimic what most customers do.
        PCollectionTuple res = formattedResults.apply(queryName + ".Partition",
            ParDo.of(new PartitionDoFn()).withOutputTags(MAIN, TupleTagList.of(SIDE)));
        sinkResultsToBigQuery(res.get(MAIN), now, "main");
        sinkResultsToBigQuery(res.get(SIDE), now, "side");
        sinkResultsToBigQuery(formattedResults, now, "copy");
        break;
      case COUNT_ONLY:
        // Short-circuited above.
        throw new RuntimeException();
    }
  }

  // ================================================================================
  // Entry point
  // ================================================================================

  /**
   * Calculate the distribution of the expected rate of results per minute (in event time, not
   * wallclock time).
   */
  private void modelResultRates(NexmarkQueryModel model) {
    List<Long> counts = Lists.newArrayList(model.simulator().resultsPerWindow());
    Collections.sort(counts);
    int n = counts.size();
    if (n < 5) {
      NexmarkUtils.console("Query%d: only %d samples", model.configuration.query, n);
    } else {
      NexmarkUtils.console("Query%d: N:%d; min:%d; 1st%%:%d; mean:%d; 3rd%%:%d; max:%d",
                           model.configuration.query, n, counts.get(0), counts.get(n / 4),
                           counts.get(n / 2),
                           counts.get(n - 1 - n / 4), counts.get(n - 1));
    }
  }

  /**
   * Run {@code configuration} and return its performance if possible.
   */
  @Nullable
  public NexmarkPerf run(NexmarkConfiguration runConfiguration) {
    if (options.getManageResources() && !options.getMonitorJobs()) {
      throw new RuntimeException("If using --manageResources then must also use --monitorJobs.");
    }

    //
    // Setup per-run state.
    //
    checkState(configuration == null);
    checkState(pubsub == null);
    checkState(queryName == null);
    configuration = runConfiguration;

    // GCS URI patterns to delete on exit.
    List<String> pathsToDelete = new ArrayList<>();

    try {
      NexmarkUtils.console("Running %s", configuration.toShortString());

      if (configuration.numEvents < 0) {
        NexmarkUtils.console("skipping since configuration is disabled");
        return null;
      }

      List<NexmarkQuery> queries = Arrays.asList(new Query0(configuration),
                                                 new Query1(configuration),
                                                 new Query2(configuration),
                                                 new Query3(configuration),
                                                 new Query4(configuration),
                                                 new Query5(configuration),
                                                 new Query6(configuration),
                                                 new Query7(configuration),
                                                 new Query8(configuration),
                                                 new Query9(configuration),
                                                 new Query10(configuration),
                                                 new Query11(configuration),
                                                 new Query12(configuration));
      NexmarkQuery query = queries.get(configuration.query);
      queryName = query.getName();

      List<NexmarkQueryModel> models = Arrays.asList(
          new Query0Model(configuration),
          new Query1Model(configuration),
          new Query2Model(configuration),
          new Query3Model(configuration),
          new Query4Model(configuration),
          new Query5Model(configuration),
          new Query6Model(configuration),
          new Query7Model(configuration),
          new Query8Model(configuration),
          new Query9Model(configuration),
          null,
          null,
          null);
      NexmarkQueryModel model = models.get(configuration.query);

      if (options.getJustModelResultRate()) {
        if (model == null) {
          throw new RuntimeException(String.format("No model for %s", queryName));
        }
        modelResultRates(model);
        return null;
      }

      long now = System.currentTimeMillis();
      Pipeline p = Pipeline.create(options);
      NexmarkUtils.setupPipeline(configuration.coderStrategy, p);

      // Generate events.
      PCollection<Event> source = createSource(p, now);

      if (options.getLogEvents()) {
        source = source.apply(queryName + ".Events.Log",
                NexmarkUtils.<Event>log(queryName + ".Events"));
      }

      // Source will be null if source type is PUBSUB and mode is PUBLISH_ONLY.
      // In that case there's nothing more to add to pipeline.
      if (source != null) {
        // Optionally sink events in Avro format.
        // (Query results are ignored).
        if (configuration.sinkType == NexmarkUtils.SinkType.AVRO) {
          sinkEventsToAvro(source);
        }

        // Special hacks for Query 10 (big logger).
        if (configuration.query == 10) {
          String path = null;
          if (options.getOutputPath() != null && !options.getOutputPath().isEmpty()) {
            path = logsDir(now);
          }
          ((Query10) query).setOutputPath(path);
          ((Query10) query).setMaxNumWorkers(maxNumWorkers());
          if (path != null && options.getManageResources()) {
            pathsToDelete.add(path + "/**");
          }
        }

        // Apply query.
        PCollection<TimestampedValue<KnownSize>> results = source.apply(query);

        if (options.getAssertCorrectness()) {
          if (model == null) {
            throw new RuntimeException(String.format("No model for %s", queryName));
          }
          // We know all our streams have a finite number of elements.
          results.setIsBoundedInternal(PCollection.IsBounded.BOUNDED);
          // If we have a finite number of events then assert our pipeline's
          // results match those of a model using the same sequence of events.
          PAssert.that(results).satisfies(model.assertionFor());
        }

        // Output results.
        sink(results, now);
      }

      if (publisherResult != null) {
        waitForPublisherPreload();
      }
      mainResult = p.run();
      mainResult.waitUntilFinish();
      return monitor(query);
    } finally {
      //
      // Cleanup per-run state.
      //
      if (pubsub != null) {
        // Delete any subscriptions and topics we created.
        pubsub.close();
        pubsub = null;
      }
      configuration = null;
      queryName = null;
      // TODO: Cleanup pathsToDelete
    }

  }

}
