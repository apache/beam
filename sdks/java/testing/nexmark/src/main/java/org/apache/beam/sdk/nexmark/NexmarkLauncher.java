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

import static org.apache.beam.sdk.nexmark.NexmarkUtils.PubSubMode.COMBINED;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.nexmark.NexmarkUtils.PubSubMode;
import org.apache.beam.sdk.nexmark.NexmarkUtils.PubsubMessageSerializationMethod;
import org.apache.beam.sdk.nexmark.NexmarkUtils.SourceType;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Event;
import org.apache.beam.sdk.nexmark.model.KnownSize;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.beam.sdk.nexmark.queries.BoundedSideInputJoin;
import org.apache.beam.sdk.nexmark.queries.BoundedSideInputJoinModel;
import org.apache.beam.sdk.nexmark.queries.NexmarkQuery;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryModel;
import org.apache.beam.sdk.nexmark.queries.NexmarkQueryUtil;
import org.apache.beam.sdk.nexmark.queries.Query0;
import org.apache.beam.sdk.nexmark.queries.Query0Model;
import org.apache.beam.sdk.nexmark.queries.Query1;
import org.apache.beam.sdk.nexmark.queries.Query10;
import org.apache.beam.sdk.nexmark.queries.Query11;
import org.apache.beam.sdk.nexmark.queries.Query12;
import org.apache.beam.sdk.nexmark.queries.Query1Model;
import org.apache.beam.sdk.nexmark.queries.Query2;
import org.apache.beam.sdk.nexmark.queries.Query2Model;
import org.apache.beam.sdk.nexmark.queries.Query3;
import org.apache.beam.sdk.nexmark.queries.Query3Model;
import org.apache.beam.sdk.nexmark.queries.Query4;
import org.apache.beam.sdk.nexmark.queries.Query4Model;
import org.apache.beam.sdk.nexmark.queries.Query5;
import org.apache.beam.sdk.nexmark.queries.Query5Model;
import org.apache.beam.sdk.nexmark.queries.Query6;
import org.apache.beam.sdk.nexmark.queries.Query6Model;
import org.apache.beam.sdk.nexmark.queries.Query7;
import org.apache.beam.sdk.nexmark.queries.Query7Model;
import org.apache.beam.sdk.nexmark.queries.Query8;
import org.apache.beam.sdk.nexmark.queries.Query8Model;
import org.apache.beam.sdk.nexmark.queries.Query9;
import org.apache.beam.sdk.nexmark.queries.Query9Model;
import org.apache.beam.sdk.nexmark.queries.SessionSideInputJoin;
import org.apache.beam.sdk.nexmark.queries.SessionSideInputJoinModel;
import org.apache.beam.sdk.nexmark.queries.sql.SqlBoundedSideInputJoin;
import org.apache.beam.sdk.nexmark.queries.sql.SqlQuery0;
import org.apache.beam.sdk.nexmark.queries.sql.SqlQuery1;
import org.apache.beam.sdk.nexmark.queries.sql.SqlQuery2;
import org.apache.beam.sdk.nexmark.queries.sql.SqlQuery3;
import org.apache.beam.sdk.nexmark.queries.sql.SqlQuery7;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.LoggerFactory;

/** Run a single Nexmark query using a given configuration. */
public class NexmarkLauncher<OptionT extends NexmarkOptions> {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(NexmarkLauncher.class);

  /** Command line parameter value for query language. */
  private static final String SQL = "sql";

  /** Command line parameter value for zetasql language. */
  private static final String ZETA_SQL = "zetasql";

  /** Minimum number of samples needed for 'stead-state' rate calculation. */
  private static final int MIN_SAMPLES = 9;
  /** Minimum length of time over which to consider samples for 'steady-state' rate calculation. */
  private static final Duration MIN_WINDOW = Duration.standardMinutes(2);
  /** Delay between perf samples. */
  private static final Duration PERF_DELAY = Duration.standardSeconds(15);
  /**
   * How long to let streaming pipeline run after all events have been generated and we've seen no
   * activity.
   */
  private static final Duration DONE_DELAY = Duration.standardMinutes(1);
  /** How long to allow no activity at sources and sinks without warning. */
  private static final Duration STUCK_WARNING_DELAY = Duration.standardMinutes(10);
  /**
   * How long to let streaming pipeline run after we've seen no activity at sources or sinks, even
   * if all events have not been generated.
   */
  private static final Duration STUCK_TERMINATE_DELAY = Duration.standardHours(1);

  /** NexmarkOptions for this run. */
  private final OptionT options;

  /** Which configuration we are running. */
  private NexmarkConfiguration configuration;

  /** If in --pubsubMode=COMBINED, the event monitor for the publisher pipeline. Otherwise null. */
  private @Nullable Monitor<Event> publisherMonitor;

  /**
   * If in --pubsubMode=COMBINED, the pipeline result for the publisher pipeline. Otherwise null.
   */
  private @Nullable PipelineResult publisherResult;

  /** Result for the main pipeline. */
  private @Nullable PipelineResult mainResult;

  /** Query name we are running. */
  private @Nullable String queryName;

  /** Full path of the PubSub topic (when PubSub is enabled). */
  private @Nullable String pubsubTopic;

  /** Full path of the PubSub subscription (when PubSub is enabled). */
  private @Nullable String pubsubSubscription;

  private @Nullable PubsubHelper pubsubHelper;
  private final Map<NexmarkQueryName, NexmarkQuery> queries;
  private final Map<NexmarkQueryName, NexmarkQueryModel> models;

  public NexmarkLauncher(OptionT options, NexmarkConfiguration configuration) {
    this.options = options;
    this.configuration = configuration;
    queries = createQueries();
    models = createQueryModels();
  }

  /** Is this query running in streaming mode? */
  private boolean isStreaming() {
    return options.isStreaming();
  }

  /** Return maximum number of workers. */
  private int maxNumWorkers() {
    return 5;
  }

  /**
   * Find a 'steady state' events/sec from {@code snapshots} and store it in {@code perf} if found.
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
      NexmarkUtils.console(
          "%d samples not enough to calculate steady-state event rate", numSamples);
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
          "sample of %.1f sec not long enough to calculate steady-state event rate", sampleSec);
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

  /** Return the current performance given {@code eventMonitor} and {@code resultMonitor}. */
  private NexmarkPerf currentPerf(
      long startMsSinceEpoch,
      long now,
      PipelineResult result,
      List<NexmarkPerf.ProgressSnapshot> snapshots,
      Monitor<?> eventMonitor,
      Monitor<?> resultMonitor) {
    NexmarkPerf perf = new NexmarkPerf();

    MetricsReader eventMetrics = new MetricsReader(result, eventMonitor.name);

    long numEvents = eventMetrics.getCounterMetric(eventMonitor.prefix + ".elements");
    long numEventBytes = eventMetrics.getCounterMetric(eventMonitor.prefix + ".bytes");
    long eventStart = eventMetrics.getStartTimeMetric(eventMonitor.prefix + ".startTime");
    long eventEnd = eventMetrics.getEndTimeMetric(eventMonitor.prefix + ".endTime");

    MetricsReader resultMetrics = new MetricsReader(result, resultMonitor.name);

    long numResults = resultMetrics.getCounterMetric(resultMonitor.prefix + ".elements");
    long numResultBytes = resultMetrics.getCounterMetric(resultMonitor.prefix + ".bytes");
    long resultStart = resultMetrics.getStartTimeMetric(resultMonitor.prefix + ".startTime");
    long resultEnd = resultMetrics.getEndTimeMetric(resultMonitor.prefix + ".endTime");
    long timestampStart =
        resultMetrics.getStartTimeMetric(resultMonitor.prefix + ".startTimestamp");
    long timestampEnd = resultMetrics.getEndTimeMetric(resultMonitor.prefix + ".endTimestamp");

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

    captureSteadyState(perf, snapshots);

    return perf;
  }

  /** Build and run a pipeline using specified options. */
  interface PipelineBuilder<OptionT extends NexmarkOptions> {
    void build(OptionT publishOnlyOptions);
  }

  /** Invoke the builder with options suitable for running a publish-only child pipeline. */
  private void invokeBuilderForPublishOnlyPipeline(PipelineBuilder<NexmarkOptions> builder) {
    String jobName = options.getJobName();
    String appName = options.getAppName();
    int numWorkers = options.getNumWorkers();
    int maxNumWorkers = options.getMaxNumWorkers();

    options.setJobName("p-" + jobName);
    options.setAppName("p-" + appName);
    int eventGeneratorWorkers = configuration.numEventGenerators;
    // TODO: assign one generator per core rather than one per worker.
    if (numWorkers > 0 && eventGeneratorWorkers > 0) {
      options.setNumWorkers(Math.min(numWorkers, eventGeneratorWorkers));
    }
    if (maxNumWorkers > 0 && eventGeneratorWorkers > 0) {
      options.setMaxNumWorkers(Math.min(maxNumWorkers, eventGeneratorWorkers));
    }
    try {
      builder.build(options);
    } finally {
      options.setJobName(jobName);
      options.setAppName(appName);
      options.setNumWorkers(numWorkers);
      options.setMaxNumWorkers(maxNumWorkers);
    }
  }

  /**
   * Monitor the performance and progress of a running job. Return final performance if it was
   * measured.
   */
  private @Nullable NexmarkPerf monitor(NexmarkQuery query) {
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
      endMsSinceEpoch =
          startMsSinceEpoch
              + Duration.standardMinutes(options.getRunningTimeMinutes()).getMillis()
              - Duration.standardSeconds(configuration.preloadSeconds).getMillis();
    }
    long lastActivityMsSinceEpoch = -1;
    NexmarkPerf perf = null;
    boolean waitingForShutdown = false;
    boolean cancelJob = false;
    boolean publisherCancelled = false;
    List<String> errors = new ArrayList<>();

    while (true) {
      long now = System.currentTimeMillis();
      if (endMsSinceEpoch >= 0 && now > endMsSinceEpoch && !waitingForShutdown) {
        NexmarkUtils.console("Reached end of test, cancelling job");
        try {
          cancelJob = true;
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
      NexmarkUtils.console(
          "%s %s%s", state, queryName, waitingForShutdown ? " (waiting for shutdown)" : "");

      NexmarkPerf currPerf;
      if (configuration.debug) {
        currPerf =
            currentPerf(
                startMsSinceEpoch, now, job, snapshots, query.eventMonitor, query.resultMonitor);
      } else {
        currPerf = null;
      }

      if (perf == null || perf.anyActivity(currPerf)) {
        lastActivityMsSinceEpoch = now;
      }

      if (options.isStreaming() && !waitingForShutdown) {
        Duration quietFor = new Duration(lastActivityMsSinceEpoch, now);
        long fatalCount = new MetricsReader(job, query.getName()).getCounterMetric("fatal");

        if (fatalCount == -1) {
          fatalCount = 0;
        }

        if (fatalCount > 0) {
          NexmarkUtils.console("ERROR: job has fatal errors, cancelling.");
          errors.add(String.format("Pipeline reported %s fatal errors", fatalCount));
          waitingForShutdown = true;
          cancelJob = true;
        } else if (configuration.debug
            && configuration.numEvents > 0
            && currPerf.numEvents >= configuration.numEvents
            && currPerf.numResults >= 0
            && quietFor.isLongerThan(DONE_DELAY)) {
          NexmarkUtils.console("streaming query appears to have finished waiting for completion.");
          waitingForShutdown = true;
          if (options.getCancelStreamingJobAfterFinish()) {
            cancelJob = true;
          }
        } else if (quietFor.isLongerThan(STUCK_TERMINATE_DELAY)) {
          NexmarkUtils.console(
              "ERROR: streaming query appears to have been stuck for %d minutes, cancelling job.",
              quietFor.getStandardMinutes());
          errors.add(
              String.format(
                  "Cancelling streaming job since it appeared stuck for %d min.",
                  quietFor.getStandardMinutes()));
          waitingForShutdown = true;
          cancelJob = true;
        } else if (quietFor.isLongerThan(STUCK_WARNING_DELAY)) {
          NexmarkUtils.console(
              "WARNING: streaming query appears to have been stuck for %d min.",
              quietFor.getStandardMinutes());
        }

        if (cancelJob) {
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
        case UNRECOGNIZED:
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
          if (!cancelJob) {
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
      }
    }

    return perf;
  }

  // ================================================================================
  // Basic sources and sinks
  // ================================================================================

  /** Return a topic name. */
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
      case QUERY_RUNNER_AND_MODE:
        return String.format(
            "%s_%s_%s_%s_source",
            baseTopic, queryName, options.getRunner().getSimpleName(), options.isStreaming());
    }
    throw new RuntimeException("Unrecognized enum " + options.getResourceNameMode());
  }

  /** Return a subscription name. */
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
      case QUERY_RUNNER_AND_MODE:
        return String.format(
            "%s_%s_%s_%s_source",
            baseSubscription,
            queryName,
            options.getRunner().getSimpleName(),
            options.isStreaming());
    }
    throw new RuntimeException("Unrecognized enum " + options.getResourceNameMode());
  }

  /** Return a file name for plain text. */
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
      case QUERY_RUNNER_AND_MODE:
        return String.format(
            "%s/nexmark_%s_%s_%s",
            baseFilename, queryName, options.getRunner().getSimpleName(), options.isStreaming());
    }
    throw new RuntimeException("Unrecognized enum " + options.getResourceNameMode());
  }

  /** Return a directory for logs. */
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
      case QUERY_RUNNER_AND_MODE:
        return String.format(
            "%s/logs_%s_%s_%s",
            baseFilename, queryName, options.getRunner().getSimpleName(), options.isStreaming());
    }
    throw new RuntimeException("Unrecognized enum " + options.getResourceNameMode());
  }

  /** Return a source of synthetic events. */
  private PCollection<Event> sourceEventsFromSynthetic(Pipeline p) {
    if (isStreaming()) {
      NexmarkUtils.console("Generating %d events in streaming mode", configuration.numEvents);
      return p.apply(queryName + ".ReadUnbounded", NexmarkUtils.streamEventsSource(configuration));
    } else {
      NexmarkUtils.console("Generating %d events in batch mode", configuration.numEvents);
      return p.apply(queryName + ".ReadBounded", NexmarkUtils.batchEventsSource(configuration));
    }
  }

  /** Return source of events from Pubsub. */
  private PCollection<Event> sourceEventsFromPubsub(Pipeline p) {
    NexmarkUtils.console("Reading events from Pubsub %s", pubsubSubscription);

    PubsubIO.Read<PubsubMessage> io =
        PubsubIO.readMessagesWithAttributes()
            .fromSubscription(pubsubSubscription)
            .withIdAttribute(NexmarkUtils.PUBSUB_ID);
    if (!configuration.usePubsubPublishTime) {
      io = io.withTimestampAttribute(NexmarkUtils.PUBSUB_TIMESTAMP);
    }

    return p.apply(queryName + ".ReadPubsubEvents", io)
        .apply(queryName + ".PubsubMessageToEvent", ParDo.of(new PubsubMessageEventDoFn()));
  }

  static final DoFn<Event, byte[]> EVENT_TO_BYTEARRAY =
      new DoFn<Event, byte[]>() {
        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
          byte[] encodedEvent = CoderUtils.encodeToByteArray(Event.CODER, c.element());
          c.output(encodedEvent);
        }
      };

  /** Send {@code events} to Kafka. */
  private void sinkEventsToKafka(PCollection<Event> events) {
    checkArgument((options.getBootstrapServers() != null), "Missing --bootstrapServers");
    NexmarkUtils.console("Writing events to Kafka Topic %s", options.getKafkaTopic());

    PCollection<byte[]> eventToBytes = events.apply("Event to bytes", ParDo.of(EVENT_TO_BYTEARRAY));
    eventToBytes.apply(
        KafkaIO.<Void, byte[]>write()
            .withBootstrapServers(options.getBootstrapServers())
            .withTopic(options.getKafkaTopic())
            .withValueSerializer(ByteArraySerializer.class)
            .values());
  }

  static final DoFn<KV<Long, byte[]>, Event> BYTEARRAY_TO_EVENT =
      new DoFn<KV<Long, byte[]>, Event>() {
        @ProcessElement
        public void processElement(ProcessContext c) throws IOException {
          byte[] encodedEvent = c.element().getValue();
          Event event = CoderUtils.decodeFromByteArray(Event.CODER, encodedEvent);
          c.output(event);
        }
      };

  /** Return source of events from Kafka. */
  private PCollection<Event> sourceEventsFromKafka(Pipeline p, final Instant now) {
    checkArgument((options.getBootstrapServers() != null), "Missing --bootstrapServers");
    NexmarkUtils.console("Reading events from Kafka Topic %s", options.getKafkaTopic());

    KafkaIO.Read<Long, byte[]> read =
        KafkaIO.<Long, byte[]>read()
            .withBootstrapServers(options.getBootstrapServers())
            .withTopic(options.getKafkaTopic())
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(ByteArrayDeserializer.class)
            .withStartReadTime(now)
            .withMaxNumRecords(
                options.getNumEvents() != null ? options.getNumEvents() : Long.MAX_VALUE);

    return p.apply(queryName + ".ReadKafkaEvents", read.withoutMetadata())
        .apply(queryName + ".KafkaToEvents", ParDo.of(BYTEARRAY_TO_EVENT));
  }

  /** Return Avro source of events from {@code options.getInputFilePrefix}. */
  private PCollection<Event> sourceEventsFromAvro(Pipeline p) {
    String filename = options.getInputPath();
    if (Strings.isNullOrEmpty(filename)) {
      throw new RuntimeException("Missing --inputPath");
    }
    NexmarkUtils.console("Reading events from Avro files at %s", filename);
    return p.apply(
            queryName + ".ReadAvroEvents", AvroIO.read(Event.class).from(filename + "*.avro"))
        .apply("OutputWithTimestamp", NexmarkQueryUtil.EVENT_TIMESTAMP_FROM_DATA);
  }

  /** Send {@code events} to Pubsub. */
  private void sinkEventsToPubsub(PCollection<Event> events) {
    checkState(pubsubTopic != null, "Pubsub topic needs to be set up before initializing sink");
    NexmarkUtils.console("Writing events to Pubsub %s", pubsubTopic);

    PubsubIO.Write<PubsubMessage> io =
        PubsubIO.writeMessages().to(pubsubTopic).withIdAttribute(NexmarkUtils.PUBSUB_ID);
    if (!configuration.usePubsubPublishTime) {
      io = io.withTimestampAttribute(NexmarkUtils.PUBSUB_TIMESTAMP);
    }

    events
        .apply(
            queryName + ".EventToPubsubMessage",
            ParDo.of(new EventPubsubMessageDoFn(configuration.pubsubMessageSerializationMethod)))
        .apply(queryName + ".WritePubsubEvents", io);
  }

  /** Send {@code events} to file with prefix {@code generateInputFileOnlyPrefix}. */
  private void sinkEventsToFile(PCollection<Event> events) {
    events
        .apply(MapElements.into(TypeDescriptors.strings()).via(Event::toString))
        .apply(
            "writeToFile",
            TextIO.write()
                .to(configuration.generateInputFileOnlyPrefix)
                .withSuffix(".json")
                .withNumShards(1));
  }

  /** Send {@code formattedResults} to Kafka. */
  private void sinkResultsToKafka(PCollection<String> formattedResults) {
    checkArgument((options.getBootstrapServers() != null), "Missing --bootstrapServers");
    NexmarkUtils.console("Writing results to Kafka Topic %s", options.getKafkaResultsTopic());

    formattedResults.apply(
        queryName + ".WriteKafkaResults",
        KafkaIO.<Void, String>write()
            .withBootstrapServers(options.getBootstrapServers())
            .withTopic(options.getKafkaResultsTopic())
            .withValueSerializer(StringSerializer.class)
            .values());
  }

  /** Send {@code formattedResults} to Pubsub. */
  private void sinkResultsToPubsub(PCollection<String> formattedResults, long now) {
    String shortTopic = shortTopic(now);
    NexmarkUtils.console("Writing results to Pubsub %s", shortTopic);
    PubsubIO.Write<String> io =
        PubsubIO.writeStrings().to(shortTopic).withIdAttribute(NexmarkUtils.PUBSUB_ID);
    if (!configuration.usePubsubPublishTime) {
      io = io.withTimestampAttribute(NexmarkUtils.PUBSUB_TIMESTAMP);
    }
    formattedResults.apply(queryName + ".WritePubsubResults", io);
  }

  /**
   * Sink all raw Events in {@code source} to {@code options.getOutputPath}. This will configure the
   * job to write the following files:
   *
   * <ul>
   *   <li>{@code $outputPath/event*.avro} All Event entities.
   *   <li>{@code $outputPath/auction*.avro} Auction entities.
   *   <li>{@code $outputPath/bid*.avro} Bid entities.
   *   <li>{@code $outputPath/person*.avro} Person entities.
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
    source.apply(
        queryName + ".WriteAvroEvents",
        AvroIO.write(Event.class).to(filename + "/event").withSuffix(".avro"));
    source
        .apply(NexmarkQueryUtil.JUST_BIDS)
        .apply(
            queryName + ".WriteAvroBids",
            AvroIO.write(Bid.class).to(filename + "/bid").withSuffix(".avro"));
    source
        .apply(NexmarkQueryUtil.JUST_NEW_AUCTIONS)
        .apply(
            queryName + ".WriteAvroAuctions",
            AvroIO.write(Auction.class).to(filename + "/auction").withSuffix(".avro"));
    source
        .apply(NexmarkQueryUtil.JUST_NEW_PERSONS)
        .apply(
            queryName + ".WriteAvroPeople",
            AvroIO.write(Person.class).to(filename + "/person").withSuffix(".avro"));
  }

  /** Send {@code formattedResults} to text files. */
  private void sinkResultsToText(PCollection<String> formattedResults, long now) {
    String filename = textFilename(now);
    NexmarkUtils.console("Writing results to text files at %s", filename);
    formattedResults.apply(queryName + ".WriteTextResults", TextIO.write().to(filename));
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

  /** Send {@code formattedResults} to BigQuery. */
  private void sinkResultsToBigQuery(
      PCollection<String> formattedResults, long now, String version) {
    String tableSpec = NexmarkUtils.tableSpec(options, queryName, now, version);
    TableSchema tableSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("result").setType("STRING"),
                    new TableFieldSchema()
                        .setName("records")
                        .setMode("REPEATED")
                        .setType("RECORD")
                        .setFields(
                            ImmutableList.of(
                                new TableFieldSchema().setName("index").setType("INTEGER"),
                                new TableFieldSchema().setName("value").setType("STRING")))));
    NexmarkUtils.console("Writing results to BigQuery table %s", tableSpec);
    BigQueryIO.Write io =
        BigQueryIO.write()
            .to(tableSpec)
            .withSchema(tableSchema)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND);
    formattedResults
        .apply(queryName + ".StringToTableRow", ParDo.of(new StringToTableRow()))
        .apply(queryName + ".WriteBigQueryResults", io);
  }

  /** Creates or reuses PubSub topics and subscriptions as configured. */
  private void setupPubSubResources(long now) throws IOException {
    String shortTopic = shortTopic(now);
    String shortSubscription = shortSubscription(now);

    if (!options.getManageResources() || configuration.pubSubMode == PubSubMode.SUBSCRIBE_ONLY) {
      // The topic should already have been created by the user or
      // a companion 'PUBLISH_ONLY' process.
      pubsubTopic = pubsubHelper.reuseTopic(shortTopic).getPath();
    } else {
      // Create a fresh topic. It will be removed when the job is done.
      pubsubTopic = pubsubHelper.createTopic(shortTopic).getPath();
    }

    // Create/confirm the subscription.
    if (configuration.pubSubMode == PubSubMode.PUBLISH_ONLY) {
      // Nothing to consume.
    } else if (options.getManageResources()) {
      pubsubSubscription = pubsubHelper.createSubscription(shortTopic, shortSubscription).getPath();
    } else {
      // The subscription should already have been created by the user.
      pubsubSubscription = pubsubHelper.reuseSubscription(shortTopic, shortSubscription).getPath();
    }
  }

  // ================================================================================
  // Construct overall pipeline
  // ================================================================================

  /** Return source of events for this run, or null if we are simply publishing events to Pubsub. */
  private PCollection<Event> createSource(Pipeline p, final Instant now) throws IOException {
    PCollection<Event> source = null;

    switch (configuration.sourceType) {
      case DIRECT:
        source = sourceEventsFromSynthetic(p);
        if (configuration.generateInputFileOnlyPrefix != null) {
          PCollection<Event> events = source;
          source = null;
          sinkEventsToFile(events);
        }
        break;
      case AVRO:
        source = sourceEventsFromAvro(p);
        break;
      case KAFKA:
      case PUBSUB:
        if (configuration.sourceType == SourceType.PUBSUB) {
          setupPubSubResources(now.getMillis());
        }
        // Setup the sink for the publisher.
        switch (configuration.pubSubMode) {
          case SUBSCRIBE_ONLY:
            // Nothing to publish.
            break;
          case PUBLISH_ONLY:
            {
              // Send synthesized events to Kafka or Pubsub in this job.
              PCollection<Event> events =
                  sourceEventsFromSynthetic(p)
                      .apply(queryName + ".Snoop", NexmarkUtils.snoop(queryName));
              if (configuration.sourceType == NexmarkUtils.SourceType.KAFKA) {
                sinkEventsToKafka(events);
              } else { // pubsub
                sinkEventsToPubsub(events);
              }
            }
            break;
          case COMBINED:
            // Send synthesized events to Kafka or Pubsub in separate publisher job.
            // We won't start the main pipeline until the publisher has sent the pre-load events.
            // We'll shutdown the publisher job when we notice the main job has finished.
            invokeBuilderForPublishOnlyPipeline(
                publishOnlyOptions -> {
                  Pipeline sp = Pipeline.create(publishOnlyOptions);
                  NexmarkUtils.setupPipeline(configuration.coderStrategy, sp);
                  publisherMonitor = new Monitor<>(queryName, "publisher");
                  PCollection<Event> events =
                      sourceEventsFromSynthetic(sp)
                          .apply(queryName + ".Monitor", publisherMonitor.getTransform());
                  if (configuration.sourceType == NexmarkUtils.SourceType.KAFKA) {
                    sinkEventsToKafka(events);
                  } else { // pubsub
                    sinkEventsToPubsub(events);
                  }
                  publisherResult = sp.run();
                  NexmarkUtils.console("Publisher job is started.");
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
            {
              // Read events from Kafka or Pubsub.
              if (configuration.sourceType == NexmarkUtils.SourceType.KAFKA) {
                // We need to have the same indexes for Publisher (sink) and Subscriber (source)
                // pipelines in COMBINED mode (when we run them in sequence). It means that
                // Subscriber should start reading from the same index as Publisher started to write
                // pre-load events even if we run Subscriber right after Publisher has been
                // finished. In other case. when pubSubMode=SUBSCRIBE_ONLY, now should be null and
                // it will be ignored.
                source =
                    sourceEventsFromKafka(p, configuration.pubSubMode == COMBINED ? now : null);
              } else {
                source = sourceEventsFromPubsub(p);
              }
            }
            break;
        }
        break;
    }
    return source;
  }

  private static final TupleTag<String> MAIN = new TupleTag<String>() {};
  private static final TupleTag<String> SIDE = new TupleTag<String>() {};

  private static class PartitionDoFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.element().hashCode() % 2 == 0) {
        c.output(c.element());
      } else {
        c.output(SIDE, c.element());
      }
    }
  }

  /** Consume {@code results}. */
  private void sink(PCollection<TimestampedValue<KnownSize>> results, long now) {
    if (configuration.sinkType == NexmarkUtils.SinkType.COUNT_ONLY) {
      // Avoid the cost of formatting the results.
      results.apply(queryName + ".DevNull", NexmarkUtils.devNull(queryName));
      return;
    }

    PCollection<String> formattedResults =
        results.apply(queryName + ".Format", NexmarkUtils.format(queryName));
    if (options.getLogResults()) {
      formattedResults =
          formattedResults.apply(
              queryName + ".Results.Log", NexmarkUtils.log(queryName + ".Results"));
    }

    switch (configuration.sinkType) {
      case DEVNULL:
        // Discard all results
        formattedResults.apply(queryName + ".DevNull", NexmarkUtils.devNull(queryName));
        break;
      case PUBSUB:
        sinkResultsToPubsub(formattedResults, now);
        break;
      case KAFKA:
        sinkResultsToKafka(formattedResults);
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
        PCollectionTuple res =
            formattedResults.apply(
                queryName + ".Partition",
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
      NexmarkUtils.console("Query%s: only %d samples", model.configuration.query, n);
    } else {
      NexmarkUtils.console(
          "Query%d: N:%d; min:%d; 1st%%:%d; mean:%d; 3rd%%:%d; max:%d",
          model.configuration.query,
          n,
          counts.get(0),
          counts.get(n / 4),
          counts.get(n / 2),
          counts.get(n - 1 - n / 4),
          counts.get(n - 1));
    }
  }

  /** Run {@code configuration} and return its performance if possible. */
  public @Nullable NexmarkPerf run() throws IOException {
    if (options.getManageResources() && !options.getMonitorJobs()) {
      throw new RuntimeException("If using --manageResources then must also use --monitorJobs.");
    }

    //
    // Setup per-run state.
    //
    checkState(queryName == null);
    if (configuration.sourceType.equals(SourceType.PUBSUB)) {
      pubsubHelper = PubsubHelper.create(options);
    }

    try {
      NexmarkUtils.console("Running %s", configuration.toShortString());

      if (configuration.numEvents < 0) {
        NexmarkUtils.console("skipping since configuration is disabled");
        return null;
      }

      NexmarkQuery<? extends KnownSize> query = getNexmarkQuery();
      if (query == null) {
        NexmarkUtils.console("skipping since configuration is not implemented");
        return null;
      }

      queryName = query.getName();

      // Append queryName to temp location
      if (!"".equals(options.getTempLocation())) {
        options.setTempLocation(options.getTempLocation() + "/" + queryName);
      }

      NexmarkQueryModel model = getNexmarkQueryModel();

      if (options.getJustModelResultRate()) {
        if (model == null) {
          throw new RuntimeException(String.format("No model for %s", queryName));
        }
        modelResultRates(model);
        return null;
      }

      final Instant now = Instant.now();
      Pipeline p = Pipeline.create(options);
      NexmarkUtils.setupPipeline(configuration.coderStrategy, p);

      // Generate events.
      PCollection<Event> source = createSource(p, now);

      if (query.getTransform().needsSideInput()) {
        query.getTransform().setSideInput(NexmarkUtils.prepareSideInput(p, configuration));
      }

      if (options.getLogEvents()) {
        source = source.apply(queryName + ".Events.Log", NexmarkUtils.log(queryName + ".Events"));
      }

      // Source will be null if source type is PUBSUB and mode is PUBLISH_ONLY.
      // In that case there's nothing more to add to pipeline.
      if (source != null) {
        // Optionally sink events in Avro format.
        // (Query results are ignored).
        if (configuration.sinkType == NexmarkUtils.SinkType.AVRO) {
          sinkEventsToAvro(source);
        }

        // Query 10 logs all events to Google Cloud storage files. It could generate a lot of logs,
        // so, set parallelism. Also set the output path where to write log files.
        if (configuration.query == NexmarkQueryName.LOG_TO_SHARDED_FILES) {
          String path = null;
          if (options.getOutputPath() != null && !options.getOutputPath().isEmpty()) {
            path = logsDir(now.getMillis());
          }
          ((Query10) query.getTransform()).setOutputPath(path);
          ((Query10) query.getTransform()).setMaxNumWorkers(maxNumWorkers());
        }

        // Apply query.
        PCollection<TimestampedValue<KnownSize>> results =
            (PCollection<TimestampedValue<KnownSize>>) source.apply(query);

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
        sink(results, now.getMillis());
      }

      mainResult = p.run();
      mainResult.waitUntilFinish(Duration.standardSeconds(configuration.streamTimeout));
      return monitor(query);
    } finally {
      if (pubsubHelper != null) {
        pubsubHelper.cleanup();
        pubsubHelper = null;
      }
      configuration = null;
      queryName = null;
    }
  }

  private boolean isSql() {
    return SQL.equalsIgnoreCase(options.getQueryLanguage());
  }

  private boolean isZetaSql() {
    return ZETA_SQL.equalsIgnoreCase(options.getQueryLanguage());
  }

  private NexmarkQueryModel getNexmarkQueryModel() {
    return models.get(configuration.query);
  }

  private NexmarkQuery<?> getNexmarkQuery() {
    return queries.get(configuration.query);
  }

  private Map<NexmarkQueryName, NexmarkQueryModel> createQueryModels() {
    return (isSql() || isZetaSql()) ? createSqlQueryModels() : createJavaQueryModels();
  }

  private Map<NexmarkQueryName, NexmarkQueryModel> createSqlQueryModels() {
    return ImmutableMap.of();
  }

  private Map<NexmarkQueryName, NexmarkQueryModel> createJavaQueryModels() {
    return ImmutableMap.<NexmarkQueryName, NexmarkQueryModel>builder()
        .put(NexmarkQueryName.PASSTHROUGH, new Query0Model(configuration))
        .put(NexmarkQueryName.CURRENCY_CONVERSION, new Query1Model(configuration))
        .put(NexmarkQueryName.SELECTION, new Query2Model(configuration))
        .put(NexmarkQueryName.LOCAL_ITEM_SUGGESTION, new Query3Model(configuration))
        .put(NexmarkQueryName.AVERAGE_PRICE_FOR_CATEGORY, new Query4Model(configuration))
        .put(NexmarkQueryName.HOT_ITEMS, new Query5Model(configuration))
        .put(NexmarkQueryName.AVERAGE_SELLING_PRICE_BY_SELLER, new Query6Model(configuration))
        .put(NexmarkQueryName.HIGHEST_BID, new Query7Model(configuration))
        .put(NexmarkQueryName.MONITOR_NEW_USERS, new Query8Model(configuration))
        .put(NexmarkQueryName.WINNING_BIDS, new Query9Model(configuration))
        .put(NexmarkQueryName.BOUNDED_SIDE_INPUT_JOIN, new BoundedSideInputJoinModel(configuration))
        .put(NexmarkQueryName.SESSION_SIDE_INPUT_JOIN, new SessionSideInputJoinModel(configuration))
        .build();
  }

  private Map<NexmarkQueryName, NexmarkQuery> createQueries() {
    Map<NexmarkQueryName, NexmarkQuery> defaultQueries;
    if (isSql()) {
      defaultQueries = createSqlQueries();
    } else if (isZetaSql()) {
      defaultQueries = createZetaSqlQueries();
    } else {
      defaultQueries = createJavaQueries();
    }

    Set<NexmarkQueryName> skippableQueries = getSkippableQueries();
    return ImmutableMap.copyOf(
        Maps.filterKeys(defaultQueries, query -> !skippableQueries.contains(query)));
  }

  private Set<NexmarkQueryName> getSkippableQueries() {
    Set<NexmarkQueryName> skipQueries = new LinkedHashSet<>();
    if (options.getSkipQueries() != null && !options.getSkipQueries().trim().equals("")) {
      Iterable<String> queries = Splitter.on(',').split(options.getSkipQueries());
      for (String query : queries) {
        skipQueries.add(NexmarkQueryName.fromId(query.trim()));
      }
    }
    return skipQueries;
  }

  private Map<NexmarkQueryName, NexmarkQuery> createSqlQueries() {
    return ImmutableMap.<NexmarkQueryName, NexmarkQuery>builder()
        .put(
            NexmarkQueryName.PASSTHROUGH,
            new NexmarkQuery(configuration, SqlQuery0.calciteSqlQuery0()))
        .put(NexmarkQueryName.CURRENCY_CONVERSION, new NexmarkQuery(configuration, new SqlQuery1()))
        .put(
            NexmarkQueryName.SELECTION,
            new NexmarkQuery(configuration, SqlQuery2.calciteSqlQuery2(configuration.auctionSkip)))
        .put(
            NexmarkQueryName.LOCAL_ITEM_SUGGESTION,
            new NexmarkQuery(configuration, SqlQuery3.calciteSqlQuery3(configuration)))

        // SqlQuery5 is disabled for now, uses non-equi-joins,
        // never worked right, was giving incorrect results.
        // Gets rejected after PR/8301, causing failures.
        //
        // See:
        //   https://issues.apache.org/jira/browse/BEAM-7072
        //   https://github.com/apache/beam/pull/8301
        //   https://github.com/apache/beam/pull/8422#issuecomment-487676350
        //
        //        .put(
        //            NexmarkQueryName.HOT_ITEMS,
        //            new NexmarkQuery(configuration, new SqlQuery5(configuration)))
        .put(
            NexmarkQueryName.HIGHEST_BID,
            new NexmarkQuery(configuration, new SqlQuery7(configuration)))
        .put(
            NexmarkQueryName.BOUNDED_SIDE_INPUT_JOIN,
            new NexmarkQuery(
                configuration,
                SqlBoundedSideInputJoin.calciteSqlBoundedSideInputJoin(configuration)))
        .build();
  }

  private Map<NexmarkQueryName, NexmarkQuery> createZetaSqlQueries() {
    return ImmutableMap.<NexmarkQueryName, NexmarkQuery>builder()
        .put(
            NexmarkQueryName.PASSTHROUGH,
            new NexmarkQuery(configuration, SqlQuery0.zetaSqlQuery0()))
        .put(
            NexmarkQueryName.SELECTION,
            new NexmarkQuery(configuration, SqlQuery2.zetaSqlQuery2(configuration.auctionSkip)))
        .put(
            NexmarkQueryName.LOCAL_ITEM_SUGGESTION,
            new NexmarkQuery(configuration, SqlQuery3.zetaSqlQuery3(configuration)))
        .put(
            NexmarkQueryName.BOUNDED_SIDE_INPUT_JOIN,
            new NexmarkQuery(
                configuration, SqlBoundedSideInputJoin.zetaSqlBoundedSideInputJoin(configuration)))
        .build();
  }

  private Map<NexmarkQueryName, NexmarkQuery> createJavaQueries() {
    return ImmutableMap.<NexmarkQueryName, NexmarkQuery>builder()
        .put(NexmarkQueryName.PASSTHROUGH, new NexmarkQuery(configuration, new Query0()))
        .put(
            NexmarkQueryName.CURRENCY_CONVERSION,
            new NexmarkQuery(configuration, new Query1(configuration)))
        .put(NexmarkQueryName.SELECTION, new NexmarkQuery(configuration, new Query2(configuration)))
        .put(
            NexmarkQueryName.LOCAL_ITEM_SUGGESTION,
            new NexmarkQuery(configuration, new Query3(configuration)))
        .put(
            NexmarkQueryName.AVERAGE_PRICE_FOR_CATEGORY,
            new NexmarkQuery(configuration, new Query4(configuration)))
        .put(NexmarkQueryName.HOT_ITEMS, new NexmarkQuery(configuration, new Query5(configuration)))
        .put(
            NexmarkQueryName.AVERAGE_SELLING_PRICE_BY_SELLER,
            new NexmarkQuery(configuration, new Query6(configuration)))
        .put(
            NexmarkQueryName.HIGHEST_BID,
            new NexmarkQuery(configuration, new Query7(configuration)))
        .put(
            NexmarkQueryName.MONITOR_NEW_USERS,
            new NexmarkQuery(configuration, new Query8(configuration)))
        .put(
            NexmarkQueryName.WINNING_BIDS,
            new NexmarkQuery(configuration, new Query9(configuration)))
        .put(
            NexmarkQueryName.LOG_TO_SHARDED_FILES,
            new NexmarkQuery(configuration, new Query10(configuration)))
        .put(
            NexmarkQueryName.USER_SESSIONS,
            new NexmarkQuery(configuration, new Query11(configuration)))
        .put(
            NexmarkQueryName.PROCESSING_TIME_WINDOWS,
            new NexmarkQuery(configuration, new Query12(configuration)))
        .put(
            NexmarkQueryName.BOUNDED_SIDE_INPUT_JOIN,
            new NexmarkQuery(configuration, new BoundedSideInputJoin(configuration)))
        .put(
            NexmarkQueryName.SESSION_SIDE_INPUT_JOIN,
            new NexmarkQuery(configuration, new SessionSideInputJoin(configuration)))
        .build();
  }

  private static class PubsubMessageEventDoFn extends DoFn<PubsubMessage, Event> {
    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      byte[] payload = c.element().getPayload();
      Event event = CoderUtils.decodeFromByteArray(Event.CODER, payload);
      c.output(event);
    }
  }

  private static class EventPubsubMessageDoFn extends DoFn<Event, PubsubMessage> {
    private final PubsubMessageSerializationMethod serializationMethod;

    public EventPubsubMessageDoFn(PubsubMessageSerializationMethod serializationMethod) {
      this.serializationMethod = serializationMethod;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      byte[] payload;
      switch (serializationMethod) {
        case CODER:
          payload = CoderUtils.encodeToByteArray(Event.CODER, c.element());
          break;
        case TO_STRING:
          payload = c.element().toString().getBytes(StandardCharsets.UTF_8);
          break;
        default:
          throw new IllegalArgumentException("Unsupported serialization method used.");
      }
      c.output(new PubsubMessage(payload, Collections.emptyMap()));
    }
  }
}
