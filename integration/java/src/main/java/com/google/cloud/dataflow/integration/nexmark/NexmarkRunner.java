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

package com.google.cloud.dataflow.integration.nexmark;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

/**
 * Run a single Nexmark query using a given configuration.
 */
public abstract class NexmarkRunner<OptionT extends Options> {
  /**
   * Options shared by all runs.
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
   * Return true if runner is blocking.
   */
  protected abstract boolean isBlocking();

  /**
   * Return true if runner can monitor running jobs.
   */
  protected abstract boolean canMonitor();

  /**
   * Build and run a pipeline using specified options.
   */
  protected interface PipelineBuilder<OptionT extends Options> {
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
   * If monitoring, print stats on the main pipeline and return the final perf
   * when it has run long enough. Otherwise, return null immediately.
   */
  protected abstract NexmarkPerf monitor(NexmarkQuery query);

  // ================================================================================
  // Basic sources and sinks
  // ================================================================================

  /**
   * Return a source of synthetic events.
   */
  private PCollection<Event> sourceFromSynthetic(Pipeline p) {
    if (isStreaming()) {
      NexmarkUtils.console("Generating %d events in streaming mode", configuration.numEvents);
      return p.apply(NexmarkUtils.streamEventsSource(queryName, configuration));
    } else {
      NexmarkUtils.console("Generating %d events in batch mode", configuration.numEvents);
      return p.apply(NexmarkUtils.batchEventsSource(queryName, configuration));
    }
  }

  /**
   * Return source of events from Pubsub.
   */
  private PCollection<Event> sourceEventsFromPubsub(Pipeline p, long now) {
    String baseTopic = options.getPubsubTopic();
    if (baseTopic == null || baseTopic.isEmpty()) {
      throw new RuntimeException("Missing --pubsubTopic");
    }
    String shortTopic;
    if (options.getUniqify()) {
      // Salt the topic name so we can run multiple jobs in parallel.
      shortTopic = String.format("%s_%d_%s_source", baseTopic, now, queryName);
    } else {
      shortTopic = String.format("%s_%s_source", baseTopic, queryName);
    }
    String shortSubscription = shortTopic;
    // Create/confirm the subscription.
    String subscription = null;
    if (!options.getManageResources()) {
      // The subscription should already have been created by the user.
      subscription = getPubsub().reuseSubscription(shortTopic, shortSubscription);
    } else {
      subscription = getPubsub().createSubscription(shortTopic, shortSubscription);
    }
    NexmarkUtils.console("Reading events from Pubsub %s", subscription);
    PubsubIO.Read.Bound<Event> io =
        PubsubIO.Read.named(queryName + ".ReadPubsubEvents(" + subscription.replace('/', '.') + ")")
                     .subscription(subscription)
                     .idLabel(NexmarkUtils.PUBSUB_ID)
                     .withCoder(Event.CODER);
    if (!configuration.usePubsubPublishTime) {
      io = io.timestampLabel(NexmarkUtils.PUBSUB_TIMESTAMP);
    }
    return p.apply(io);
  }

  /**
   * Return Avro source of events from {@code options.getInputFilePrefix}.
   */
  private PCollection<Event> sourceFromAvro(Pipeline p) {
    String filename = options.getInputFilePrefix();
    if (filename == null || filename.isEmpty()) {
      throw new RuntimeException("Missing --inputFilePrefix");
    }
    NexmarkUtils.console("Reading events from Avro files at %s", filename);
    return p
        .apply(AvroIO.Read.named(queryName + ".ReadAvroEvents(" + filename.replace('/', '.') + ")")
                          .from(filename + "*.avro")
                          .withSchema(Event.class))
        .apply(NexmarkQuery.EVENT_TIMESTAMP_FROM_DATA);
  }

  /**
   * Send {@code events} to Pubsub.
   */
  private void sinkEventsToPubsub(PCollection<Event> events, long now) {
    String baseTopic = options.getPubsubTopic();
    if (baseTopic == null || baseTopic.isEmpty()) {
      throw new RuntimeException("Missing --pubsubTopic");
    }
    String shortTopic;
    if (options.getUniqify()) {
      // Salt the topic name so we can run multiple jobs in parallel.
      shortTopic = String.format("%s_%d_%s_source", baseTopic, now, queryName);
    } else {
      shortTopic = String.format("%s_%s_source", baseTopic, queryName);
    }
    // Create/confirm the topic.
    String topic;
    if (!options.getManageResources()
        || configuration.pubSubMode == NexmarkUtils.PubSubMode.SUBSCRIBE_ONLY) {
      // The topic should already have been created by the user or
      // a companion 'PUBLISH_ONLY' process.
      topic = getPubsub().reuseTopic(shortTopic);
    } else {
      // Create a fresh topic to loopback via. It will be destroyed when the
      // (necessarily blocking) job is done.
      topic = getPubsub().createTopic(shortTopic);
    }
    NexmarkUtils.console("Writing events to Pubsub %s", topic);
    PubsubIO.Write.Bound<Event> io =
        PubsubIO.Write.named(queryName + ".WritePubsubEvents(" + topic.replace('/', '.') + ")")
                      .topic(topic)
                      .idLabel(NexmarkUtils.PUBSUB_ID)
                      .withCoder(Event.CODER);
    if (!configuration.usePubsubPublishTime) {
      io = io.timestampLabel(NexmarkUtils.PUBSUB_TIMESTAMP);
    }
    events.apply(io);
  }

  /**
   * Send {@code formattedResults} to Pubsub.
   */
  private void sinkResultsToPubsub(PCollection<String> formattedResults, long now) {
    String baseTopic = options.getPubsubTopic();
    if (baseTopic == null || baseTopic.isEmpty()) {
      throw new RuntimeException("Missing --pubsubTopic");
    }
    String shortTopic;
    if (options.getUniqify()) {
      shortTopic = String.format("%s_%d_%s_sink", baseTopic, now, queryName);
    } else {
      shortTopic = String.format("%s_%s_sink", baseTopic, queryName);
    }
    String topic;
    if (!options.getManageResources()) {
      topic = getPubsub().reuseTopic(shortTopic);
    } else {
      topic = getPubsub().createTopic(shortTopic);
    }
    NexmarkUtils.console("Writing results to Pubsub %s", topic);
    PubsubIO.Write.Bound<String> io =
        PubsubIO.Write.named(queryName + ".WritePubsubResults(" + topic.replace('/', '.') + ")")
                      .topic(topic)
                      .idLabel(NexmarkUtils.PUBSUB_ID);
    if (!configuration.usePubsubPublishTime) {
      io = io.timestampLabel(NexmarkUtils.PUBSUB_TIMESTAMP);
    }
    formattedResults.apply(io);
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
  private void sinkToAvro(PCollection<Event> source) {
    String filename = options.getOutputPath();
    if (filename == null || filename.isEmpty()) {
      throw new RuntimeException("Missing --outputPath");
    }
    NexmarkUtils.console("Writing events to Avro files at %s", filename);
    source.apply(AvroIO.Write.named(queryName + ".WriteAvroEvents(" + filename.replace('/', '.')
                                    + ")")
                             .to(filename + "/event")
                             .withSuffix(".avro")
                             .withSchema(Event.class));
    source.apply(NexmarkQuery.JUST_BIDS)
          .apply(
              AvroIO.Write.named(queryName + ".WriteAvroBids(" + filename.replace('/', '.') + ")")
                          .to(filename + "/bid")
                          .withSuffix(".avro")
                          .withSchema(Bid.class));
    source.apply(NexmarkQuery.JUST_NEW_AUCTIONS)
          .apply(AvroIO.Write.named(
              queryName + ".WriteAvroAuctions(" + filename.replace('/', '.') + ")")
                             .to(filename + "/auction")
                             .withSuffix(".avro")
                             .withSchema(Auction.class));
    source.apply(NexmarkQuery.JUST_NEW_PERSONS)
          .apply(
              AvroIO.Write.named(queryName + ".WriteAvroPeople(" + filename.replace('/', '.') + ")")
                          .to(filename + "/person")
                          .withSuffix(".avro")
                          .withSchema(Person.class));
  }

  private static class StringToTableRow extends DoFn<String, TableRow> {
    @Override
    public void processElement(ProcessContext c) {
      TableRow row = new TableRow();
      row.set("event", c.element());
      c.output(row);
    }
  }

  /**
   * Send {@code formattedResults} to text files.
   */
  private void sinkToText(PCollection<String> formattedResults, long now) {
    String filename = options.getOutputPath();
    if (filename == null || filename.isEmpty()) {
      throw new RuntimeException("Missing --outputPath");
    }
    String fullFilename;
    if (options.getUniqify()) {
      fullFilename = String.format("%s/nexmark_%d_%s.txt", filename, now, queryName);
    } else {
      fullFilename = String.format("%s/nexmark_%s.txt", filename, queryName);
    }
    NexmarkUtils.console("Writing results to text files at %s", fullFilename);
    formattedResults.apply(
        TextIO.Write.named(queryName + ".WriteTextResults(" + fullFilename.replace('/', '.') + ")")
                    .to(fullFilename));
  }

  /**
   * Send {@code formattedResults} to BigQuery.
   */
  private void sinkToBigQuery(PCollection<String> formattedResults, long now) {
    String tableName;
    if (options.getUniqify()) {
      tableName = String.format("%s:nexmark.table_%d", options.getProject(), now);
    } else {
      tableName = String.format("%s:nexmark.table", options.getProject());
    }
    TableSchema schema =
        new TableSchema()
            .setFields(ImmutableList.of(new TableFieldSchema().setName("result")
                                                              .setType("STRING")));
    NexmarkUtils.console("Writing results to BigQuery table %s", tableName);
    BigQueryIO.Write.Bound io =
        BigQueryIO.Write.named(
            queryName + ".WriteBigQueryResults(" + tableName.replace('/', '.') + ")")
                        .to(tableName)
                        .withSchema(schema);
    formattedResults
        .apply(ParDo.of(new StringToTableRow()))
        .apply(io);
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
        source = sourceFromSynthetic(p);
        break;
      case AVRO:
        source = sourceFromAvro(p);
        break;
      case PUBSUB:
        // Check some flags.
        switch (configuration.pubSubMode) {
          case SUBSCRIBE_ONLY:
            break;
          case PUBLISH_ONLY:
            if (options.getManageResources() && !isBlocking()) {
              throw new RuntimeException(
                  "If --manageResources=true and --pubSubMode=PUBLISH_ONLY then "
                  + "runner must be blocking so that this program "
                  + "can cleanup the Pubsub topic on exit.");
            }
            break;
          case COMBINED:
            if (!canMonitor() || !options.getMonitorJobs()) {
              throw new RuntimeException(
                  "if --pubSubMode=COMBINED then you must use a monitoring runner "
                  + "and set --monitorJobs=true so that the two pipelines can be coordinated.");
            }
            break;
        }

        // Setup the sink for the publisher.
        switch (configuration.pubSubMode) {
          case SUBSCRIBE_ONLY:
            // Nothing to publish.
            break;
          case PUBLISH_ONLY:
            // Send synthesized events to Pubsub in this job.
            sinkEventsToPubsub(sourceFromSynthetic(p).apply(NexmarkUtils.snoop(queryName)), now);
            break;
          case COMBINED:
            // Send synthesized events to Pubsub in separate publisher job.
            // We won't start the main pipeline until the publisher has sent the pre-load events.
            // We'll shutdown the publisher job when we notice the main job has finished.
            invokeBuilderForPublishOnlyPipeline(new PipelineBuilder() {
              @Override
              public void build(Options publishOnlyOptions) {
                Pipeline sp = Pipeline.create(options);
                NexmarkUtils.setupPipeline(configuration.coderStrategy, sp);
                publisherMonitor = new Monitor<Event>(queryName, "publisher");
                sinkEventsToPubsub(sourceFromSynthetic(sp).apply(publisherMonitor.getTransform()),
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

  /**
   * Consume {@code results}.
   */
  private void sink(PCollection<TimestampedValue<KnownSize>> results, long now) {
    if (configuration.sinkType == NexmarkUtils.SinkType.COUNT_ONLY) {
      // Avoid the cost of formatting the results.
      results.apply(NexmarkUtils.devNull(queryName));
      return;
    }

    PCollection<String> formattedResults = results.apply(NexmarkUtils.format(queryName));
    if (configuration.logResults) {
      formattedResults = formattedResults.apply(NexmarkUtils.<String>log(queryName));
    }

    switch (configuration.sinkType) {
      case DEVNULL:
        // Discard all results
        formattedResults.apply(NexmarkUtils.devNull(queryName));
        break;
      case PUBSUB:
        sinkResultsToPubsub(formattedResults, now);
        break;
      case TEXT:
        sinkToText(formattedResults, now);
        break;
      case AVRO:
        NexmarkUtils.console(
            "WARNING: with --sinkType=AVRO, actual query results will be discarded.");
        break;
      case BIGQUERY:
        sinkToBigQuery(formattedResults, now);
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
    //
    // Setup per-run state.
    //
    Preconditions.checkState(configuration == null);
    Preconditions.checkState(pubsub == null);
    Preconditions.checkState(queryName == null);
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

      if (configuration.justModelResultRate) {
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

      // Source will be null if source type is PUBSUB and mode is PUBLISH_ONLY.
      // In that case there's nothing more to add to pipeline.
      if (source != null) {
        if (options.getMonitorJobs() && !canMonitor()) {
          throw new RuntimeException("Cannot use --monitorJobs=true with this runner");
        }

        // Optionally sink events in Avro format.
        // (Query results are ignored).
        if (configuration.sinkType == NexmarkUtils.SinkType.AVRO) {
          sinkToAvro(source);
        }

        // Special hacks for Query 10 (big logger).
        if (configuration.query == 10) {
          String path = null;
          if (options.getOutputPath() != null && !options.getOutputPath().isEmpty()) {
            if (options.getUniqify()) {
              path = String.format("%s/%d_logs", options.getOutputPath(), now);
            } else {
              path = String.format("%s/logs", options.getOutputPath());
            }
          }
          ((Query10) query).setOutputPath(path);
          ((Query10) query).setMaxNumWorkers(maxNumWorkers());
          if (path != null && options.getManageResources()) {
            pathsToDelete.add(path + "/**");
          }
        }

        // Apply query.
        PCollection<TimestampedValue<KnownSize>> results = source.apply(query);

        if (configuration.assertCorrectness) {
          if (model == null) {
            throw new RuntimeException(String.format("No model for %s", queryName));
          }
          // We know all our streams have a finite number of elements.
          results.setIsBoundedInternal(PCollection.IsBounded.BOUNDED);
          // If we have a finite number of events then assert our pipeline's
          // results match those of a model using the same sequence of events.
          DataflowAssert.that(results).satisfies(model.assertionFor());
        }

        // Output results.
        sink(results, now);
      }

      if (publisherResult != null) {
        waitForPublisherPreload();
      }
      mainResult = p.run();
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
