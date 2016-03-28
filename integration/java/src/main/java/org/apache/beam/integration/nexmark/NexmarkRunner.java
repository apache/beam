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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.BigQueryIO;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
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
   * when it has run long enough. Otherwise, return {@literal null} immediately.
   */
  @Nullable
  protected abstract NexmarkPerf monitor(NexmarkQuery query);

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
    PubsubIO.Read.Bound<Event> io =
        PubsubIO.Read.named(queryName + ".ReadPubsubEvents")
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
  private PCollection<Event> sourceEventsFromAvro(Pipeline p) {
    String filename = options.getInputPath();
    if (Strings.isNullOrEmpty(filename)) {
      throw new RuntimeException("Missing --inputPath");
    }
    NexmarkUtils.console("Reading events from Avro files at %s", filename);
    return p
        .apply(AvroIO.Read.named(queryName + ".ReadAvroEvents")
                          .from(filename + "*.avro")
                          .withSchema(Event.class))
        .apply(NexmarkQuery.EVENT_TIMESTAMP_FROM_DATA);
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
    PubsubIO.Write.Bound<Event> io =
        PubsubIO.Write.named(queryName + ".WritePubsubEvents")
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
    String shortTopic = shortTopic(now);
    String topic;
    if (!options.getManageResources()) {
      topic = getPubsub().reuseTopic(shortTopic).getPath();
    } else {
      topic = getPubsub().createTopic(shortTopic).getPath();
    }
    NexmarkUtils.console("Writing results to Pubsub %s", topic);
    PubsubIO.Write.Bound<String> io =
        PubsubIO.Write.named(queryName + ".WritePubsubResults")
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
  private void sinkEventsToAvro(PCollection<Event> source) {
    String filename = options.getOutputPath();
    if (Strings.isNullOrEmpty(filename)) {
      throw new RuntimeException("Missing --outputPath");
    }
    NexmarkUtils.console("Writing events to Avro files at %s", filename);
    source.apply(AvroIO.Write.named(queryName + ".WriteAvroEvents")
                             .to(filename + "/event")
                             .withSuffix(".avro")
                             .withSchema(Event.class));
    source.apply(NexmarkQuery.JUST_BIDS)
          .apply(AvroIO.Write.named(queryName + ".WriteAvroBids")
                             .to(filename + "/bid")
                             .withSuffix(".avro")
                             .withSchema(Bid.class));
    source.apply(NexmarkQuery.JUST_NEW_AUCTIONS)
          .apply(AvroIO.Write.named(
              queryName + ".WriteAvroAuctions")
                             .to(filename + "/auction")
                             .withSuffix(".avro")
                             .withSchema(Auction.class));
    source.apply(NexmarkQuery.JUST_NEW_PERSONS)
          .apply(AvroIO.Write.named(queryName + ".WriteAvroPeople")
                             .to(filename + "/person")
                             .withSuffix(".avro")
                             .withSchema(Person.class));
  }

  /**
   * Send {@code formattedResults} to text files.
   */
  private void sinkResultsToText(PCollection<String> formattedResults, long now) {
    String filename = textFilename(now);
    NexmarkUtils.console("Writing results to text files at %s", filename);
    formattedResults.apply(
        TextIO.Write.named(queryName + ".WriteTextResults")
                    .to(filename));
  }

  private static class StringToTableRow extends DoFn<String, TableRow> {
    @Override
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
    BigQueryIO.Write.Bound io =
        BigQueryIO.Write.named(queryName + ".WriteBigQueryResults")
                        .to(tableSpec)
                        .withSchema(tableSchema)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND);
    formattedResults
        .apply(ParDo.named(queryName + ".StringToTableRow")
                    .of(new StringToTableRow()))
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
            sinkEventsToPubsub(sourceEventsFromSynthetic(p).apply(NexmarkUtils.snoop(queryName)),
                               now);
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
                sinkEventsToPubsub(
                    sourceEventsFromSynthetic(sp).apply(publisherMonitor.getTransform()),
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
    @Override
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
      results.apply(NexmarkUtils.devNull(queryName));
      return;
    }

    PCollection<String> formattedResults = results.apply(NexmarkUtils.format(queryName));
    if (options.getLogResults()) {
      formattedResults = formattedResults.apply(NexmarkUtils.<String>log(queryName + ".Results"));
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
        sinkResultsToText(formattedResults, now);
        break;
      case AVRO:
        NexmarkUtils.console(
            "WARNING: with --sinkType=AVRO, actual query results will be discarded.");
        break;
      case BIGQUERY:
        // Multiple BigQuery backends to mimic what most customers do.
        PCollectionTuple res = formattedResults.apply(
            ParDo.named(queryName + ".Partition")
                 .withOutputTags(MAIN, TupleTagList.of(SIDE))
                 .of(new PartitionDoFn()));
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
    if (options.getMonitorJobs() && !canMonitor()) {
      throw new RuntimeException("Cannot use --monitorJobs with this runner since it does not "
                                 + "support monitoring.");
    }
    if (options.getManageResources() && !options.getMonitorJobs()) {
      throw new RuntimeException("If using --manageResources then must also use --monitorJobs.");
    }

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
        source = source.apply(NexmarkUtils.<Event>log(queryName + ".Events"));
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
