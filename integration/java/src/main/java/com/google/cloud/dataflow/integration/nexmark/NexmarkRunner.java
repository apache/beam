package com.google.cloud.dataflow.integration.nexmark;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Run a single Nexmark query using a given configuration.
 */
public class NexmarkRunner {
  private final Options options;

  /**
   * Which configuration should we run.
   */
  @Nullable
  private NexmarkConfiguration configuration;

  /**
   * Accumulate the pub/sub subscriptions etc which should be cleaned up on end of run.
   */
  @Nullable
  private PubsubHelper pubsub;

  /**
   * Monitor for published events if in pub/sub COMBINED mode.
   */
  @Nullable
  private Monitor<Event> publisherMonitor;

  /**
   * Query name.
   */
  @Nullable
  private String queryName;

  /**
   * If sending events via pub/sub, the full topic name to use.
   */
  @Nullable
  private String inputTopic;

  public NexmarkRunner(Options options) {
    this.options = options;
  }

  /**
   * Return a pub/sub helper.
   *
   * @throws IOException
   */
  private PubsubHelper getPubsub() throws IOException {
    if (pubsub == null) {
      pubsub = PubsubHelper.create(options, options.getProject());
    }
    return pubsub;
  }

  /**
   * Return a source of synthetic events.
   */
  private PCollection<Event> createSyntheticSource(Pipeline p) {
    if (p.getRunner() instanceof DirectPipelineRunner || !options.isStreaming()) {
      return p.apply(NexmarkUtils.batchEventsSource(queryName, configuration));
    } else {
      return p.apply(NexmarkUtils.streamEventsSource(queryName, configuration));
    }
  }

  /**
   * Return pub/sub sink.
   */
  private PubsubIO.Write.Bound<Event> pubsubEventSink() {
    PubsubIO.Write.Bound<Event> io =
        PubsubIO.Write.named(queryName + ".Write(" + inputTopic + ")")
                      .topic(inputTopic)
                      .idLabel(NexmarkUtils.PUBSUB_ID)
                      .withCoder(Event.CODER)
                      .named(queryName + ".PubsubSourceWrite");
    if (!configuration.usePubsubPublishTime) {
      io = io.timestampLabel(NexmarkUtils.PUBSUB_TIMESTAMP);
    }
    return io;
  }

  /**
   * Return pub/sub source.
   */
  private PubsubIO.Read.Bound<Event> pubsubEventSource(String subscription) {
    PubsubIO.Read.Bound<Event> io =
        PubsubIO.Read.named(queryName + ".Read(" + subscription + ")")
                     .subscription(subscription)
                     .idLabel(NexmarkUtils.PUBSUB_ID)
                     .withCoder(Event.CODER)
                     .named(queryName + ".PubsubSourceRead");
    if (!configuration.usePubsubPublishTime) {
      io = io.timestampLabel(NexmarkUtils.PUBSUB_TIMESTAMP);
    }
    return io;
  }

  /**
   * Return pub/sub sink for results.
   */
  private PubsubIO.Write.Bound<String> pubsubResultSink(String topic) {
    PubsubIO.Write.Bound<String> io =
        PubsubIO.Write.named(queryName + ".Write(" + topic + ")")
                      .topic(topic)
                      .idLabel(NexmarkUtils.PUBSUB_ID)
                      .named(queryName + ".PubsubSinkWrite");
    if (!configuration.usePubsubPublishTime) {
      io = io.timestampLabel(NexmarkUtils.PUBSUB_TIMESTAMP);
    }
    return io;
  }

  /**
   * Return number of cores for configuration's machine type.
   */
  private int coresPerWorker(DataflowPipelineOptions options) {
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

  /**
   * Return source of events for this run, or null if we are simply publishing events
   * to pub/sub.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  private PCollection<Event> createSource(Pipeline p, long now)
      throws IOException, InterruptedException {
    PCollection<Event> source = null;
    switch (configuration.sourceType) {
      case DIRECT:
        source = createSyntheticSource(p);
        break;
      case AVRO:
        if (options.getInputFilePrefix() == null) {
          throw new RuntimeException(
              "If sourceType is AVRO, --inputFilePrefix must be specified.");
        }
        PCollection<Event> preTimestamp = p.apply(AvroIO.Read.named("ReadFromAvro")
                                                             .from(options.getInputFilePrefix()
                                                                   + "*.avro")
                                                             .withSchema(Event.class));
        source = preTimestamp.apply("adjust timestamp",
            ParDo.of(NexmarkQuery.EVENT_TIMESTAMP_FROM_DATA));
        break;
      case PUBSUB:
        if (pubsubTopic == null) {
          throw new RuntimeException("must supply a --pubsubTopic if using --sourceType=PUBSUB");
        }

        // Check some flags.
        switch (configuration.pubSubMode) {
          case SUBSCRIBE_ONLY:
            break;
          case PUBLISH_ONLY:
            if (manageResources && options.getRunner() != BlockingDataflowPipelineRunner.class) {
              throw new RuntimeException(
                  "If --manageResources=true and --pubSubMode=PUBLISH_ONLY then "
                  + "--runner=BlockingDataflowPipelineRunner so that this program "
                  + "can cleanup the pub/sub topic on exit.");
            }
            break;
          case COMBINED:
            if (options.getRunner() != DataflowPipelineRunner.class || !monitorJobs) {
              throw new RuntimeException(
                  "if --pubSubMode=COMBINED then --runner=DataflowPipelineRunner and "
                  + "--monitorJobs=true so that the two pipelines can be managed.");
            }
            break;
        }

        // Choose a topic name for events, and optionally topics to use to synchronize
        // the publisher and subscriber jobs.
        String shortTopic;
        if (uniqify) {
          // Salt the topic name so we can run multiple jobs in parallel.
          shortTopic = String.format("%s_%d_%s_source", pubsubTopic, now, queryName);
        } else {
          shortTopic = String.format("%s_%s_source", pubsubTopic, queryName);
        }
        String shortSubscription = shortTopic;

        // Create/confirm the topic.
        if (!manageResources || configuration.pubSubMode == PubSubMode.SUBSCRIBE_ONLY) {
          // The topic should already have been created by the user or
          // a companion 'PUBLISH_ONLY' process.
          inputTopic = getPubsub().reuseTopic(shortTopic);
        } else {
          // Create a fresh topic to loopback via. It will be destroyed when the
          // (necessarily blocking) job is done.
          inputTopic = getPubsub().createTopic(shortTopic);
        }

        // Create/confirm the subscription.
        String subscription = null;
        if (configuration.pubSubMode == PubSubMode.PUBLISH_ONLY) {
          // Nothing to consume.
        } else if (!manageResources) {
          // The subscription should already have been created by the user.
          subscription = getPubsub().reuseSubscription(shortTopic, shortSubscription);
        } else {
          subscription = getPubsub().createSubscription(shortTopic, shortSubscription);
        }

        // Setup the sink for the publisher.
        switch (configuration.pubSubMode) {
          case SUBSCRIBE_ONLY:
            // Nothing to publish.
            break;
          case PUBLISH_ONLY:
            // Send synthesized events to pub/sub in this job.
            createSyntheticSource(p).apply(NexmarkUtils.snoop(queryName)).apply(pubsubEventSink());
            break;
          case COMBINED:
            if (options.getRunner() != DataflowPipelineRunner.class || !monitorJobs) {
              throw new RuntimeException(
                  "if --pubSubMode=COMBINED then you must use --runner=DataflowPipelineRunner "
                  + "and --monitorJobs=true so that the publisher job can be shutdown cleanly");
            }
            // Send synthesized events to pub/sub in separate publisher job.
            // We won't start the main pipeline until the publisher has sent the pre-load events.
            // We'll shutdown the publisher job when we notice the main job has finished.
            String jobName = options.getJobName();
            String appName = options.getAppName();
            options.setJobName("p-" + jobName);
            options.setAppName("p-" + appName);
            int coresPerWorker = coresPerWorker(options);
            int eventGeneratorWorkers =
                (configuration.numEventGenerators + coresPerWorker - 1)
                / coresPerWorker;
            options.setMaxNumWorkers(Math.min(options.getMaxNumWorkers(), eventGeneratorWorkers));
            options.setNumWorkers(Math.min(options.getNumWorkers(), eventGeneratorWorkers));
            publisherMonitor = new Monitor<Event>(queryName, "publisher");
            Pipeline q = Pipeline.create(options);
            createSyntheticSource(q)
                .apply(publisherMonitor.getTransform())
                .apply(pubsubEventSink());
            PrintStream stdout = System.out;
            try {
              // Suppress output of publisher job; it makes the output of the command harder to
              // interpret.
              System.setOut(new PrintStream(new OutputStream() {
                @Override
                public void write(int b) {
                }
              }));
              publisherResult = q.run();
            } finally {
              System.setOut(stdout);
            }
            System.out.println(
                "Publisher job running as " + ((DataflowPipelineJob) publisherResult).getJobId());
            options.setJobName(jobName);
            options.setAppName(appName);
            options.setMaxNumWorkers(options.getMaxNumWorkers());
            options.setNumWorkers(options.getNumWorkers());
            waitForPublisherPreload();
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
            source = p.apply(pubsubEventSource(subscription));
            break;
        }
        break;
    }
    return source;
  }

  /**
   * Consume the given results.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  private void createSink(PCollection<TimestampedValue<KnownSize>> results, long now)
      throws IOException, InterruptedException {

    if (NexmarkUtils.SinkType.COUNT_ONLY == configuration.sinkType) {
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
        // Publish results to pubsub. Don't bother consuming them, the published
        // results will be discarded when we delete the 'sink' topic.
        if (pubsubTopic == null) {
          throw new RuntimeException("must supply a --pubsubTopic if using --sinkType=PUBSUB");
        }
        String shortTopic;
        if (uniqify) {
          shortTopic = String.format("%s_%d_%s_sink", pubsubTopic, now, queryName);
        } else {
          shortTopic = String.format("%s_%s_sink", pubsubTopic, queryName);
        }
        String topic;
        if (!manageResources) {
          topic = getPubsub().reuseTopic(shortTopic);
        } else {
          topic = getPubsub().createTopic(shortTopic);
        }
        formattedResults.apply(pubsubResultSink(topic));
        break;
      case TEXT:
        // Write results to text. Only works in batch mode.
        if (options.isStreaming()) {
          throw new RuntimeException("can only use --sinkType=TEXT with --streaming=false");
        }
        if (outputPath == null) {
          throw new RuntimeException("must supply an --outputPath if using --sinkType=TEXT");
        }
        String fullFilename;
        if (uniqify) {
          fullFilename = String.format("%s/nexmark_%d_%s.txt", outputPath, now, queryName);
        } else {
          fullFilename = String.format("%s/nexmark_%s.txt", outputPath, queryName);
        }
        formattedResults.apply(TextIO.Write.to(fullFilename).named(queryName + ".Text"));
        break;
      case AVRO:
        NexmarkUtils.console(null, "WARNING: with --sinkType=AVRO, actual query results will be "
                                   + "discarded.", outputPath);
        break;
      case BIGQUERY:
        NexmarkUtils.console(null, "Writing events to BigQuery table %s", outputPath);
        formattedResults
            .apply(ParDo.of(new StringToTableRow()))
            .apply(BigQueryIO.Write.named("WriteBigQuery(Events)")
                                   .to(options.getProject() + ":nexmark.table_"
                                       + new Random().nextInt(Integer.MAX_VALUE))
                                   .withSchema(new TableSchema().setFields(
                                       new ArrayList<TableFieldSchema>() {
                                         {
                                           add(new TableFieldSchema().setName("event")
                                                                     .setType("STRING"));
                                         }
                                       })));
        break;
      case COUNT_ONLY:
        // Short-circuited above.
        throw new RuntimeException();
    }
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
   * Sink all raw Events in {@code source} to {@code this.outputPath}.
   * <p>
   * This will configure the job to write the following files:
   * <p>
   * <ul>
   * <li>{@code $outputPath/event*.avro} All Event entities.
   * <li>{@code $outputPath/auction*.avro} Auction entities.
   * <li>{@code $outputPath/bid*.avro} Bid entities.
   * <li>{@code $outputPath/person*.avro} Person entities.
   * </ul>
   *
   * @param source A PCollection of events.
   */
  private void sinkToAvro(final PCollection<Event> source) {
    if (options.isStreaming()) {
      throw new RuntimeException("can only use Avro SinkType with --streaming=false");
    }
    if (outputPath == null) {
      throw new RuntimeException("Must supply an --outputPath if using --sinkType=AVRO");
    }
    NexmarkUtils.console(null, "Writing events in Avro to %s", outputPath);
    source.apply(AvroIO.Write.named("WriteAvro(Events)")
                             .to(outputPath + "/event")
                             .withSuffix(".avro")
                             .withSchema(Event.class));
    source.apply(NexmarkQuery.JUST_BIDS)
          .apply(AvroIO.Write.named("WriteAvro(Bids)")
                             .to(outputPath + "/bid")
                             .withSuffix(".avro")
                             .withSchema(Bid.class));
    source.apply(NexmarkQuery.JUST_NEW_AUCTIONS)
          .apply(AvroIO.Write.named("WriteAvro(Auctions)")
                             .to(outputPath + "/auction")
                             .withSuffix(".avro")
                             .withSchema(Auction.class));
    source.apply(NexmarkQuery.JUST_NEW_PERSONS)
          .apply(AvroIO.Write.named("WriteAvro(People)")
                             .to(outputPath + "/person")
                             .withSuffix(".avro")
                             .withSchema(Person.class));
  }

  /**
   * Run {@code configuration} and return its performance (if using DataflowPipelineRunner
   * and {@link #monitorJobs} is true).
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Nullable
  public NexmarkPerf run(NexmarkConfiguration runConfiguration)
      throws IOException, InterruptedException {
    //
    // Setup per-run state.
    //
    Preconditions.checkState(configuration == null);
    Preconditions.checkState(pubsub == null);
    Preconditions.checkState(publisherResult == null);
    Preconditions.checkState(mainResult == null);
    Preconditions.checkState(queryName == null);
    configuration = runConfiguration;

    // GCS URI patterns to delete on exit.
    List<String> pathsToDelete = new ArrayList<>();

    try {
      NexmarkUtils.console(null, "running %s", configuration.toShortString());

      if (configuration.numEvents < 0) {
        NexmarkUtils.console(null, "skipping since configuration is disabled");
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
          new Query0Model(configuration), new Query1Model(configuration),
          new Query2Model(configuration), new Query3Model(configuration),
          new Query4Model(configuration), new Query5Model(configuration),
          new Query6Model(configuration), new Query7Model(configuration),
          new Query8Model(configuration), new Query9Model(configuration),
          null, null, null);
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
        if (monitorJobs && options.getRunner() != DataflowPipelineRunner.class) {
          throw new RuntimeException("can only use --monitorJobs=true if also "
                                     + "using --runner=DataflowPipelineRunner");
        }

        // Optionally sink events in Avro format
        if (configuration.sinkType == NexmarkUtils.SinkType.AVRO) {
          this.sinkToAvro(source);
        }

        // Special hacks for Query 10 (big logger).
        if (configuration.query == 10) {
          String path = null;
          if (outputPath != null) {
            if (uniqify) {
              path = String.format("%s/%d_logs", outputPath, now);
            } else {
              path = String.format("%s/logs", outputPath);
            }
          }
          ((Query10) query).setOutputPath(path);
          ((Query10) query).setMaxNumWorkers(
              Math.max(options.getMaxNumWorkers(), options.getNumWorkers()));
          if (path != null && manageResources) {
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
          results.setIsBoundedInternal(IsBounded.BOUNDED);
          // If we have a finite number of events then assert our pipeline's
          // results match those of a model using the same sequence of events.
          DataflowAssert.that(results).satisfies(model.assertionFor());
        }

        // Output results.
        createSink(results, now);
      }

      mainResult = p.run();
      return monitor(query);
    } finally {
      //
      // Cleanup per-run state.
      //
      if (pubsub != null) {
        // Delete any subscriptions and topics we created.
        pubsub.cleanup();
        pubsub = null;
      }
      // TODO: Find a way to cleanup pathsToDelete robustly without depending on gsutil.
      configuration = null;
      publisherResult = null;
      mainResult = null;
      queryName = null;
    }
  }

  /**
   * Calculate the distribution of the expected rate of results per minute (in event time, not
   * wallclock time).
   */
  private void modelResultRates(NexmarkQueryModel model) {
    List<Long> counts = Lists.newArrayList(model.simulator().resultsPerWindow());
    Collections.sort(counts);
    int n = counts.size();
    if (n < 5) {
      NexmarkUtils.console(null, "Query%d: only %d samples", model.configuration.query, n);
    } else {
      NexmarkUtils.console(null, "Query%d: N:%d; min:%d; 1st%%:%d; mean:%d; 3rd%%:%d; max:%d",
          model.configuration.query, n, counts.get(0), counts.get(n / 4),
          counts.get(n / 2),
          counts.get(n - 1 - n / 4), counts.get(n - 1));
    }
  }

  /**
   * Monitor the progress of the publisher job. Return when it has produced at
   * least {@code configuration.preloadSeconds} worth of events.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  private void waitForPublisherPreload() throws IOException, InterruptedException {
    if (!monitorJobs) {
      return;
    }
    if (options.getRunner() != DataflowPipelineRunner.class) {
      return;
    }
    if (!(publisherResult instanceof DataflowPipelineJob)) {
      return;
    }
    if (configuration.preloadSeconds <= 0) {
      return;
    }

    NexmarkUtils.console(null, "waiting for publisher to pre-load");

    DataflowPipelineJob job = (DataflowPipelineJob) publisherResult;

    while (true) {
      PipelineResult.State state = job.getState();
      long numEvents = getLong(job, publisherMonitor.getElementCounter());
      if (numEvents > 0) {
        int waitSeconds = configuration.preloadSeconds;
        NexmarkUtils.console(null, "%s publisher (Saw first event, waiting %ds for preload)", state,
            configuration.preloadSeconds);
        while (waitSeconds > 0) {
          NexmarkUtils.console(
              null, "%s publisher (%ds until preload is done)", state, waitSeconds);
          Thread.sleep(TimeUnit.SECONDS.toMillis(Math.min(60, waitSeconds)));
          waitSeconds -= 60;
        }
        return;
      }

      switch (state) {
        case UNKNOWN:
        case STOPPED:
        case RUNNING:
          // Keep waiting.
          break;
        case DONE:
          // All done.
          NexmarkUtils.console(null, "publisher pipeline done");
          return;
        case CANCELLED:
        case FAILED:
        case UPDATED:
          // Something went wrong.
          NexmarkUtils.console(null, "publisher pipeline failed", state, numEvents);
          return;
      }

      Thread.sleep(PERF_DELAY.getMillis());
    }
  }

  /**
   * Monitor the performance and progress of a running job. Return final performance if
   * it was measured.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Nullable
  private NexmarkPerf monitor(NexmarkQuery query) throws IOException, InterruptedException {
    if (!monitorJobs) {
      return null;
    }
    if (options.getRunner() != DataflowPipelineRunner.class) {
      return null;
    }
    if (!(mainResult instanceof DataflowPipelineJob)) {
      return null;
    }
    // If we are not in debug mode, we have no event count or result count monitors.
    boolean monitorsActive =  configuration.debug;

    if (monitorsActive) {
      NexmarkUtils.console(null, "waiting for main pipeline to 'finish'");
    } else {
      NexmarkUtils.console(null, "debug=false, so job will not self-cancel");
    }


    DataflowPipelineJob job = (DataflowPipelineJob) mainResult;
    DataflowPipelineJob publisherJob = (DataflowPipelineJob) publisherResult;
    List<NexmarkPerf.ProgressSnapshot> snapshots = new ArrayList<>();
    Instant start = Instant.now();
    Instant end =
        options.getRunningTimeMinutes() != null
        ? start.plus(Duration.standardMinutes(options.getRunningTimeMinutes()))
        : new Instant(Long.MAX_VALUE);
    if (options.getPreloadSeconds() != null) {
      end = end.minus(Duration.standardSeconds(options.getPreloadSeconds()));
    }
    Instant lastActivity = null;
    NexmarkPerf perf = null;
    boolean waitingForShutdown = false;
    boolean publisherCancelled = false;
    List<String> errors = new ArrayList<>();

    while (true) {
      Instant now = Instant.now();
      if (now.isAfter(end) && !waitingForShutdown) {
        NexmarkUtils.console(null, "Reached end of test, cancelling job");
        job.cancel();
        if (publisherResult != null) {
          publisherJob.cancel();
          publisherCancelled = true;
        }
        waitingForShutdown = true;
      }

      PipelineResult.State state = job.getState();
      NexmarkUtils.console(
          now, "%s %s%s", state, queryName, waitingForShutdown ? " (waiting for shutdown)" : "");

      NexmarkPerf currPerf;
      if (monitorsActive) {
        currPerf = currentPerf(start, now, job, snapshots, query.eventMonitor,
            query.resultMonitor);
      } else {
        currPerf = null;
      }

      if (perf == null || perf.anyActivity(currPerf)) {
        lastActivity = now;
      }

      if (options.isStreaming() && !waitingForShutdown) {
        Duration quietFor = new Duration(lastActivity, now);
        if (query.getFatalCount() != null && getLong(job, query.getFatalCount()) > 0) {
          NexmarkUtils.console(now, "job has fatal errors, cancelling.");
          errors.add(String.format("Pipeline reported %s fatal errors", query.getFatalCount()));
          waitingForShutdown = true;
        } else if (monitorsActive && configuration.numEvents > 0
                   && currPerf.numEvents == configuration.numEvents
                   && currPerf.numResults >= 0 && quietFor.isLongerThan(DONE_DELAY)) {
          NexmarkUtils.console(now, "streaming query appears to have finished, cancelling job.");
          waitingForShutdown = true;
        } else if (quietFor.isLongerThan(STUCK_TERMINATE_DELAY)) {
          NexmarkUtils.console(
              now, "streaming query appears to have gotten stuck, cancelling job.");
          errors.add("Streaming job was cancelled since appeared stuck");
          waitingForShutdown = true;
        } else if (quietFor.isLongerThan(STUCK_WARNING_DELAY)) {
          NexmarkUtils.console(
              now, "WARNING: streaming query appears to have been stuck for %d min.",
              quietFor.getStandardMinutes());
          errors.add(
              String.format("Streaming query was stuck for %d min", quietFor.getStandardMinutes()));
        }

        errors.addAll(checkWatermarks(job, start));

        if (waitingForShutdown) {
          job.cancel();
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

      if (lastActivity.equals(now)) {
        NexmarkUtils.console(now, "new perf %s", perf);
      } else {
        NexmarkUtils.console(now, "no activity");
      }

      Thread.sleep(PERF_DELAY.getMillis());
    }

    perf.errors = errors;
    perf.snapshots = snapshots;
    NexmarkUtils.console(null, "final perf %s", perf);

    if (publisherResult != null) {
      if (publisherCancelled) {
        publisherJob.waitToFinish(5, TimeUnit.MINUTES, null);
      } else {
        publisherJob.cancel();
      }
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
   * <p>
   * <p>Returns a list of errors detected.
   */
  private List<String> checkWatermarks(DataflowPipelineJob job, Instant start) {
    Instant now = Instant.now();
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
              || now.isAfter(start.plus(
              Duration.standardSeconds(options.getWatermarkValidationDelaySeconds())))) {
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
              NexmarkUtils.console(null, msg);
            }
          }
          if (!foundWatermarks) {
            NexmarkUtils.console(null, "No known watermarks in update: " + metrics);
            if (now.isAfter(start.plus(Duration.standardMinutes(5)))) {
              errors.add("No known watermarks found.  Metrics were " + metrics);
            }
          }
        }
      }
    } catch (IOException e) {
      NexmarkUtils.console(null, "Warning: failed to get JobMetrics: " + e);
    }

    return errors;
  }

  /**
   * Return the current performance given {@code eventMonitor} and {@code resultMonitor}.
   */
  private NexmarkPerf currentPerf(
      Instant start, Instant now, DataflowPipelineJob job,
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
      perf.startupDelaySec = (eventStart - start.getMillis()) / 1000.0;
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
      perf.shutdownDelaySec = (now.getMillis() - resultEnd) / 1000.0;
    }

    perf.jobId = job.getJobId();
    // As soon as available, try to capture cumulative cost at this point too.

    NexmarkPerf.ProgressSnapshot snapshot = new NexmarkPerf.ProgressSnapshot();
    snapshot.secSinceStart = (now.getMillis() - start.getMillis()) / 1000.0;
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
      NexmarkUtils.console(
          null, "%d samples not enough to calculate steady-state event rate", numSamples);
      return;
    }

    // We'll look at only the middle third samples.
    int sampleStart = dataStart + numSamples / 3;
    int sampleEnd = dataEnd - numSamples / 3;

    double sampleSec =
        snapshots.get(sampleEnd).secSinceStart - snapshots.get(sampleStart).secSinceStart;
    if (sampleSec < MIN_WINDOW.getStandardSeconds()) {
      // Not sampled over enough time.
      NexmarkUtils.console(null,
          "sample of %.1f sec not long enough to calculate steady-state event "
          + "rate",
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
    NexmarkUtils.console(
        null, "revising events/sec from %.1f to %.1f", perf.eventsPerSec, eventsPerSec);
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
      Instant now, DataflowPipelineJob job, Aggregator<Long, Long> aggregator) {
    try {
      Collection<Long> values = job.getAggregatorValues(aggregator).getValues();
      if (values.size() != 1) {
        return -1;
      }
      long value = Iterables.getOnlyElement(values);
      if (Math.abs(value - now.getMillis()) > Duration.standardDays(10000).getMillis()) {
        return -1;
      }
      return value;
    } catch (AggregatorRetrievalException e) {
      return -1;
    }
  }

}
