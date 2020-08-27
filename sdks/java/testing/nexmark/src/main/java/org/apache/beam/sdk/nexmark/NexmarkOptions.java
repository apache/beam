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

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Command line flags. */
public interface NexmarkOptions
    extends ApplicationNameOptions, GcpOptions, PipelineOptions, PubsubOptions, StreamingOptions {
  @Description("Which suite to run. Default is to use command line arguments for one job.")
  @Default.Enum("DEFAULT")
  NexmarkSuite getSuite();

  void setSuite(NexmarkSuite suite);

  @Description("If true, monitor the jobs as they run.")
  @Default.Boolean(false)
  boolean getMonitorJobs();

  void setMonitorJobs(boolean monitorJobs);

  @Description("Where the events come from.")
  NexmarkUtils.@Nullable SourceType getSourceType();

  void setSourceType(NexmarkUtils.SourceType sourceType);

  @Description("Prefix for input files if using avro input")
  @Nullable
  String getInputPath();

  void setInputPath(String inputPath);

  @Description("Where results go.")
  NexmarkUtils.@Nullable SinkType getSinkType();

  void setSinkType(NexmarkUtils.SinkType sinkType);

  @Description("Shall we export the summary to BigQuery.")
  @Default.Boolean(false)
  Boolean getExportSummaryToBigQuery();

  void setExportSummaryToBigQuery(Boolean exportSummaryToBigQuery);

  @Description("Which mode to run in when source is PUBSUB.")
  NexmarkUtils.@Nullable PubSubMode getPubSubMode();

  void setPubSubMode(NexmarkUtils.PubSubMode pubSubMode);

  @Description("How to serialize event objects to pubsub messages.")
  NexmarkUtils.@Nullable PubsubMessageSerializationMethod getPubsubMessageSerializationMethod();

  void setPubsubMessageSerializationMethod(
      NexmarkUtils.PubsubMessageSerializationMethod pubsubMessageSerializationMethod);

  @Description("Which query to run.")
  @Nullable
  String getQuery();

  void setQuery(String query);

  @Description("Skip the execution of the given queries (comma separated)")
  @Nullable
  String getSkipQueries();

  void setSkipQueries(String queries);

  @Description("Prefix for output files if using text output for results or running Query 10.")
  @Nullable
  String getOutputPath();

  void setOutputPath(String outputPath);

  @Description("Base name of pubsub topic to publish to in streaming mode.")
  @Nullable
  @Default.String("nexmark")
  String getPubsubTopic();

  void setPubsubTopic(String pubsubTopic);

  @Description("Base name of pubsub subscription to read from in streaming mode.")
  @Nullable
  @Default.String("nexmark")
  String getPubsubSubscription();

  void setPubsubSubscription(String pubsubSubscription);

  @Description("Base name of BigQuery table name if using BigQuery output.")
  @Nullable
  @Default.String("nexmark")
  String getBigQueryTable();

  void setBigQueryTable(String bigQueryTable);

  @Description("BigQuery dataset")
  @Default.String("nexmark")
  String getBigQueryDataset();

  void setBigQueryDataset(String bigQueryDataset);

  @Description(
      "Approximate number of events to generate. "
          + "Zero for effectively unlimited in streaming mode.")
  @Nullable
  Long getNumEvents();

  void setNumEvents(Long numEvents);

  @Description(
      "Time in seconds to preload the subscription with data, at the initial input rate "
          + "of the pipeline.")
  @Nullable
  Integer getPreloadSeconds();

  void setPreloadSeconds(Integer preloadSeconds);

  @Description(
      "Time in seconds to wait in pipelineResult.waitUntilFinish(), useful in streaming mode")
  @Nullable
  Integer getStreamTimeout();

  void setStreamTimeout(Integer streamTimeout);

  @Description("Proactively cancels streaming job after query is completed")
  @Default.Boolean(false)
  boolean getCancelStreamingJobAfterFinish();

  void setCancelStreamingJobAfterFinish(boolean cancelStreamingJobAfterFinish);

  @Description("Number of unbounded sources to create events.")
  @Nullable
  Integer getNumEventGenerators();

  void setNumEventGenerators(Integer numEventGenerators);

  @Description("Shape of event rate curve.")
  NexmarkUtils.@Nullable RateShape getRateShape();

  void setRateShape(NexmarkUtils.RateShape rateShape);

  @Description("Initial overall event rate (in --rateUnit).")
  @Nullable
  Integer getFirstEventRate();

  void setFirstEventRate(Integer firstEventRate);

  @Description("Next overall event rate (in --rateUnit).")
  @Nullable
  Integer getNextEventRate();

  void setNextEventRate(Integer nextEventRate);

  @Description("Unit for rates.")
  NexmarkUtils.@Nullable RateUnit getRateUnit();

  void setRateUnit(NexmarkUtils.RateUnit rateUnit);

  @Description("Overall period of rate shape, in seconds.")
  @Nullable
  Integer getRatePeriodSec();

  void setRatePeriodSec(Integer ratePeriodSec);

  @Description("If true, relay events in real time in streaming mode.")
  @Nullable
  Boolean getIsRateLimited();

  void setIsRateLimited(Boolean isRateLimited);

  @Description(
      "If true, use wallclock time as event time. Otherwise, use a deterministic"
          + " time in the past so that multiple runs will see exactly the same event streams"
          + " and should thus have exactly the same results.")
  @Nullable
  Boolean getUseWallclockEventTime();

  void setUseWallclockEventTime(Boolean useWallclockEventTime);

  @Description("Assert pipeline results match model results.")
  boolean getAssertCorrectness();

  void setAssertCorrectness(boolean assertCorrectness);

  @Description("Log all input events.")
  boolean getLogEvents();

  void setLogEvents(boolean logEvents);

  @Description("Log all query results.")
  boolean getLogResults();

  void setLogResults(boolean logResults);

  @Description("Average size in bytes for a person record.")
  @Nullable
  Integer getAvgPersonByteSize();

  void setAvgPersonByteSize(Integer avgPersonByteSize);

  @Description("Average size in bytes for an auction record.")
  @Nullable
  Integer getAvgAuctionByteSize();

  void setAvgAuctionByteSize(Integer avgAuctionByteSize);

  @Description("Average size in bytes for a bid record.")
  @Nullable
  Integer getAvgBidByteSize();

  void setAvgBidByteSize(Integer avgBidByteSize);

  @Description("Ratio of bids for 'hot' auctions above the background.")
  @Nullable
  Integer getHotAuctionRatio();

  void setHotAuctionRatio(Integer hotAuctionRatio);

  @Description("Ratio of auctions for 'hot' sellers above the background.")
  @Nullable
  Integer getHotSellersRatio();

  void setHotSellersRatio(Integer hotSellersRatio);

  @Description("Ratio of auctions for 'hot' bidders above the background.")
  @Nullable
  Integer getHotBiddersRatio();

  void setHotBiddersRatio(Integer hotBiddersRatio);

  @Description("Window size in seconds.")
  @Nullable
  Long getWindowSizeSec();

  void setWindowSizeSec(Long windowSizeSec);

  @Description("Window period in seconds.")
  @Nullable
  Long getWindowPeriodSec();

  void setWindowPeriodSec(Long windowPeriodSec);

  @Description("If in streaming mode, the holdback for watermark in seconds.")
  @Nullable
  Long getWatermarkHoldbackSec();

  void setWatermarkHoldbackSec(Long watermarkHoldbackSec);

  @Description("Roughly how many auctions should be in flight for each generator.")
  @Nullable
  Integer getNumInFlightAuctions();

  void setNumInFlightAuctions(Integer numInFlightAuctions);

  @Description("Maximum number of people to consider as active for placing auctions or bids.")
  @Nullable
  Integer getNumActivePeople();

  void setNumActivePeople(Integer numActivePeople);

  @Description("Filename of perf data to append to.")
  @Nullable
  String getPerfFilename();

  void setPerfFilename(String perfFilename);

  @Description("Filename of baseline perf data to read from.")
  @Nullable
  String getBaselineFilename();

  void setBaselineFilename(String baselineFilename);

  @Description("Filename of summary perf data to append to.")
  @Nullable
  String getSummaryFilename();

  void setSummaryFilename(String summaryFilename);

  @Description("Filename for javascript capturing all perf data and any baselines.")
  @Nullable
  String getJavascriptFilename();

  void setJavascriptFilename(String javascriptFilename);

  @Description(
      "If true, don't run the actual query. Instead, calculate the distribution "
          + "of number of query results per (event time) minute according to the query model.")
  boolean getJustModelResultRate();

  void setJustModelResultRate(boolean justModelResultRate);

  @Description("Coder strategy to use.")
  NexmarkUtils.@Nullable CoderStrategy getCoderStrategy();

  void setCoderStrategy(NexmarkUtils.CoderStrategy coderStrategy);

  @Description(
      "Delay, in milliseconds, for each event. We will peg one core for this "
          + "number of milliseconds to simulate CPU-bound computation.")
  @Nullable
  Long getCpuDelayMs();

  void setCpuDelayMs(Long cpuDelayMs);

  @Description(
      "Extra data, in bytes, to save to persistent state for each event. "
          + "This will force I/O all the way to durable storage to simulate an "
          + "I/O-bound computation.")
  @Nullable
  Long getDiskBusyBytes();

  void setDiskBusyBytes(Long diskBusyBytes);

  @Description("Skip factor for query 2. We select bids for every {@code auctionSkip}'th auction")
  @Nullable
  Integer getAuctionSkip();

  void setAuctionSkip(Integer auctionSkip);

  @Description("Fanout for queries 4 (groups by category id) and 7 (finds a global maximum).")
  @Nullable
  Integer getFanout();

  void setFanout(Integer fanout);

  @Description(
      "Maximum waiting time to clean personState in query3 (ie maximum waiting of the auctions"
          + " related to person in state in seconds in event time).")
  @Nullable
  Integer getMaxAuctionsWaitingTime();

  void setMaxAuctionsWaitingTime(Integer fanout);

  @Description("Length of occasional delay to impose on events (in seconds).")
  @Nullable
  Long getOccasionalDelaySec();

  void setOccasionalDelaySec(Long occasionalDelaySec);

  @Description("Probability that an event will be delayed by delayS.")
  @Nullable
  Double getProbDelayedEvent();

  void setProbDelayedEvent(Double probDelayedEvent);

  @Description("Maximum size of each log file (in events). For Query10 only.")
  @Nullable
  Integer getMaxLogEvents();

  void setMaxLogEvents(Integer maxLogEvents);

  @Description("How to derive names of resources.")
  @Default.Enum("QUERY_AND_SALT")
  NexmarkUtils.ResourceNameMode getResourceNameMode();

  void setResourceNameMode(NexmarkUtils.ResourceNameMode mode);

  @Description("If true, manage the creation and cleanup of topics, subscriptions and gcs files.")
  @Default.Boolean(true)
  boolean getManageResources();

  void setManageResources(boolean manageResources);

  @Description("If true, use pub/sub publish time instead of event time.")
  @Nullable
  Boolean getUsePubsubPublishTime();

  void setUsePubsubPublishTime(Boolean usePubsubPublishTime);

  @Description(
      "Number of events in out-of-order groups. 1 implies no out-of-order events. "
          + "1000 implies every 1000 events per generator are emitted in pseudo-random order.")
  @Nullable
  Long getOutOfOrderGroupSize();

  void setOutOfOrderGroupSize(Long outOfOrderGroupSize);

  @Description("If false, do not add the Monitor and Snoop transforms.")
  @Nullable
  Boolean getDebug();

  void setDebug(Boolean value);

  @Description("If set, cancel running pipelines after this long")
  @Nullable
  Long getRunningTimeMinutes();

  void setRunningTimeMinutes(Long value);

  @Description(
      "If set and --monitorJobs is true, check that the system watermark is never more "
          + "than this far behind real time")
  @Nullable
  Long getMaxSystemLagSeconds();

  void setMaxSystemLagSeconds(Long value);

  @Description(
      "If set and --monitorJobs is true, check that the data watermark is never more "
          + "than this far behind real time")
  @Nullable
  Long getMaxDataLagSeconds();

  void setMaxDataLagSeconds(Long value);

  @Description("Only start validating watermarks after this many seconds")
  @Nullable
  Long getWatermarkValidationDelaySeconds();

  void setWatermarkValidationDelaySeconds(Long value);

  @Description(
      "Specify 'sql' to use Calcite SQL queries "
          + "or 'zetasql' to use ZetaSQL queries."
          + "Otherwise Java transforms will be used")
  @Nullable
  String getQueryLanguage();

  void setQueryLanguage(String value);

  @Description("Base name of Kafka events topic in streaming mode.")
  @Nullable
  @Default.String("nexmark")
  String getKafkaTopic();

  void setKafkaTopic(String value);

  @Description("Base name of Kafka results topic in streaming mode.")
  @Nullable
  @Default.String("nexmark-results")
  String getKafkaResultsTopic();

  void setKafkaResultsTopic(String value);

  @Description("Kafka Bootstrap Server domains.")
  @Nullable
  String getBootstrapServers();

  void setBootstrapServers(String value);

  @Description("Same as --numWorkers in DataflowPipelineWorkerPoolOptions")
  int getNumWorkers();

  void setNumWorkers(int value);

  @Description("Same as --maxNumWorkers in DataflowPipelineWorkerPoolOptions.")
  int getMaxNumWorkers();

  void setMaxNumWorkers(int value);

  @Description("Number of queries to run in parallel.")
  @Default.Integer(1)
  int getNexmarkParallel();

  void setNexmarkParallel(int value);

  @Description("InfluxDB measurement to publish results to.")
  @Nullable
  String getInfluxMeasurement();

  void setInfluxMeasurement(@Nullable String measurement);

  @Description("InfluxDB host.")
  @Nullable
  String getInfluxHost();

  void setInfluxHost(@Nullable String host);

  @Description("InfluxDB database.")
  @Nullable
  String getInfluxDatabase();

  void setInfluxDatabase(@Nullable String database);

  @Description("Shall we export the summary to InfluxDB.")
  @Default.Boolean(false)
  boolean getExportSummaryToInfluxDB();

  void setExportSummaryToInfluxDB(boolean exportSummaryToInfluxDB);

  @Description("Base name of measurement name if using InfluxDB output.")
  @Nullable
  @Default.String("nexmark")
  String getBaseInfluxMeasurement();

  void setBaseInfluxMeasurement(String influxDBMeasurement);

  @Description("Name of retention policy for Influx data.")
  @Nullable
  String getInfluxRetentionPolicy();

  void setInfluxRetentionPolicy(String influxRetentionPolicy);
}
