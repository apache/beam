/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.integration.nexmark;

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.common.collect.Sets;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * An implementation of the 'NEXMark queries.' These are 8 queries in Oracle's 'Continuous Query
 * Language' (CQL) over a three table schema representing on online auction system:
 * <ul>
 * <li>{@link Person} represents a person submitting an item for auction and/or making a bid
 * on an auction.
 * <li>{@link Auction} represents an item under auction.
 * <li>{@link Bid} represents a bid for an item under auction.
 * </ul>
 * The queries exercise many aspects of streaming dataflow.
 *
 * <p>We synthesize the creation of people, auctions and bids in real-time. The data is not
 * particularly sensible.
 *
 * <p>See
 * <a href="http://datalab.cs.pdx.edu/niagaraST/NEXMark/">
 * http://datalab.cs.pdx.edu/niagaraST/NEXMark/</a>
 */
class NexmarkDriver {
  /**
   * Command line flags.
   */
  public interface Options extends DataflowPipelineOptions {
    @Description("Which suite to run. Default is to use command line arguments for one job.")
    @Default.Enum("DEFAULT")
    NexmarkSuite getSuite();

    void setSuite(NexmarkSuite suite);

    @Description("If true, and using the DataflowPipelineRunner, monitor the jobs as they run.")
    @Default.Boolean(false)
    boolean getMonitorJobs();

    void setMonitorJobs(boolean monitorJobs);

    @Description("Where the events come from.")
    @Nullable
    NexmarkUtils.SourceType getSourceType();

    void setSourceType(NexmarkUtils.SourceType sourceType);

    @Description("Avro input file pattern; only valid if source type is avro")
    @Nullable
    String getInputFilePrefix();

    void setInputFilePrefix(String filepattern);

    @Description("Where results go.")
    @Nullable
    NexmarkUtils.SinkType getSinkType();

    void setSinkType(NexmarkUtils.SinkType sinkType);

    @Description("Which mode to run in when source is PUBSUB.")
    @Nullable
    NexmarkUtils.PubSubMode getPubSubMode();

    void setPubSubMode(NexmarkUtils.PubSubMode pubSubMode);

    @Description("Which query to run.")
    @Nullable
    Integer getQuery();

    void setQuery(Integer query);

    @Description("Prefix for output files if using text output for results or running Query 10.")
    @Nullable
    String getOutputPath();

    void setOutputPath(String outputPath);

    @Description("Base name of pubsub topic to publish to in streaming mode.")
    @Nullable
    @Default.String("nexmark")
    String getPubsubTopic();

    void setPubsubTopic(String pubsubTopic);

    @Description("Approximate number of events to generate."
        + "Zero for effectively unlimited in streaming mode.")
    @Nullable
    Long getNumEvents();

    void setNumEvents(Long numEvents);

    @Description("Number of events to generate at the special pre-load rate. This is useful "
        + "for generating a backlog of events on pub/sub before the main query begins.")
    @Nullable
    Long getNumPreloadEvents();

    void setNumPreloadEvents(Long numPreloadEvents);

    @Description("Number of unbounded sources to create events.")
    @Nullable
    Integer getNumEventGenerators();

    void setNumEventGenerators(Integer numEventGenerators);

    @Description("Shape of event qps curve.")
    @Nullable
    NexmarkUtils.QpsShape getQpsShape();

    void setQpsShape(NexmarkUtils.QpsShape qpsShape);

    @Description("Initial overall event qps.")
    @Nullable
    Integer getFirstEventQps();

    void setFirstEventQps(Integer firstEventQps);

    @Description("Next overall event qps.")
    @Nullable
    Integer getNextEventQps();

    void setNextEventQps(Integer nextEventQps);

    @Description("Overall period of qps shape, in seconds.")
    @Nullable
    Integer getQpsPeriodSec();

    void setQpsPeriodSec(Integer qpsPeriodSec);

    @Description("Overall event qps while pre-loading. "
        + "Typcially as large as possible for given pub/sub quota.")
    @Nullable
    Integer getPreloadEventQps();

    void setPreloadEventQps(Integer preloadEventQps);

    @Description("If true, relay events in real time in streaming mode.")
    @Nullable
    Boolean getIsRateLimited();

    void setIsRateLimited(Boolean isRateLimited);

    @Description("If true, use wallclock time as event time. Otherwise, use a deterministic"
        + " time in the past so that multiple runs will see exactly the same event streams"
        + " and should thus have exactly the same results.")
    @Nullable
    Boolean getUseWallclockEventTime();

    void setUseWallclockEventTime(Boolean useWallclockEventTime);

    @Description("Assert pipeline results match model results.")
    @Nullable
    Boolean getAssertCorrectness();

    void setAssertCorrectness(Boolean assertCorrectness);

    @Description("Log all query results.")
    @Nullable
    Boolean getLogResults();

    void setLogResults(Boolean logResults);

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

    @Description("If true, don't run the actual query. Instead, calculate the distribution "
        + "of number of query results per (event time) minute according to the query model.")
    @Nullable
    Boolean getJustModelResultRate();

    void setJustModelResultRate(Boolean justModelResultRate);

    @Description("Coder strategy to use.")
    @Nullable
    NexmarkUtils.CoderStrategy getCoderStrategy();

    void setCoderStrategy(NexmarkUtils.CoderStrategy coderStrategy);

    @Description("Delay, in milliseconds, for each event. We will peg one core for this "
        + "number of milliseconds to simulate CPU-bound computation.")
    @Nullable
    Long getCpuDelayMs();

    void setCpuDelayMs(Long cpuDelayMs);

    @Description("Extra data, in bytes, to save to persistent state for each event. "
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

    @Description("If true, make sure all topics, subscriptions and gcs filenames are unique.")
    @Default.Boolean(true)
    boolean getUniqify();

    void setUniqify(boolean uniqify);

    @Description("If true, manage the creation and cleanup of topics, subscriptions and gcs files.")
    @Default.Boolean(true)
    boolean getManageResources();

    void setManageResources(boolean manageResources);

    @Description("If true, use pub/sub publish time instead of event time.")
    @Nullable
    Boolean getUsePubsubPublishTime();

    void setUsePubsubPublishTime(Boolean usePubsubPublishTime);

    @Description("Number of events in out-of-order groups. 1 implies no out-of-order events. "
        + "1000 implies every 1000 events per generator are emitted in pseudo-random order.")
    @Nullable
    Long getOutOfOrderGroupSize();

    void setOutOfOrderGroupSize(Long outOfOrderGroupSize);


    @Description("If set, cancel running pipelines after this long")
    @Nullable
    Long getRunningTimeMinutes();
    void setRunningTimeMinutes(Long value);

    @Description("If set and --monitorJobs is true, check that the system watermark is never more "
        + "than this far behind real time")
    @Nullable
    Long getMaxSystemLagSeconds();
    void setMaxSystemLagSeconds(Long value);

    @Description("If set and --monitorJobs is true, check that the data watermark is never more "
        + "than this far behind real time")
    @Nullable
    Long getMaxDataLagSeconds();
    void setMaxDataLagSeconds(Long value);

    @Description("If false, do not add the Monitor and Snoop transforms.")
    @Nullable
    Boolean getDebug();
    void setDebug(Boolean value);
  }

  /**
   * Entry point.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    // Gather command line args, baseline, configurations, etc.
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    NexmarkRunner runner =
        new NexmarkRunner(options, options.getOutputPath(), options.getPubsubTopic(),
            options.getMonitorJobs(), options.getUniqify(), options.getManageResources());
    Instant start = Instant.now();
    Map<NexmarkConfiguration, NexmarkPerf> baseline = loadBaseline(options.getBaselineFilename());
    Map<NexmarkConfiguration, NexmarkPerf> actual = new LinkedHashMap<>();
    Iterable<NexmarkConfiguration> configurations =
        Sets.newLinkedHashSet(options.getSuite().getConfigurations(options));

    boolean successful = true;
    try {
      // Run all the configurations.
      for (NexmarkConfiguration configuration : configurations) {
        NexmarkPerf perf = runner.run(configuration);
        if (perf != null) {
          if (perf.errors == null || perf.errors.size() > 0) {
            successful = false;
          }
          appendPerf(options.getPerfFilename(), configuration, perf);
          actual.put(configuration, perf);
          // Summarize what we've run so far.
          saveSummary(null, configurations, actual, baseline, start);
        }
      }
    } finally {
      if (options.getMonitorJobs()) {
        // Report overall performance.
        saveSummary(options.getSummaryFilename(), configurations, actual, baseline, start);
        saveJavascript(options.getJavascriptFilename(), configurations, actual, baseline, start);
      }
    }

    System.exit(successful ? 0 : 1);
  }

  /**
   * Append the pair of {@code configuration} and {@code perf} to perf file.
   *
   * @throws IOException
   */
  private static void appendPerf(@Nullable String perfFilename, NexmarkConfiguration configuration,
      NexmarkPerf perf) throws IOException {
    if (perfFilename == null) {
      return;
    }
    List<String> lines = new ArrayList<>();
    lines.add("");
    lines.add(String.format("# %s", Instant.now()));
    lines.add(String.format("# %s", configuration.toShortString()));
    lines.add(configuration.toString());
    lines.add(perf.toString());
    Files.write(Paths.get(perfFilename), lines, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
        StandardOpenOption.APPEND);
    NexmarkUtils.console(null, "appended results to perf file %s.", perfFilename);
  }

  /**
   * Load the baseline perf.
   *
   * @throws IOException
   */
  @Nullable
  private static Map<NexmarkConfiguration, NexmarkPerf> loadBaseline(
      @Nullable String baselineFilename) throws IOException {
    if (baselineFilename == null) {
      return null;
    }
    Map<NexmarkConfiguration, NexmarkPerf> baseline = new LinkedHashMap<>();
    List<String> lines = Files.readAllLines(Paths.get(baselineFilename), StandardCharsets.UTF_8);
    for (int i = 0; i < lines.size(); i++) {
      if (lines.get(i).startsWith("#") || lines.get(i).trim().isEmpty()) {
        continue;
      }
      NexmarkConfiguration configuration = NexmarkConfiguration.fromString(lines.get(i++));
      NexmarkPerf perf = NexmarkPerf.fromString(lines.get(i));
      baseline.put(configuration, perf);
    }
    NexmarkUtils.console(
        null, "loaded %d entries from baseline file %s.", baseline.size(), baselineFilename);
    return baseline;
  }

  private static final String LINE =
      "==========================================================================================";

  /**
   * Print summary  of {@code actual} vs (if non-null) {@code baseline}.
   * @throws IOException
   */
  private static void saveSummary(@Nullable String summaryFilename,
      Iterable<NexmarkConfiguration> configurations, Map<NexmarkConfiguration, NexmarkPerf> actual,
      @Nullable Map<NexmarkConfiguration, NexmarkPerf> baseline, Instant start) throws IOException {
    List<String> lines = new ArrayList<>();

    lines.add("");
    lines.add(LINE);

    lines.add(
        String.format("Run started %s and ran for %s", start, new Duration(start, Instant.now())));
    lines.add("");

    lines.add("Default configuration:");
    lines.add(NexmarkConfiguration.DEFAULT.toString());
    lines.add("");

    lines.add("Configurations:");
    lines.add("  Conf  Description");
    int conf = 0;
    for (NexmarkConfiguration configuration : configurations) {
      lines.add(String.format("  %04d  %s", conf++, configuration.toShortString()));
      NexmarkPerf actualPerf = actual.get(configuration);
      if (actualPerf != null && actualPerf.jobId != null) {
        lines.add(String.format("  %4s  [Ran as job %s]", "", actualPerf.jobId));
      }
    }

    lines.add("");
    lines.add("Performance:");
    lines.add(String.format("  %4s  %12s  %12s  %12s  %12s  %12s  %12s", "Conf", "Runtime(sec)",
        "(Baseline)", "Events(/sec)", "(Baseline)", "Results", "(Baseline)"));
    conf = 0;
    for (NexmarkConfiguration configuration : configurations) {
      String line = String.format("  %04d  ", conf++);
      NexmarkPerf actualPerf = actual.get(configuration);
      if (actualPerf == null) {
        line += "*** not run ***";
      } else {
        NexmarkPerf baselinePerf = baseline == null ? null : baseline.get(configuration);
        double runtimeSec = actualPerf.runtimeSec;
        line += String.format("%12.1f  ", runtimeSec);
        if (baselinePerf == null) {
          line += String.format("%12s  ", "");
        } else {
          double baselineRuntimeSec = baselinePerf.runtimeSec;
          double diff = ((runtimeSec - baselineRuntimeSec) / baselineRuntimeSec) * 100.0;
          line += String.format("%+11.2f%%  ", diff);
        }

        double eventsPerSec = actualPerf.eventsPerSec;
        line += String.format("%12.1f  ", eventsPerSec);
        if (baselinePerf == null) {
          line += String.format("%12s  ", "");
        } else {
          double baselineEventsPerSec = baselinePerf.eventsPerSec;
          double diff = ((eventsPerSec - baselineEventsPerSec) / baselineEventsPerSec) * 100.0;
          line += String.format("%+11.2f%%  ", diff);
        }

        long numResults = actualPerf.numResults;
        line += String.format("%12d  ", numResults);
        if (baselinePerf == null) {
          line += String.format("%12s", "");
        } else {
          long baselineNumResults = baselinePerf.numResults;
          long diff = numResults - baselineNumResults;
          line += String.format("%+12d", diff);
        }
      }
      lines.add(line);

      if (actualPerf != null) {
        List<String> errors = actualPerf.errors;
        if (errors == null) {
          errors = new ArrayList<String>();
          errors.add("NexmarkRunner returned null errors list");
        }
        for (String error : errors) {
          lines.add(String.format("  %4s  *** %s ***", "", error));
        }
      }
    }

    lines.add(LINE);
    lines.add("");

    for (String line : lines) {
      System.out.println(line);
    }

    if (summaryFilename != null) {
      Files.write(Paths.get(summaryFilename), lines, StandardCharsets.UTF_8,
          StandardOpenOption.CREATE, StandardOpenOption.APPEND);
      NexmarkUtils.console(null, "appended summary to summary file %s.", summaryFilename);
    }
  }

  /**
   * Write all perf data and any baselines to a javascript file which can be used by
   * graphing page etc.
   */
  private static void saveJavascript(@Nullable String javascriptFilename,
      Iterable<NexmarkConfiguration> configurations, Map<NexmarkConfiguration, NexmarkPerf> actual,
      @Nullable Map<NexmarkConfiguration, NexmarkPerf> baseline, Instant start) throws IOException {
    if (javascriptFilename == null) {
      return;
    }

    List<String> lines = new ArrayList<>();
    lines.add(String.format(
        "// Run started %s and ran for %s", start, new Duration(start, Instant.now())));
    lines.add("var all = [");

    for (NexmarkConfiguration configuration : configurations) {
      lines.add("  {");
      lines.add(String.format("    config: %s", configuration));
      NexmarkPerf actualPerf = actual.get(configuration);
      if (actualPerf != null) {
        lines.add(String.format("    ,perf: %s", actualPerf));
      }
      NexmarkPerf baselinePerf = baseline == null ? null : baseline.get(configuration);
      if (baselinePerf != null) {
        lines.add(String.format("    ,baseline: %s", baselinePerf));
      }
      lines.add("  },");
    }

    lines.add("];");

    Files.write(Paths.get(javascriptFilename), lines, StandardCharsets.UTF_8,
        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    NexmarkUtils.console(null, "saved javascript to file %s.", javascriptFilename);
  }
}

