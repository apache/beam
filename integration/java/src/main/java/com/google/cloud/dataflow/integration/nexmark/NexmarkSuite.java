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

import com.google.cloud.dataflow.integration.nexmark.NexmarkDriver.Options;
import com.google.cloud.dataflow.integration.nexmark.NexmarkUtils.SinkType;
import com.google.cloud.dataflow.integration.nexmark.NexmarkUtils.SourceType;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineWorkerPoolOptions;

import java.util.ArrayList;
import java.util.List;

/**
 * A set of {@link NexmarkConfiguration}s.
 */
public enum NexmarkSuite {
  /** The default. isStreaming controlled from command line. */
  DEFAULT(true, defaultConf()),

  /** Sweep through all 10 queries with both streaming and batch using the default
   * configuration. That means one worker, 100k/10k events (depending on query). */
  SMOKE(false, smoke()),

  /**
   * As for SMOKE, but with 10m/1m events and 5 workers.
   */
  STRESS(false, stress()),

  /**
   * As for SMOKE, but with 1b/100m events and 25 workers.
   */
  FULL_THROTTLE(false, fullThrottle()),

  /**
   * Assert correspondence of actual against model for all queries in both
   * batch and streaming. Only 2000 events.
   */
  CORRECT(false, correct()),

  /**
   * Calculate expected distribution of number of results per minute for all queries.
   * Does not run any pipelines.
   */
  JUST_MODEL_RESULT_RATE(false, justModelResultRate()),

  /**
   * Compare coders on query 0 in batch and streaming. 1m events and 1 worker.
   */
  CODERS(false, coders()),

  /**
   * Query 10.
   */
  BIG_LOGGER(false, bigLogger()),

  /**
   * Query 10, but slow and small for debugging.
   */
  SMALL_LOGGER(false, smallLogger()),

  /**
   * Query 10, at high volume with no autoscaling.
   */
  LONG_RUNNING_LOGGER(false, longRunningLogger()),

  /**
   * Query 11 in streaming for 100b events to check for progressive slowdown.
   */
  LONG_RUNNING_SESSIONS(false, longRunningSessions()),

  /**
   * High-volume BigQuery output.
   **/
  LONG_RUNNING_BIGQUERY(false, longRunningBigQuery());

  private static List<NexmarkConfiguration> defaultConf() {
    List<NexmarkConfiguration> configurations = new ArrayList<>();
    configurations.add(new NexmarkConfiguration());
    return configurations;
  }

  private static List<NexmarkConfiguration> smoke() {
    List<NexmarkConfiguration> configurations = new ArrayList<>();
    for (int query = 0; query <= 10; query++) {
      for (int streaming = 0; streaming <= 1; streaming++) {
        NexmarkConfiguration configuration = NexmarkConfiguration.DEFAULT.clone();
        configuration.query = query;
        configuration.streaming = streaming > 0;
        configuration.numEvents = 100_000;
        if (query == 7) {
          // ######################################################################
          // Currently broken. b/22932773.
          // ######################################################################
          configuration.numEvents = -1;
        } else if (query == 4 || query == 6 || query == 9) {
          // Scale back so overall runtimes are reasonably close across all queries.
          configuration.numEvents /= 10;
        }
        configurations.add(configuration);
      }
    }
    return configurations;
  }

  private static List<NexmarkConfiguration> stress() {
    List<NexmarkConfiguration> configurations = smoke();
    for (NexmarkConfiguration configuration : configurations) {
      if (configuration.numEvents >= 0) {
        configuration.numEvents *= 1000;
      }
      configuration.numWorkers *= 5;
      configuration.maxNumWorkers *= 5;
    }
    return configurations;
  }

  private static List<NexmarkConfiguration> fullThrottle() {
    List<NexmarkConfiguration> configurations = smoke();
    for (NexmarkConfiguration configuration : configurations) {
      if (configuration.numEvents >= 0) {
        configuration.numEvents *= 1000;
      }
      configuration.numWorkers *= 25;
      configuration.maxNumWorkers *= 25;
    }
    return configurations;
  }

  private static List<NexmarkConfiguration> correct() {
    List<NexmarkConfiguration> configurations = smoke();
    for (NexmarkConfiguration configuration : configurations) {
      if (configuration.numEvents >= 0) {
        configuration.numEvents = 2_000L;
      }
      configuration.assertCorrectness = true;
      configuration.logResults = true;
    }
    return configurations;
  }

  private static List<NexmarkConfiguration> justModelResultRate() {
    List<NexmarkConfiguration> configurations = new ArrayList<>();
    for (int query = 0; query <= 10; query++) {
      if (query == 10) {
        // No model for Query 10.
        continue;
      }
      NexmarkConfiguration configuration = NexmarkConfiguration.DEFAULT.clone();
      configuration.query = query;
      configuration.justModelResultRate = true;
      configurations.add(configuration);
    }
    return configurations;
  }

  private static List<NexmarkConfiguration> coders() {
    List<NexmarkConfiguration> configurations = new ArrayList<>();
    for (int streaming = 0; streaming <= 1; streaming++) {
      for (NexmarkUtils.CoderStrategy coderStrategy : NexmarkUtils.CoderStrategy.values()) {
        NexmarkConfiguration configuration = NexmarkConfiguration.DEFAULT.clone();
        configuration.query = 0;
        configuration.streaming = streaming > 0;
        configuration.numEvents = 1_000_000L;
        configuration.coderStrategy = coderStrategy;
        configurations.add(configuration);
      }
    }
    return configurations;
  }

  private static List<NexmarkConfiguration> bigLogger() {
    NexmarkConfiguration configuration = NexmarkConfiguration.DEFAULT.clone();
    int totalCores = 400;
    int generatorCores = 24;
    int queryCores = totalCores - generatorCores;
    int maxDisksPerWorker = 15;
    configuration.maxNumWorkers = queryCores / configuration.coresPerWorker();
    configuration.numWorkers =
        Math.max(1, (configuration.maxNumWorkers + maxDisksPerWorker - 1) / maxDisksPerWorker);
    configuration.numEventGenerators = generatorCores;

    configuration.query = 10;
    configuration.streaming = true;
    configuration.isRateLimited = true;
    configuration.experiments.add("enable_ppm");
    configuration.experiments.add("enable_streaming_scaling");
    configuration.sourceType = SourceType.PUBSUB;
    configuration.autoscalingAlgorithm =
        DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.THROUGHPUT_BASED;
    configuration.numEvents = 0; // as many as possible without overflow.
    configuration.numPreloadEvents = 1_000_000L;
    configuration.avgPersonByteSize = 500;
    configuration.avgAuctionByteSize = 500;
    configuration.avgBidByteSize = 500;
    configuration.windowSizeSec = 60 * 20;
    configuration.occasionalDelaySec = 60 * 60;
    configuration.probDelayedEvent = 0.000001;
    // Due to poor interaction of pub/sub and custom timestamps we need to make
    // event timestamps as close as possible to wallclock time.
    configuration.useWallclockEventTime = true;
    configuration.qpsShape = NexmarkUtils.QpsShape.SINE;
    configuration.firstEventQps = 100_000;
    configuration.nextEventQps = 5_000;
    configuration.qpsPeriodSec = 60 * 60;
    configuration.preloadEventQps = 100_000;
    // At 100k qps and 94 workers (ie 188 log shards) a pane will fill every 16 mins.
    configuration.maxLogEvents = 500_000;

    configuration.usePubsubPublishTime = false;

    List<NexmarkConfiguration> configurations = new ArrayList<>();
    configurations.add(configuration);
    return configurations;
  }

  private static List<NexmarkConfiguration> smallLogger() {
    NexmarkConfiguration configuration = NexmarkConfiguration.DEFAULT.clone();
    configuration.maxNumWorkers = 1;
    configuration.numWorkers = 1;
    configuration.numEventGenerators = 1;

    configuration.query = 10;
    configuration.streaming = true;
    configuration.isRateLimited = true;
    configuration.experiments.add("enable_ppm");
    configuration.experiments.add("enable_streaming_scaling");
    configuration.sourceType = SourceType.PUBSUB;
    configuration.autoscalingAlgorithm =
        DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.NONE;
    configuration.numEvents = 0; // as many as possible without overflow.
    configuration.numPreloadEvents = 0L;
    configuration.avgPersonByteSize = 500;
    configuration.avgAuctionByteSize = 500;
    configuration.avgBidByteSize = 500;
    configuration.windowSizeSec = 30;
    configuration.occasionalDelaySec = 360;
    configuration.probDelayedEvent = 0.001;
    configuration.useWallclockEventTime = true;
    configuration.firstEventQps = 100;
    configuration.nextEventQps = 100;
    configuration.maxLogEvents = 15000;

    List<NexmarkConfiguration> configurations = new ArrayList<>();
    configurations.add(configuration);
    return configurations;
  }

  private static List<NexmarkConfiguration> longRunningSessions() {
    NexmarkConfiguration configuration = NexmarkConfiguration.DEFAULT.clone();
    configuration.maxNumWorkers = 25;
    configuration.numWorkers = 25;
    configuration.numEventGenerators = 25;

    configuration.query = 11;
    configuration.sourceType = SourceType.PUBSUB;
    configuration.autoscalingAlgorithm =
        DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.NONE;
    configuration.streaming = true;
    configuration.isRateLimited = true;
    configuration.firstEventQps = 50_000;
    configuration.nextEventQps = 25_000;
    configuration.numEvents = 0L;
    configuration.outOfOrderGroupSize = 10_000L;
    configuration.occasionalDelaySec = 60;
    configuration.probDelayedEvent = 0.00001;
    configuration.numInFlightAuctions = 1000;
    configuration.windowSizeSec = 5;  // 5sec session gap.
    configuration.maxLogEvents = 1000; // break sessions at around 1000 events.

    List<NexmarkConfiguration> configurations = new ArrayList<>();
    configurations.add(configuration);
    return configurations;
  }

  private static List<NexmarkConfiguration> longRunningLogger() {
    NexmarkConfiguration configuration = NexmarkConfiguration.DEFAULT.clone();
    configuration.maxNumWorkers = 25;
    configuration.numWorkers = 25;
    configuration.numEventGenerators = 10;

    configuration.query = 10;
    configuration.streaming = true;
    configuration.isRateLimited = true;
    configuration.sourceType = SourceType.PUBSUB;
    configuration.numEvents = 0; // as many as possible without overflow.
    configuration.numPreloadEvents = 0L;
    configuration.avgPersonByteSize = 500;
    configuration.avgAuctionByteSize = 500;
    configuration.avgBidByteSize = 500;
    configuration.windowSizeSec = 300;
    configuration.occasionalDelaySec = 360;
    configuration.probDelayedEvent = 0.001;
    configuration.useWallclockEventTime = true;
    configuration.firstEventQps = 30000;
    configuration.nextEventQps = 30000;
    configuration.maxLogEvents = 15000;

    List<NexmarkConfiguration> configurations = new ArrayList<>();
    configurations.add(configuration);
    return configurations;
  }

  private static List<NexmarkConfiguration> longRunningBigQuery() {
    NexmarkConfiguration configuration = NexmarkConfiguration.DEFAULT.clone();
    configuration.maxNumWorkers = 25;
    configuration.numWorkers = 25;
    configuration.numEventGenerators = 10;

    configuration.query = 0;
    configuration.streaming = true;
    configuration.isRateLimited = true;
    configuration.sourceType = SourceType.PUBSUB;
    configuration.sinkType = SinkType.BIGQUERY;
    configuration.numEvents = 0; // as many as possible without overflow.
    configuration.numPreloadEvents = 0L;
    configuration.avgPersonByteSize = 500;
    configuration.avgAuctionByteSize = 500;
    configuration.avgBidByteSize = 500;
    configuration.windowSizeSec = 300;
    configuration.probDelayedEvent = 0;
    configuration.useWallclockEventTime = true;
    configuration.firstEventQps = 60000;
    configuration.nextEventQps = 60000;

    List<NexmarkConfiguration> configurations = new ArrayList<>();
    configurations.add(configuration);
    return configurations;
  }

  private final boolean takeIsStreamingFromCommandLine;
  private final List<NexmarkConfiguration> configurations;

  private NexmarkSuite(
      boolean takeIsStreamingFromCommandLine, List<NexmarkConfiguration> configurations) {
    this.takeIsStreamingFromCommandLine = takeIsStreamingFromCommandLine;
    this.configurations = configurations;
  }

  /**
   * Return the configurations corresponding to this suite. We'll override each configuration
   * with any set command line flags, except for --isStreaming which is only respected for
   * the {@link #DEFAULT} suite.
   */
  public Iterable<NexmarkConfiguration> getConfigurations(Options options) {
    List<NexmarkConfiguration> results = new ArrayList<>();
    for (NexmarkConfiguration configuration : configurations) {
      NexmarkConfiguration result = configuration.clone();
      if (takeIsStreamingFromCommandLine) {
        // The --isStreaming flag is the ONLY one we can't distinguish between not-set
        // and set to false. So only copy its value in a few special cases.
        result.streaming = options.isStreaming();
      }
      result.overrideFromOptions(options);
      results.add(result);
    }
    return results;
  }
}
