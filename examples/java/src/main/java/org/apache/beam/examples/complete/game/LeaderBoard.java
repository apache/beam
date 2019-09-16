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
package org.apache.beam.examples.complete.game;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.examples.common.ExampleOptions;
import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.examples.complete.game.utils.GameConstants;
import org.apache.beam.examples.complete.game.utils.WriteToBigQuery;
import org.apache.beam.examples.complete.game.utils.WriteWindowedToBigQuery;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * This class is the third in a series of four pipelines that tell a story in a 'gaming' domain,
 * following {@link UserScore} and {@link HourlyTeamScore}. Concepts include: processing unbounded
 * data using fixed windows; use of custom timestamps and event-time processing; generation of
 * early/speculative results; using .accumulatingFiredPanes() to do cumulative processing of late-
 * arriving data.
 *
 * <p>This pipeline processes an unbounded stream of 'game events'. The calculation of the team
 * scores uses fixed windowing based on event time (the time of the game play event), not processing
 * time (the time that an event is processed by the pipeline). The pipeline calculates the sum of
 * scores per team, for each window. By default, the team scores are calculated using one-hour
 * windows.
 *
 * <p>In contrast-- to demo another windowing option-- the user scores are calculated using a global
 * window, which periodically (every ten minutes) emits cumulative user score sums.
 *
 * <p>In contrast to the previous pipelines in the series, which used static, finite input data,
 * here we're using an unbounded data source, which lets us provide speculative results, and allows
 * handling of late data, at much lower latency. We can use the early/speculative results to keep a
 * 'leaderboard' updated in near-realtime. Our handling of late data lets us generate correct
 * results, e.g. for 'team prizes'. We're now outputting window results as they're calculated,
 * giving us much lower latency than with the previous batch examples.
 *
 * <p>Run {@code injector.Injector} to generate pubsub data for this pipeline. The Injector
 * documentation provides more detail on how to do this.
 *
 * <p>To execute this pipeline, specify the pipeline configuration like this:
 *
 * <pre>{@code
 * --project=YOUR_PROJECT_ID
 * --tempLocation=gs://YOUR_TEMP_DIRECTORY
 * --runner=YOUR_RUNNER
 * --dataset=YOUR-DATASET
 * --topic=projects/YOUR-PROJECT/topics/YOUR-TOPIC
 * }</pre>
 *
 * <p>The BigQuery dataset you specify must already exist. The PubSub topic you specify should be
 * the same topic to which the Injector is publishing.
 */
public class LeaderBoard extends HourlyTeamScore {

  static final Duration FIVE_MINUTES = Duration.standardMinutes(5);
  static final Duration TEN_MINUTES = Duration.standardMinutes(10);

  /** Options supported by {@link LeaderBoard}. */
  public interface Options extends ExampleOptions, StreamingOptions {

    @Description("BigQuery Dataset to write tables to. Must already exist.")
    @Validation.Required
    String getDataset();

    void setDataset(String value);

    @Description("Pub/Sub topic to read from")
    @Validation.Required
    String getTopic();

    void setTopic(String value);

    @Description("Numeric value of fixed window duration for team analysis, in minutes")
    @Default.Integer(60)
    Integer getTeamWindowDuration();

    void setTeamWindowDuration(Integer value);

    @Description("Numeric value of allowed data lateness, in minutes")
    @Default.Integer(120)
    Integer getAllowedLateness();

    void setAllowedLateness(Integer value);

    @Description("Prefix used for the BigQuery table names")
    @Default.String("leaderboard")
    String getLeaderBoardTableName();

    void setLeaderBoardTableName(String value);
  }

  /**
   * Create a map of information that describes how to write pipeline output to BigQuery. This map
   * is used to write team score sums and includes event timing information.
   */
  protected static Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>>
      configureWindowedTableWrite() {

    Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>> tableConfigure =
        new HashMap<>();
    tableConfigure.put(
        "team", new WriteWindowedToBigQuery.FieldInfo<>("STRING", (c, w) -> c.element().getKey()));
    tableConfigure.put(
        "total_score",
        new WriteWindowedToBigQuery.FieldInfo<>("INTEGER", (c, w) -> c.element().getValue()));
    tableConfigure.put(
        "window_start",
        new WriteWindowedToBigQuery.FieldInfo<>(
            "STRING",
            (c, w) -> {
              IntervalWindow window = (IntervalWindow) w;
              return GameConstants.DATE_TIME_FORMATTER.print(window.start());
            }));
    tableConfigure.put(
        "processing_time",
        new WriteWindowedToBigQuery.FieldInfo<>(
            "STRING", (c, w) -> GameConstants.DATE_TIME_FORMATTER.print(Instant.now())));
    tableConfigure.put(
        "timing",
        new WriteWindowedToBigQuery.FieldInfo<>(
            "STRING", (c, w) -> c.pane().getTiming().toString()));
    return tableConfigure;
  }

  /**
   * Create a map of information that describes how to write pipeline output to BigQuery. This map
   * is passed to the {@link WriteToBigQuery} constructor to write user score sums.
   */
  protected static Map<String, WriteToBigQuery.FieldInfo<KV<String, Integer>>>
      configureBigQueryWrite() {
    Map<String, WriteToBigQuery.FieldInfo<KV<String, Integer>>> tableConfigure = new HashMap<>();
    tableConfigure.put(
        "user", new WriteToBigQuery.FieldInfo<>("STRING", (c, w) -> c.element().getKey()));
    tableConfigure.put(
        "total_score",
        new WriteToBigQuery.FieldInfo<>("INTEGER", (c, w) -> c.element().getValue()));
    return tableConfigure;
  }

  /**
   * Create a map of information that describes how to write pipeline output to BigQuery. This map
   * is used to write user score sums.
   */
  protected static Map<String, WriteToBigQuery.FieldInfo<KV<String, Integer>>>
      configureGlobalWindowBigQueryWrite() {

    Map<String, WriteToBigQuery.FieldInfo<KV<String, Integer>>> tableConfigure =
        configureBigQueryWrite();
    tableConfigure.put(
        "processing_time",
        new WriteToBigQuery.FieldInfo<>(
            "STRING", (c, w) -> GameConstants.DATE_TIME_FORMATTER.print(Instant.now())));
    return tableConfigure;
  }

  public static void main(String[] args) throws Exception {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    // Enforce that this pipeline is always run in streaming mode.
    options.setStreaming(true);
    ExampleUtils exampleUtils = new ExampleUtils(options);
    Pipeline pipeline = Pipeline.create(options);

    // Read game events from Pub/Sub using custom timestamps, which are extracted from the pubsub
    // data elements, and parse the data.
    PCollection<GameActionInfo> gameEvents =
        pipeline
            .apply(
                PubsubIO.readStrings()
                    .withTimestampAttribute(GameConstants.TIMESTAMP_ATTRIBUTE)
                    .fromTopic(options.getTopic()))
            .apply("ParseGameEvent", ParDo.of(new ParseEventFn()));

    gameEvents
        .apply(
            "CalculateTeamScores",
            new CalculateTeamScores(
                Duration.standardMinutes(options.getTeamWindowDuration()),
                Duration.standardMinutes(options.getAllowedLateness())))
        // Write the results to BigQuery.
        .apply(
            "WriteTeamScoreSums",
            new WriteWindowedToBigQuery<>(
                options.as(GcpOptions.class).getProject(),
                options.getDataset(),
                options.getLeaderBoardTableName() + "_team",
                configureWindowedTableWrite()));
    gameEvents
        .apply(
            "CalculateUserScores",
            new CalculateUserScores(Duration.standardMinutes(options.getAllowedLateness())))
        // Write the results to BigQuery.
        .apply(
            "WriteUserScoreSums",
            new WriteToBigQuery<>(
                options.as(GcpOptions.class).getProject(),
                options.getDataset(),
                options.getLeaderBoardTableName() + "_user",
                configureGlobalWindowBigQueryWrite()));

    // Run the pipeline and wait for the pipeline to finish; capture cancellation requests from the
    // command line.
    PipelineResult result = pipeline.run();
    exampleUtils.waitToFinish(result);
  }

  /** Calculates scores for each team within the configured window duration. */
  // [START DocInclude_WindowAndTrigger]
  // Extract team/score pairs from the event stream, using hour-long windows by default.
  @VisibleForTesting
  static class CalculateTeamScores
      extends PTransform<PCollection<GameActionInfo>, PCollection<KV<String, Integer>>> {
    private final Duration teamWindowDuration;
    private final Duration allowedLateness;

    CalculateTeamScores(Duration teamWindowDuration, Duration allowedLateness) {
      this.teamWindowDuration = teamWindowDuration;
      this.allowedLateness = allowedLateness;
    }

    @Override
    public PCollection<KV<String, Integer>> expand(PCollection<GameActionInfo> infos) {
      return infos
          .apply(
              "LeaderboardTeamFixedWindows",
              Window.<GameActionInfo>into(FixedWindows.of(teamWindowDuration))
                  // We will get early (speculative) results as well as cumulative
                  // processing of late data.
                  .triggering(
                      AfterWatermark.pastEndOfWindow()
                          .withEarlyFirings(
                              AfterProcessingTime.pastFirstElementInPane()
                                  .plusDelayOf(FIVE_MINUTES))
                          .withLateFirings(
                              AfterProcessingTime.pastFirstElementInPane()
                                  .plusDelayOf(TEN_MINUTES)))
                  .withAllowedLateness(allowedLateness)
                  .accumulatingFiredPanes())
          // Extract and sum teamname/score pairs from the event data.
          .apply("ExtractTeamScore", new ExtractAndSumScore("team"));
    }
  }
  // [END DocInclude_WindowAndTrigger]

  // [START DocInclude_ProcTimeTrigger]
  /**
   * Extract user/score pairs from the event stream using processing time, via global windowing. Get
   * periodic updates on all users' running scores.
   */
  @VisibleForTesting
  static class CalculateUserScores
      extends PTransform<PCollection<GameActionInfo>, PCollection<KV<String, Integer>>> {
    private final Duration allowedLateness;

    CalculateUserScores(Duration allowedLateness) {
      this.allowedLateness = allowedLateness;
    }

    @Override
    public PCollection<KV<String, Integer>> expand(PCollection<GameActionInfo> input) {
      return input
          .apply(
              "LeaderboardUserGlobalWindow",
              Window.<GameActionInfo>into(new GlobalWindows())
                  // Get periodic results every ten minutes.
                  .triggering(
                      Repeatedly.forever(
                          AfterProcessingTime.pastFirstElementInPane().plusDelayOf(TEN_MINUTES)))
                  .accumulatingFiredPanes()
                  .withAllowedLateness(allowedLateness))
          // Extract and sum username/score pairs from the event data.
          .apply("ExtractUserScore", new ExtractAndSumScore("user"));
    }
  }
  // [END DocInclude_ProcTimeTrigger]
}
