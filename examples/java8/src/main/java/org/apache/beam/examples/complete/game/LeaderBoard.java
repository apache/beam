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

import org.apache.beam.examples.common.DataflowExampleOptions;
import org.apache.beam.examples.common.DataflowExampleUtils;
import org.apache.beam.examples.complete.game.utils.WriteToBigQuery;
import org.apache.beam.examples.complete.game.utils.WriteWindowedToBigQuery;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
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

import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * This class is the third in a series of four pipelines that tell a story in a 'gaming' domain,
 * following {@link UserScore} and {@link HourlyTeamScore}. Concepts include: processing unbounded
 * data using fixed windows; use of custom timestamps and event-time processing; generation of
 * early/speculative results; using .accumulatingFiredPanes() to do cumulative processing of late-
 * arriving data.
 *
 * <p> This pipeline processes an unbounded stream of 'game events'. The calculation of the team
 * scores uses fixed windowing based on event time (the time of the game play event), not
 * processing time (the time that an event is processed by the pipeline). The pipeline calculates
 * the sum of scores per team, for each window. By default, the team scores are calculated using
 * one-hour windows.
 *
 * <p> In contrast-- to demo another windowing option-- the user scores are calculated using a
 * global window, which periodically (every ten minutes) emits cumulative user score sums.
 *
 * <p> In contrast to the previous pipelines in the series, which used static, finite input data,
 * here we're using an unbounded data source, which lets us provide speculative results, and allows
 * handling of late data, at much lower latency. We can use the early/speculative results to keep a
 * 'leaderboard' updated in near-realtime. Our handling of late data lets us generate correct
 * results, e.g. for 'team prizes'. We're now outputing window results as they're
 * calculated, giving us much lower latency than with the previous batch examples.
 *
 * <p> Run {@link injector.Injector} to generate pubsub data for this pipeline.  The Injector
 * documentation provides more detail on how to do this.
 *
 * <p> To execute this pipeline using the Dataflow service, specify the pipeline configuration
 * like this:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --tempLocation=gs://YOUR_TEMP_DIRECTORY
 *   --runner=BlockingDataflowRunner
 *   --dataset=YOUR-DATASET
 *   --topic=projects/YOUR-PROJECT/topics/YOUR-TOPIC
 * }
 * </pre>
 * where the BigQuery dataset you specify must already exist.
 * The PubSub topic you specify should be the same topic to which the Injector is publishing.
 */
public class LeaderBoard extends HourlyTeamScore {

  private static final String TIMESTAMP_ATTRIBUTE = "timestamp_ms";

  private static DateTimeFormatter fmt =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
          .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));
  static final Duration FIVE_MINUTES = Duration.standardMinutes(5);
  static final Duration TEN_MINUTES = Duration.standardMinutes(10);


  /**
   * Options supported by {@link LeaderBoard}.
   */
  static interface Options extends HourlyTeamScore.Options, DataflowExampleOptions {

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

    @Override
    @Description("Prefix used for the BigQuery table names")
    @Default.String("leaderboard")
    String getTableName();
    @Override
    void setTableName(String value);
  }

  /**
   * Create a map of information that describes how to write pipeline output to BigQuery. This map
   * is used to write team score sums and includes event timing information.
   */
  protected static Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>>
      configureWindowedTableWrite() {

    Map<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>> tableConfigure =
        new HashMap<String, WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>>();
    tableConfigure.put("team",
        new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>("STRING",
            c -> c.element().getKey()));
    tableConfigure.put("total_score",
        new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>("INTEGER",
            c -> c.element().getValue()));
    tableConfigure.put("window_start",
        new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>("STRING",
          c -> {
            IntervalWindow w = (IntervalWindow) c.window();
            return fmt.print(w.start());
          }));
    tableConfigure.put("processing_time",
        new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
            "STRING", c -> fmt.print(Instant.now())));
    tableConfigure.put("timing",
        new WriteWindowedToBigQuery.FieldInfo<KV<String, Integer>>(
            "STRING", c -> c.pane().getTiming().toString()));
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
    tableConfigure.put("processing_time",
        new WriteToBigQuery.FieldInfo<KV<String, Integer>>(
            "STRING", c -> fmt.print(Instant.now())));
    return tableConfigure;
  }


  public static void main(String[] args) throws Exception {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    // Enforce that this pipeline is always run in streaming mode.
    options.setStreaming(true);
    DataflowExampleUtils dataflowUtils = new DataflowExampleUtils(options);
    Pipeline pipeline = Pipeline.create(options);

    // Read game events from Pub/Sub using custom timestamps, which are extracted from the pubsub
    // data elements, and parse the data.
    PCollection<GameActionInfo> gameEvents = pipeline
        .apply(PubsubIO.Read.timestampLabel(TIMESTAMP_ATTRIBUTE).topic(options.getTopic()))
        .apply("ParseGameEvent", ParDo.of(new ParseEventFn()));

    // [START DocInclude_WindowAndTrigger]
    // Extract team/score pairs from the event stream, using hour-long windows by default.
    gameEvents
        .apply("LeaderboardTeamFixedWindows", Window.<GameActionInfo>into(
            FixedWindows.of(Duration.standardMinutes(options.getTeamWindowDuration())))
          // We will get early (speculative) results as well as cumulative
          // processing of late data.
          .triggering(
            AfterWatermark.pastEndOfWindow()
            .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                  .plusDelayOf(FIVE_MINUTES))
            .withLateFirings(AfterProcessingTime.pastFirstElementInPane()
                  .plusDelayOf(TEN_MINUTES)))
          .withAllowedLateness(Duration.standardMinutes(options.getAllowedLateness()))
          .accumulatingFiredPanes())
        // Extract and sum teamname/score pairs from the event data.
        .apply("ExtractTeamScore", new ExtractAndSumScore("team"))
        // Write the results to BigQuery.
        .apply("WriteTeamScoreSums",
               new WriteWindowedToBigQuery<KV<String, Integer>>(
                  options.getTableName() + "_team", configureWindowedTableWrite()));
    // [END DocInclude_WindowAndTrigger]

    // [START DocInclude_ProcTimeTrigger]
    // Extract user/score pairs from the event stream using processing time, via global windowing.
    // Get periodic updates on all users' running scores.
    gameEvents
        .apply("LeaderboardUserGlobalWindow", Window.<GameActionInfo>into(new GlobalWindows())
          // Get periodic results every ten minutes.
              .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                  .plusDelayOf(TEN_MINUTES)))
              .accumulatingFiredPanes()
              .withAllowedLateness(Duration.standardMinutes(options.getAllowedLateness())))
        // Extract and sum username/score pairs from the event data.
        .apply("ExtractUserScore", new ExtractAndSumScore("user"))
        // Write the results to BigQuery.
        .apply("WriteUserScoreSums",
               new WriteToBigQuery<KV<String, Integer>>(
                  options.getTableName() + "_user", configureGlobalWindowBigQueryWrite()));
    // [END DocInclude_ProcTimeTrigger]

    // Run the pipeline and wait for the pipeline to finish; capture cancellation requests from the
    // command line.
    PipelineResult result = pipeline.run();
    dataflowUtils.waitToFinish(result);
  }
}
