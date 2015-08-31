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

package com.google.cloud.dataflow.examples.complete.game;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.examples.common.DataflowExampleOptions;
import com.google.cloud.dataflow.examples.common.DataflowExampleUtils;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.StreamingOptions;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterEach;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterProcessingTime;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterWatermark;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Repeatedly;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;


import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.List;
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
 *   --stagingLocation=gs://YOUR_STAGING_DIRECTORY
 *   --runner=BlockingDataflowPipelineRunner
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
   * Format information for scores, and write that info to BigQuery.
   * Optionally include fixed windowing information and timing in the result.
   */
  public static class WriteScoresToBigQuery
      extends PTransform<PCollection<KV<String, Integer>>, PDone> {

    private final String fieldName;
    private final String tablePrefix;
    private final boolean writeTiming; // Whether to write timing info to the resultant table.
    private final boolean writeWindowStart; // whether to include window start info.

    public WriteScoresToBigQuery(String tablePrefix, String fieldName,
        boolean writeWindowStart, boolean writeTiming) {
      this.fieldName = fieldName;
      this.tablePrefix = tablePrefix;
      this.writeWindowStart = writeWindowStart;
      this.writeTiming = writeTiming;
    }

    /** Convert each key/score pair into a BigQuery TableRow. */
    private class BuildFixedRowFn extends DoFn<KV<String, Integer>, TableRow>
        implements RequiresWindowAccess {

      @Override
      public void processElement(ProcessContext c) {

        // IntervalWindow w = (IntervalWindow) c.window();

        TableRow row = new TableRow()
          .set(fieldName, c.element().getKey())
          .set("total_score", c.element().getValue().longValue())
          .set("processing_time", fmt.print(Instant.now()));
         if (writeWindowStart) {
          IntervalWindow w = (IntervalWindow) c.window();
          row.set("window_start", fmt.print(w.start()));
         }
         if (writeTiming) {
          row.set("timing", c.pane().getTiming().toString());
         }
        c.output(row);
      }
    }

    /** Build the output table schema. */
    private TableSchema getFixedSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName(fieldName).setType("STRING"));
      fields.add(new TableFieldSchema().setName("total_score").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("processing_time").setType("STRING"));
      if (writeWindowStart) {
        fields.add(new TableFieldSchema().setName("window_start").setType("STRING"));
      }
      if (writeTiming) {
        fields.add(new TableFieldSchema().setName("timing").setType("STRING"));
      }
      return new TableSchema().setFields(fields);
    }

    @Override
    public PDone apply(PCollection<KV<String, Integer>> teamAndScore) {
      return teamAndScore
        .apply(ParDo.named("ConvertToFixedTriggersRow").of(new BuildFixedRowFn()))
        .apply(BigQueryIO.Write
                  .to(getTable(teamAndScore.getPipeline(),
                      tablePrefix + "_" + fieldName))
                  .withSchema(getFixedSchema())
                  .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));
    }
  }


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

    @Description("Prefix used for the BigQuery table names")
    @Default.String("leaderboard")
    String getTableName();
    void setTableName(String value);
  }


  public static void main(String[] args) throws Exception {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    // Enforce that this pipeline is always run in streaming mode.
    options.setStreaming(true);
    // For example purposes, allow the pipeline to be easily cancelled instead of running
    // continuously.
    options.setRunner(DataflowPipelineRunner.class);
    DataflowExampleUtils dataflowUtils = new DataflowExampleUtils(options);
    Pipeline pipeline = Pipeline.create(options);

    // Read game events from Pub/Sub using custom timestamps, which are extracted from the pubsub
    // data elements, and parse the data.
    PCollection<GameActionInfo> gameEvents = pipeline
        .apply(PubsubIO.Read.timestampLabel(TIMESTAMP_ATTRIBUTE).topic(options.getTopic()))
        .apply(ParDo.named("ParseGameEvent").of(new ParseEventFn()));

    // Extract team/score pairs from the event stream, using hour-long windows by default.
    gameEvents
        .apply(Window.named("LeaderboardTeamFixedWindows")
          .<GameActionInfo>into(FixedWindows.of(
              Duration.standardMinutes(options.getTeamWindowDuration())))
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
               new WriteScoresToBigQuery(options.getTableName(), "team", true, true));

    // Extract user/score pairs from the event stream using processing time, via global windowing.
    // Get periodic updates on all users' running scores.
    gameEvents
        .apply(Window.named("LeaderboardUserGlobalWindow")
          .<GameActionInfo>into(new GlobalWindows())
          // Get periodic results every ten minutes.
              .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                  .plusDelayOf(TEN_MINUTES)))
              .accumulatingFiredPanes()
              .withAllowedLateness(Duration.standardMinutes(options.getAllowedLateness())))
        // Extract and sum username/score pairs from the event data.
        .apply("ExtractUserScore", new ExtractAndSumScore("user"))
        // Write the results to BigQuery.
        .apply("WriteUserScoreSums",
               new WriteScoresToBigQuery(options.getTableName(), "user", false, false));

    // Run the pipeline and wait for the pipeline to finish; capture cancellation requests from the
    // command line.
    PipelineResult result = pipeline.run();
    dataflowUtils.waitToFinish(result);
  }
}
