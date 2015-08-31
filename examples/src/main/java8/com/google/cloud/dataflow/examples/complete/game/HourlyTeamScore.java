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
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.WithTimestamps;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
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
 * This class is the second in a series of four pipelines that tell a story in a 'gaming'
 * domain, following {@link UserScore}. In addition to the concepts introduced in {@ UserScore}, new
 * concepts include: windowing and element timestamps; use of {@code Filter.byPredicate()}.
 *
 * <p> This pipeline processes data collected from gaming events in batch, building on {@link
 * UserScore} but using fixed windows. It calculates the sum of scores per team, for each window,
 * optionally allowing specification of two timestamps before and after which data is filtered out.
 * This allows a model where late data collected after the intended analysis window can be included,
 * and any late-arriving data prior to the beginning of the analysis window can be removed as well.
 * By using windowing and adding element timestamps, we can do finer-grained analysis than with the
 * {@link UserScore} pipeline. However, our batch processing is high-latency, in that we don't get
 * results from plays at the beginning of the batch's time period until the batch is processed.
 *
 * <p> To execute this pipeline using the Dataflow service, specify the pipeline configuration
 * like this:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --stagingLocation=gs://YOUR_STAGING_DIRECTORY
 *   --runner=BlockingDataflowPipelineRunner
 *   --dataset=YOUR-DATASET
 * }
 * </pre>
 * where the BigQuery dataset you specify must already exist.
 *
 * <p> Optionally include {@code --input} to specify the batch input file path.
 * To indicate a time after which the data should be filtered out, include the
 * {@code --stopMin} arg. E.g., {@code --stopMin=2015-10-18-23-59} indicates that any data
 * timestamped after 23:59 PST on 2015-10-18 should not be included in the analysis.
 * To indicate a time before which data should be filtered out, include the {@code --startMin} arg.
 * If you're using the default input specified in {@link UserScore},
 * "gs://dataflow-samples/game/gaming_data*.csv", then
 * {@code --startMin=2015-11-16-16-10 --stopMin=2015-11-17-16-10} are good values.
 */
public class HourlyTeamScore extends UserScore {

  private static DateTimeFormatter fmt =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
          .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));
  private static DateTimeFormatter minFmt =
      DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm")
          .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("PST")));


  /** Format fixed window information for scores, and write that info to BigQuery. */
  public static class WriteWindowedToBigQuery
      extends PTransform<PCollection<KV<String, Integer>>, PDone> {

    private final String tableName;

    public WriteWindowedToBigQuery(String tableName) {
      this.tableName = tableName;
    }

    /** Convert each key/score pair into a BigQuery TableRow. */
    private class BuildFixedRowFn extends DoFn<KV<String, Integer>, TableRow>
        implements RequiresWindowAccess {

      @Override
      public void processElement(ProcessContext c) {

        IntervalWindow w = (IntervalWindow) c.window();

        TableRow row = new TableRow()
         .set("team", c.element().getKey())
         .set("total_score", c.element().getValue().longValue())
         // Add windowing info to the output.
         .set("window_start", fmt.print(w.start()));
        c.output(row);
      }
    }

    /** Build the output table schema. */
    private TableSchema getFixedSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("team").setType("STRING"));
      fields.add(new TableFieldSchema().setName("total_score").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("window_start").setType("STRING"));
      return new TableSchema().setFields(fields);
    }

    @Override
    public PDone apply(PCollection<KV<String, Integer>> teamAndScore) {
      return teamAndScore
        .apply(ParDo.named("ConvertToFixedRow").of(new BuildFixedRowFn()))
        .apply(BigQueryIO.Write
                  .to(getTable(teamAndScore.getPipeline(),
                      tableName))
                  .withSchema(getFixedSchema())
                  .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));
    }
  }


  /**
   * Options supported by {@link HourlyTeamScore}.
   */
  static interface Options extends UserScore.Options {

    @Description("Numeric value of fixed window duration, in minutes")
    @Default.Integer(60)
    Integer getWindowDuration();
    void setWindowDuration(Integer value);

    @Description("String representation of the first minute after which to generate results,"
        + "in the format: yyyy-MM-dd-HH-mm . This time should be in PST."
        + "Any input data timestamped prior to that minute won't be included in the sums.")
    @Default.String("1970-01-01-00-00")
    String getStartMin();
    void setStartMin(String value);

    @Description("String representation of the first minute for which to not generate results,"
        + "in the format: yyyy-MM-dd-HH-mm . This time should be in PST."
        + "Any input data timestamped after that minute won't be included in the sums.")
    @Default.String("2100-01-01-00-00")
    String getStopMin();
    void setStopMin(String value);

    @Description("The BigQuery table name. Should not already exist.")
    @Default.String("hourly_team_score")
    String getTableName();
    void setTableName(String value);
  }


  /**
   * Run a batch pipeline to do windowed analysis of the data.
   */
  public static void main(String[] args) throws Exception {
    // Begin constructing a pipeline configured by commandline flags.
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    final Instant stopMinTimestamp = new Instant(minFmt.parseMillis(options.getStopMin()));
    final Instant startMinTimestamp = new Instant(minFmt.parseMillis(options.getStartMin()));

    // Read 'gaming' events from a text file.
    pipeline.apply(TextIO.Read.from(options.getInput()))
      // Parse the incoming data.
      .apply(ParDo.named("ParseGameEvent").of(new ParseEventFn()))

      // Filter out data before and after the given times so that it is not included
      // in the calculations. As we collect data in batches (say, by day), the batch for the day
      // that we want to analyze could potentially include some late-arriving data from the previous
      // day. If so, we want to weed it out. Similarly, if we include data from the following day
      // (to scoop up late-arriving events from the day we're analyzing), we need to weed out events
      // that fall after the time period we want to analyze.
      .apply("FilterStartTime", Filter.byPredicate(
          (GameActionInfo gInfo)
              -> gInfo.getTimestamp() > startMinTimestamp.getMillis()))
      .apply("FilterEndTime", Filter.byPredicate(
          (GameActionInfo gInfo)
              -> gInfo.getTimestamp() < stopMinTimestamp.getMillis()))

      // Add an element timestamp based on the event log, and apply fixed windowing.
      .apply("AddEventTimestamps",
             WithTimestamps.of((GameActionInfo i) -> new Instant(i.getTimestamp())))
      .apply(Window.named("FixedWindowsTeam")
          .<GameActionInfo>into(FixedWindows.of(
                Duration.standardMinutes(options.getWindowDuration()))))

      // Extract and sum teamname/score pairs from the event data.
      .apply("ExtractTeamScore", new ExtractAndSumScore("team"))
      .apply("WriteTeamScoreSums", new WriteWindowedToBigQuery(options.getTableName()));

    pipeline.run();
  }

}
