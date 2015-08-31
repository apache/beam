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
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import org.apache.avro.reflect.Nullable;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

/**
 * This class is the first in a series of four pipelines that tell a story in a 'gaming' domain.
 * Concepts: batch processing; reading input from Google Cloud Storage and writing output to
 * BigQuery; using standalone DoFns; use of the sum by key transform; examples of
 * Java 8 lambda syntax.
 *
 * <p> In this gaming scenario, many users play, as members of different teams, over the course of a
 * day, and their actions are logged for processing.  Some of the logged game events may be late-
 * arriving, if users play on mobile devices and go transiently offline for a period.
 *
 * <p> This pipeline does batch processing of data collected from gaming events. It calculates the
 * sum of scores per user, over an entire batch of gaming data (collected, say, for each day). The
 * batch processing will not include any late data that arrives after the day's cutoff point.
 *
 * <p> To execute this pipeline using the Dataflow service and static example input data, specify
 * the pipeline configuration like this:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --stagingLocation=gs://YOUR_STAGING_DIRECTORY
 *   --runner=BlockingDataflowPipelineRunner
 *   --dataset=YOUR-DATASET
 * }
 * </pre>
 * where the BigQuery dataset you specify must already exist.
 *
 * <p> Optionally include the --input argument to specify a batch input file.
 * See the --input default value for example batch data file, or use {@link injector.Injector} to
 * generate your own batch data.
  */
public class UserScore {

  /**
   * Class to hold info about a game event.
   */
  @DefaultCoder(AvroCoder.class)
  static class GameActionInfo {
    @Nullable String user;
    @Nullable String team;
    @Nullable Integer score;
    @Nullable Long timestamp;

    public GameActionInfo() {}

    public GameActionInfo(String user, String team, Integer score, Long timestamp) {
      this.user = user;
      this.team = team;
      this.score = score;
      this.timestamp = timestamp;
    }

    public String getUser() {
      return this.user;
    }
    public String getTeam() {
      return this.team;
    }
    public Integer getScore() {
      return this.score;
    }
    public String getKey(String keyname) {
      if (keyname.equals("team")) {
        return this.team;
      } else {  // return username as default
        return this.user;
      }
    }
    public Long getTimestamp() {
      return this.timestamp;
    }
  }


  /**
   * Parses the raw game event info into GameActionInfo objects. Each event line has the following
   * format: username,teamname,score,timestamp_in_ms,readable_time
   * e.g.:
   * user2_AsparagusPig,AsparagusPig,10,1445230923951,2015-11-02 09:09:28.224
   * The human-readable time string is not used here.
   */
  static class ParseEventFn extends DoFn<String, GameActionInfo> {

    // Log and count parse errors.
    private static final Logger LOG = LoggerFactory.getLogger(ParseEventFn.class);
    private final Aggregator<Long, Long> numParseErrors =
        createAggregator("ParseErrors", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext c) {
      String[] components = c.element().split(",");
      try {
        String user = components[0].trim();
        String team = components[1].trim();
        Integer score = Integer.parseInt(components[2].trim());
        Long timestamp = Long.parseLong(components[3].trim());
        GameActionInfo gInfo = new GameActionInfo(user, team, score, timestamp);
        c.output(gInfo);
      } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
        numParseErrors.addValue(1L);
        LOG.info("Parse error on " + c.element() + ", " + e.getMessage());
      }
    }
  }

  /**
   * A transform to extract key/score information from GameActionInfo, and sum the scores. The
   * constructor arg determines whether 'team' or 'user' info is extracted.
   */
  public static class ExtractAndSumScore
      extends PTransform<PCollection<GameActionInfo>, PCollection<KV<String, Integer>>> {

    private final String field;

    ExtractAndSumScore(String field) {
      this.field = field;
    }

    @Override
    public PCollection<KV<String, Integer>> apply(
        PCollection<GameActionInfo> gameInfo) {

      return gameInfo
        .apply(MapElements
            .via((GameActionInfo gInfo) -> KV.of(gInfo.getKey(field), gInfo.getScore()))
            .withOutputType(new TypeDescriptor<KV<String, Integer>>() {}))
        .apply(Sum.<String>integersPerKey());
    }
  }


  /**
   * Format information for key/total_score pairs, and write that info to BigQuery.
   */
  public static class WriteToBigQuery
      extends PTransform<PCollection<KV<String, Integer>>, PDone> {

    private final String tableName;

    public WriteToBigQuery(String tableName) {
      this.tableName = tableName;
    }

    /** Convert each key/score pair into a BigQuery TableRow. */
    private class BuildRowFn extends DoFn<KV<String, Integer>, TableRow> {

      @Override
      public void processElement(ProcessContext c) {

        TableRow row = new TableRow()
         .set("user", c.element().getKey())
         .set("total_score", c.element().getValue().longValue());
        c.output(row);
      }
    }

    /** Build the output table schema. */
    private TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("user").setType("STRING"));
      fields.add(new TableFieldSchema().setName("total_score").setType("INTEGER"));
      return new TableSchema().setFields(fields);
    }

    @Override
    public PDone apply(PCollection<KV<String, Integer>> teamAndScore) {
      return teamAndScore
        .apply(ParDo.named("ConvertToRow").of(new BuildRowFn()))
        .apply(BigQueryIO.Write
                  .to(getTable(teamAndScore.getPipeline(),
                      tableName))
                  .withSchema(getSchema())
                  .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));
    }
  }

  /** Utility to construct an output table reference. */
  static TableReference getTable(Pipeline pipeline, String tableName) {
    PipelineOptions options = pipeline.getOptions();
    TableReference table = new TableReference();
    table.setDatasetId(options.as(Options.class).getDataset());
    table.setProjectId(options.as(GcpOptions.class).getProject());
    table.setTableId(tableName);
    return table;
  }

  /**
   * Options supported by {@link UserScore}.
   */
  static interface Options extends PipelineOptions {

    @Description("Path to the data file(s) containing game data.")
    // The default maps to two large Google Cloud Storage files (each ~12GB) holding two subsequent
    // day's worth (roughly) of data.
    @Default.String("gs://dataflow-samples/game/gaming_data*.csv")
    String getInput();
    void setInput(String value);

    @Description("BigQuery Dataset to write tables to. Must already exist.")
    @Validation.Required
    String getDataset();
    void setDataset(String value);

    @Description("The BigQuery table name. Should not already exist.")
    @Default.String("user_score")
    String getTableName();
    void setTableName(String value);
  }



  /**
   * Run a batch pipeline.
   */
  public static void main(String[] args) throws Exception {
    // Begin constructing a pipeline configured by commandline flags.
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    // Read events from a text file and parse them.
    pipeline.apply(TextIO.Read.from(options.getInput()))
      .apply(ParDo.named("ParseGameEvent").of(new ParseEventFn()))
      // Extract and sum username/score pairs from the event data.
      .apply("ExtractUserScore", new ExtractAndSumScore("user"))
      .apply("WriteUserScoreSums", new WriteToBigQuery(options.getTableName()));

    // Run the batch pipeline.
    pipeline.run();
  }

}
