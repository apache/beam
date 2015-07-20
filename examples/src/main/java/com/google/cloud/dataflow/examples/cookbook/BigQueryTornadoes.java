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

package com.google.cloud.dataflow.examples.cookbook;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;

/**
 * An example that reads the public samples of weather data from BigQuery, counts the number of
 * tornadoes that occur in each month, and writes the results to BigQuery.
 *
 * <p> Concepts: Reading/writing BigQuery; counting a PCollection; user-defined PTransforms
 *
 * <p> Note: Before running this example, you must create a BigQuery dataset to contain your output
 * table.
 *
 * <p> To execute this pipeline locally, specify general pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 * }
 * </pre>
 * and the BigQuery table for the output, with the form
 * <pre>{@code
 *   --output=YOUR_PROJECT_ID:DATASET_ID.TABLE_ID
 * }</pre>
 *
 * <p> To execute this pipeline using the Dataflow service, specify pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --stagingLocation=gs://YOUR_STAGING_DIRECTORY
 *   --runner=BlockingDataflowPipelineRunner
 * }
 * </pre>
 * and the BigQuery table for the output:
 * <pre>{@code
 *   --output=YOUR_PROJECT_ID:DATASET_ID.TABLE_ID
 * }</pre>
 *
 * <p> The BigQuery input table defaults to {@code clouddataflow-readonly:samples.weather_stations}
 * and can be overridden with {@code --input}.
 */
public class BigQueryTornadoes {
  // Default to using a 1000 row subset of the public weather station table publicdata:samples.gsod.
  private static final String WEATHER_SAMPLES_TABLE =
      "clouddataflow-readonly:samples.weather_stations";

  /**
   * Examines each row in the input table. If a tornado was recorded
   * in that sample, the month in which it occurred is output.
   */
  static class ExtractTornadoesFn extends DoFn<TableRow, Integer> {
    private static final long serialVersionUID = 0;

    @Override
    public void processElement(ProcessContext c){
      TableRow row = c.element();
      if ((Boolean) row.get("tornado")) {
        c.output(Integer.parseInt((String) row.get("month")));
      }
    }
  }

  /**
   * Prepares the data for writing to BigQuery by building a TableRow object containing an
   * integer representation of month and the number of tornadoes that occurred in each month.
   */
  static class FormatCountsFn extends DoFn<KV<Integer, Long>, TableRow> {
    private static final long serialVersionUID = 0;

    @Override
    public void processElement(ProcessContext c) {
      TableRow row = new TableRow()
          .set("month", c.element().getKey().intValue())
          .set("tornado_count", c.element().getValue().longValue());
      c.output(row);
    }
  }

  /**
   * Takes rows from a table and generates a table of counts.
   * <p>
   * The input schema is described by
   * https://developers.google.com/bigquery/docs/dataset-gsod .
   * The output contains the total number of tornadoes found in each month in
   * the following schema:
   * <ul>
   *   <li>month: integer</li>
   *   <li>tornado_count: integer</li>
   * </ul>
   */
  static class CountTornadoes
      extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
    private static final long serialVersionUID = 0;

    @Override
    public PCollection<TableRow> apply(PCollection<TableRow> rows) {

      // row... => month...
      PCollection<Integer> tornadoes = rows.apply(
          ParDo.of(new ExtractTornadoesFn()));

      // month... => <month,count>...
      PCollection<KV<Integer, Long>> tornadoCounts =
          tornadoes.apply(Count.<Integer>perElement());

      // <month,count>... => row...
      PCollection<TableRow> results = tornadoCounts.apply(
          ParDo.of(new FormatCountsFn()));

      return results;
    }
  }

  /**
   * Options supported by {@link BigQueryTornadoes}.
   * <p>
   * Inherits standard configuration options.
   */
  private static interface Options extends PipelineOptions {
    @Description("Table to read from, specified as "
        + "<project_id>:<dataset_id>.<table_id>")
    @Default.String(WEATHER_SAMPLES_TABLE)
    String getInput();
    void setInput(String value);

    @Description("BigQuery table to write to, specified as "
        + "<project_id>:<dataset_id>.<table_id>. The dataset must already exist.")
    @Validation.Required
    String getOutput();
    void setOutput(String value);
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline p = Pipeline.create(options);

    // Build the table schema for the output table.
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("month").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("tornado_count").setType("INTEGER"));
    TableSchema schema = new TableSchema().setFields(fields);

    p.apply(BigQueryIO.Read.from(options.getInput()))
     .apply(new CountTornadoes())
     .apply(BigQueryIO.Write
        .to(options.getOutput())
        .withSchema(schema)
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    p.run();
  }
}
