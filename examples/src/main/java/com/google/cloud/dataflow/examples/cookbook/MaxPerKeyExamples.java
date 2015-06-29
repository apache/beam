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
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Max;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;

/**
 * An example that reads the public samples of weather data from BigQuery, and finds
 * the maximum temperature ('mean_temp') for each month.
 *
 * <p> Concepts: The 'Max' statistical combination function, and how to find the max per
 * key group.
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
 * <p> The BigQuery input table defaults to {@code clouddataflow-readonly:samples.weather_stations }
 * and can be overridden with {@code --input}.
 */
public class MaxPerKeyExamples {
  // Default to using a 1000 row subset of the public weather station table publicdata:samples.gsod.
  private static final String WEATHER_SAMPLES_TABLE =
      "clouddataflow-readonly:samples.weather_stations";

  /**
   * Examines each row (weather reading) in the input table. Output the month of the reading,
   * and the mean_temp.
   */
  static class ExtractTempFn extends DoFn<TableRow, KV<Integer, Double>> {
    private static final long serialVersionUID = 0;

    @Override
    public void processElement(ProcessContext c) {
      TableRow row = c.element();
      Integer month = Integer.parseInt((String) row.get("month"));
      Double meanTemp = Double.parseDouble(row.get("mean_temp").toString());
      c.output(KV.of(month, meanTemp));
    }
  }

  /**
   * Format the results to a TableRow, to save to BigQuery.
   *
   */
  static class FormatMaxesFn extends DoFn<KV<Integer, Double>, TableRow> {
    private static final long serialVersionUID = 0;

    @Override
    public void processElement(ProcessContext c) {
      TableRow row = new TableRow()
          .set("month", c.element().getKey())
          .set("max_mean_temp", c.element().getValue());
      c.output(row);
    }
  }

  /**
   * Reads rows from a weather data table, and finds the max mean_temp for each
   * month via the 'Max' statistical combination function.
   */
  static class MaxMeanTemp
      extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
    private static final long serialVersionUID = 0;

    @Override
    public PCollection<TableRow> apply(PCollection<TableRow> rows) {

      // row... => <month, mean_temp> ...
      PCollection<KV<Integer, Double>> temps = rows.apply(
          ParDo.of(new ExtractTempFn()));

      // month, mean_temp... => <month, max mean temp>...
      PCollection<KV<Integer, Double>> tempMaxes =
          temps.apply(Max.<Integer>doublesPerKey());

      // <month, max>... => row...
      PCollection<TableRow> results = tempMaxes.apply(
          ParDo.of(new FormatMaxesFn()));

      return results;
    }
  }

  /**
   * Options supported by {@link MaxPerKeyExamples}.
   * <p>
   * Inherits standard configuration options.
   */
  private static interface Options extends PipelineOptions {
    @Description("Table to read from, specified as "
        + "<project_id>:<dataset_id>.<table_id>")
    @Default.String(WEATHER_SAMPLES_TABLE)
    String getInput();
    void setInput(String value);

    @Description("Table to write to, specified as "
        + "<project_id>:<dataset_id>.<table_id>")
    @Validation.Required
    String getOutput();
    void setOutput(String value);
  }

  public static void main(String[] args)
      throws Exception {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    // Build the table schema for the output table.
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("month").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("max_mean_temp").setType("FLOAT"));
    TableSchema schema = new TableSchema().setFields(fields);

    p.apply(BigQueryIO.Read.from(options.getInput()))
     .apply(new MaxMeanTemp())
     .apply(BigQueryIO.Write
        .to(options.getOutput())
        .withSchema(schema)
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    p.run();
  }
}
