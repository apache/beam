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
package org.apache.beam.examples.cookbook;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * This is an example that demonstrates several approaches to filtering, and use of the Mean
 * transform. It shows how to dynamically set parameters by defining and using new pipeline options,
 * and how to use a value derived by the pipeline.
 *
 * <p>Concepts: The Mean transform; Options configuration; using pipeline-derived data as a side
 * input; approaches to filtering, selection, and projection.
 *
 * <p>The example reads public samples of weather data from BigQuery. It performs a projection on
 * the data, finds the global mean of the temperature readings, filters on readings for a single
 * given month, and then outputs only data (for that month) that has a mean temp smaller than the
 * derived global mean.
 *
 * <p>Note: Before running this example, you must create a BigQuery dataset to contain your output
 * table.
 *
 * <p>To execute this pipeline locally, specify the BigQuery table for the output:
 *
 * <pre>{@code
 * --output=YOUR_PROJECT_ID:DATASET_ID.TABLE_ID
 * [--monthFilter=<month_number>]
 * }</pre>
 *
 * where optional parameter {@code --monthFilter} is set to a number 1-12.
 *
 * <p>To change the runner, specify:
 *
 * <pre>{@code
 * --runner=YOUR_SELECTED_RUNNER
 * }</pre>
 *
 * See examples/java/README.md for instructions about how to configure different runners.
 *
 * <p>The BigQuery input table defaults to {@code clouddataflow-readonly:samples.weather_stations}
 * and can be overridden with {@code --input}.
 */
public class FilterExamples {
  // Default to using a 1000 row subset of the public weather station table publicdata:samples.gsod.
  private static final String WEATHER_SAMPLES_TABLE =
      "clouddataflow-readonly:samples.weather_stations";
  static final Logger LOG = Logger.getLogger(FilterExamples.class.getName());
  static final int MONTH_TO_FILTER = 7;

  /**
   * Examines each row in the input table. Outputs only the subset of the cells this example is
   * interested in-- the mean_temp and year, month, and day-- as a bigquery table row.
   */
  static class ProjectionFn extends DoFn<TableRow, TableRow> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      TableRow row = c.element();
      // Grab year, month, day, mean_temp from the row
      Integer year = Integer.parseInt((String) row.get("year"));
      Integer month = Integer.parseInt((String) row.get("month"));
      Integer day = Integer.parseInt((String) row.get("day"));
      Double meanTemp = Double.parseDouble(row.get("mean_temp").toString());
      // Prepares the data for writing to BigQuery by building a TableRow object
      TableRow outRow =
          new TableRow()
              .set("year", year)
              .set("month", month)
              .set("day", day)
              .set("mean_temp", meanTemp);
      c.output(outRow);
    }
  }

  /**
   * Implements 'filter' functionality.
   *
   * <p>Examines each row in the input table. Outputs only rows from the month monthFilter, which is
   * passed in as a parameter during construction of this DoFn.
   */
  static class FilterSingleMonthDataFn extends DoFn<TableRow, TableRow> {
    Integer monthFilter;

    public FilterSingleMonthDataFn(Integer monthFilter) {
      this.monthFilter = monthFilter;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      TableRow row = c.element();
      Integer month;
      month = (Integer) row.get("month");
      if (month.equals(this.monthFilter)) {
        c.output(row);
      }
    }
  }

  /**
   * Examines each row (weather reading) in the input table. Output the temperature reading for that
   * row ('mean_temp').
   */
  static class ExtractTempFn extends DoFn<TableRow, Double> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      TableRow row = c.element();
      Double meanTemp = Double.parseDouble(row.get("mean_temp").toString());
      c.output(meanTemp);
    }
  }

  /**
   * Finds the global mean of the mean_temp for each day/record, and outputs only data that has a
   * mean temp larger than this global mean.
   */
  static class BelowGlobalMean extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
    Integer monthFilter;

    public BelowGlobalMean(Integer monthFilter) {
      this.monthFilter = monthFilter;
    }

    @Override
    public PCollection<TableRow> expand(PCollection<TableRow> rows) {

      // Extract the mean_temp from each row.
      PCollection<Double> meanTemps = rows.apply(ParDo.of(new ExtractTempFn()));

      // Find the global mean, of all the mean_temp readings in the weather data,
      // and prepare this singleton PCollectionView for use as a side input.
      final PCollectionView<Double> globalMeanTemp =
          meanTemps.apply(Mean.globally()).apply(View.asSingleton());

      // Rows filtered to remove all but a single month
      PCollection<TableRow> monthFilteredRows =
          rows.apply(ParDo.of(new FilterSingleMonthDataFn(monthFilter)));

      // Then, use the global mean as a side input, to further filter the weather data.
      // By using a side input to pass in the filtering criteria, we can use a value
      // that is computed earlier in pipeline execution.
      // We'll only output readings with temperatures below this mean.
      PCollection<TableRow> filteredRows =
          monthFilteredRows.apply(
              "ParseAndFilter",
              ParDo.of(
                      new DoFn<TableRow, TableRow>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                          Double meanTemp =
                              Double.parseDouble(c.element().get("mean_temp").toString());
                          Double gTemp = c.sideInput(globalMeanTemp);
                          if (meanTemp < gTemp) {
                            c.output(c.element());
                          }
                        }
                      })
                  .withSideInputs(globalMeanTemp));

      return filteredRows;
    }
  }

  /**
   * Options supported by {@link FilterExamples}.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions {
    @Description("Table to read from, specified as " + "<project_id>:<dataset_id>.<table_id>")
    @Default.String(WEATHER_SAMPLES_TABLE)
    String getInput();

    void setInput(String value);

    @Description(
        "Table to write to, specified as "
            + "<project_id>:<dataset_id>.<table_id>. "
            + "The dataset_id must already exist")
    @Validation.Required
    String getOutput();

    void setOutput(String value);

    @Description("Numeric value of month to filter on")
    @Default.Integer(MONTH_TO_FILTER)
    Integer getMonthFilter();

    void setMonthFilter(Integer value);
  }

  /** Helper method to build the table schema for the output table. */
  private static TableSchema buildWeatherSchemaProjection() {
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("year").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("month").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("day").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("mean_temp").setType("FLOAT"));
    return new TableSchema().setFields(fields);
  }

  public static void main(String[] args) throws Exception {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    runFilterExamples(options);
  }

  static void runFilterExamples(Options options) {
    Pipeline p = Pipeline.create(options);

    TableSchema schema = buildWeatherSchemaProjection();

    p.apply(BigQueryIO.readTableRows().from(options.getInput()))
        .apply(ParDo.of(new ProjectionFn()))
        .apply(new BelowGlobalMean(options.getMonthFilter()))
        .apply(
            BigQueryIO.writeTableRows()
                .to(options.getOutput())
                .withSchema(schema)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    p.run().waitUntilFinish();
  }
}
