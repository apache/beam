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

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// beam-playground:
//   name: MinimalBigQueryTornadoes
//   description: An example that reads the public samples of weather data from BigQuery.
//   multifile: false
//   never_run: true
//   always_run: true
//   default_example: false
//   pipeline_options: --project apache-beam-testing
//   context_line: 102
//   categories:
//     - Filtering
//     - IO
//     - Core Transforms
//   complexity: BASIC
//   tags:
//     - filter
//     - bigquery
//     - strings

/**
 * An example that reads the public samples of weather data from BigQuery, counts the number of
 * tornadoes that occur in each month, and writes the results to BigQuery.
 *
 * <p>Concepts: Reading/writing BigQuery; counting a PCollection; user-defined PTransforms
 *
 * <p>The BigQuery input is taken from {@code clouddataflow-readonly:samples.weather_stations}
 */
public class MinimalBigQueryTornadoes {
  private static final Logger LOG = LoggerFactory.getLogger(MinimalBigQueryTornadoes.class);

  // Use a 1000 row subset of the public weather station table publicdata:samples.gsod.
  private static final String WEATHER_SAMPLES_TABLE =
      "clouddataflow-readonly:samples.weather_stations";

  /**
   * Examines each row in the input table. If a tornado was recorded in that sample, the month in
   * which it occurred is output.
   */
  static class ExtractTornadoesFn extends DoFn<TableRow, Integer> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      TableRow row = c.element();
      if ((Boolean) row.get("tornado")) {
        c.output(Integer.parseInt((String) row.get("month")));
      }
    }
  }

  /**
   * Prepares the data for writing to BigQuery by building a TableRow object containing an integer
   * representation of month and the number of tornadoes that occurred in each month.
   */
  static class FormatCountsFn extends DoFn<KV<Integer, Long>, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().getKey() + ": " + c.element().getValue());
    }
  }

  public static void applyBigQueryTornadoes(Pipeline p) {
    TypedRead<TableRow> bigqueryIO =
        BigQueryIO.readTableRows()
            .from(WEATHER_SAMPLES_TABLE)
            .withMethod(TypedRead.Method.DIRECT_READ)
            .withSelectedFields(Lists.newArrayList("month", "tornado"));

    PCollection<TableRow> rowsFromBigQuery = p.apply(bigqueryIO);

    rowsFromBigQuery
        // Extract rows which include information on tornadoes per month.
        .apply(ParDo.of(new ExtractTornadoesFn()))
        // Count the number of times each month appears in the data.
        .apply(Count.perElement())
        // Format each month and count into a printable string.
        .apply(ParDo.of(new FormatCountsFn()))
        // Write the formatted results to the log.
        .apply(ParDo.of(new LogOutput<>("Result: ")))
        // Write the formatted results to a file.
        .apply(TextIO.write().to("tornadoes"));
  }

  public static void runBigQueryTornadoes(PipelineOptions options) {
    Pipeline p = Pipeline.create(options);
    applyBigQueryTornadoes(p);
    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    PipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);
    runBigQueryTornadoes(options);
  }

  static class LogOutput<T> extends DoFn<T, T> {
    private final String prefix;

    LogOutput(String prefix) {
      this.prefix = prefix;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      LOG.info(prefix + c.element());
      c.output(c.element());
    }
  }
}
