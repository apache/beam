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
package org.apache.beam.examples;

import org.apache.beam.examples.common.ExampleBigQueryTableOptions;
import org.apache.beam.examples.common.ExampleOptions;
import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * An example that counts words in text, and can run over either unbounded or bounded input
 * collections.
 *
 * <p>This class, {@link WindowedWordCount}, is the last in a series of four successively more
 * detailed 'word count' examples. First take a look at {@link MinimalWordCount},
 * {@link WordCount}, and {@link DebuggingWordCount}.
 *
 * <p>Basic concepts, also in the MinimalWordCount, WordCount, and DebuggingWordCount examples:
 * Reading text files; counting a PCollection; writing to GCS; executing a Pipeline both locally
 * and using the Dataflow service; defining DoFns; creating a custom aggregator;
 * user-defined PTransforms; defining PipelineOptions.
 *
 * <p>New Concepts:
 * <pre>
 *   1. Unbounded and bounded pipeline input modes
 *   2. Adding timestamps to data
 *   3. Windowing
 *   4. Re-using PTransforms over windowed PCollections
 *   5. Writing to BigQuery
 * </pre>
 *
 * <p>To execute this pipeline locally, specify general pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 * }
 * </pre>
 *
 * <p>To execute this pipeline using the Dataflow service, specify pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --tempLocation=gs://YOUR_TEMP_DIRECTORY
 *   --runner=BlockingDataflowRunner
 * }
 * </pre>
 *
 * <p>Optionally specify the input file path via:
 * {@code --inputFile=gs://INPUT_PATH},
 * which defaults to {@code gs://dataflow-samples/shakespeare/kinglear.txt}.
 *
 * <p>Specify an output BigQuery dataset and optionally, a table for the output. If you don't
 * specify the table, one will be created for you using the job name. If you don't specify the
 * dataset, a dataset called {@code dataflow-examples} must already exist in your project.
 * {@code --bigQueryDataset=YOUR-DATASET --bigQueryTable=YOUR-NEW-TABLE-NAME}.
 *
 * <p>By default, the pipeline will do fixed windowing, on 1-minute windows.  You can
 * change this interval by setting the {@code --windowSize} parameter, e.g. {@code --windowSize=10}
 * for 10-minute windows.
 *
 * <p>The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C)
 * and then exits.
 */
public class WindowedWordCount {
    static final int WINDOW_SIZE = 1;  // Default window duration in minutes

  /**
   * Concept #2: A DoFn that sets the data element timestamp. This is a silly method, just for
   * this example, for the bounded data case.
   *
   * <p>Imagine that many ghosts of Shakespeare are all typing madly at the same time to recreate
   * his masterworks. Each line of the corpus will get a random associated timestamp somewhere in a
   * 2-hour period.
   */
  static class AddTimestampFn extends DoFn<String, String> {
    private static final Duration RAND_RANGE = Duration.standardHours(2);
    private final Instant minTimestamp;

    AddTimestampFn() {
      this.minTimestamp = new Instant(System.currentTimeMillis());
    }

    @Override
    public void processElement(ProcessContext c) {
      // Generate a timestamp that falls somewhere in the past two hours.
      long randMillis = (long) (Math.random() * RAND_RANGE.getMillis());
      Instant randomTimestamp = minTimestamp.plus(randMillis);
      /**
       * Concept #2: Set the data element with that timestamp.
       */
      c.outputWithTimestamp(c.element(), new Instant(randomTimestamp));
    }
  }

  /** A DoFn that converts a Word and Count into a BigQuery table row. */
  static class FormatAsTableRowFn extends DoFn<KV<String, Long>, TableRow> {
    @Override
    public void processElement(ProcessContext c) {
      TableRow row = new TableRow()
          .set("word", c.element().getKey())
          .set("count", c.element().getValue())
          // include a field for the window timestamp
         .set("window_timestamp", c.timestamp().toString());
      c.output(row);
    }
  }

  /**
   * Helper method that defines the BigQuery schema used for the output.
   */
  private static TableSchema getSchema() {
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("word").setType("STRING"));
    fields.add(new TableFieldSchema().setName("count").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("window_timestamp").setType("TIMESTAMP"));
    TableSchema schema = new TableSchema().setFields(fields);
    return schema;
  }

  /**
   * Concept #5: We'll stream the results to a BigQuery table. The BigQuery output source is one
   * that supports both bounded and unbounded data. This is a helper method that creates a
   * TableReference from input options, to tell the pipeline where to write its BigQuery results.
   */
  private static TableReference getTableReference(Options options) {
    TableReference tableRef = new TableReference();
    tableRef.setProjectId(options.getProject());
    tableRef.setDatasetId(options.getBigQueryDataset());
    tableRef.setTableId(options.getBigQueryTable());
    return tableRef;
  }

  /**
   * Options supported by {@link WindowedWordCount}.
   *
   * <p>Inherits standard example configuration options, which allow specification of the BigQuery
   * table, as well as the {@link WordCount.WordCountOptions} support for
   * specification of the input file.
   */
  public static interface Options extends WordCount.WordCountOptions,
      ExampleOptions, ExampleBigQueryTableOptions {
    @Description("Fixed window duration, in minutes")
    @Default.Integer(WINDOW_SIZE)
    Integer getWindowSize();
    void setWindowSize(Integer value);
  }

  public static void main(String[] args) throws IOException {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setBigQuerySchema(getSchema());
    // DataflowExampleUtils creates the necessary input sources to simplify execution of this
    // Pipeline.
    ExampleUtils exampleUtils = new ExampleUtils(options);
    exampleUtils.setup();

    Pipeline pipeline = Pipeline.create(options);

    /**
     * Concept #1: the Dataflow SDK lets us run the same pipeline with either a bounded or
     * unbounded input source.
     */
    PCollection<String> input = pipeline
      /** Read from the GCS file. */
      .apply(TextIO.Read.from(options.getInputFile()))
      // Concept #2: Add an element timestamp, using an artificial time just to show windowing.
      // See AddTimestampFn for more detail on this.
      .apply(ParDo.of(new AddTimestampFn()));

    /**
     * Concept #3: Window into fixed windows. The fixed window size for this example defaults to 1
     * minute (you can change this with a command-line option). See the documentation for more
     * information on how fixed windows work, and for information on the other types of windowing
     * available (e.g., sliding windows).
     */
    PCollection<String> windowedWords = input
      .apply(Window.<String>into(
        FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))));

    /**
     * Concept #4: Re-use our existing CountWords transform that does not have knowledge of
     * windows over a PCollection containing windowed values.
     */
    PCollection<KV<String, Long>> wordCounts = windowedWords.apply(new WordCount.CountWords());

    /**
     * Concept #5: Format the results for a BigQuery table, then write to BigQuery.
     * The BigQuery output source supports both bounded and unbounded data.
     */
    wordCounts.apply(ParDo.of(new FormatAsTableRowFn()))
        .apply(BigQueryIO.Write
          .to(getTableReference(options))
          .withSchema(getSchema())
          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

    PipelineResult result = pipeline.run();

    // dataflowUtils will try to cancel the pipeline before the program exists.
    exampleUtils.waitToFinish(result);
  }
}
