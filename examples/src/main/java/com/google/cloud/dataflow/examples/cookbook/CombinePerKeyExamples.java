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
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;

/**
 * An example that reads the public 'Shakespeare' data, and for each word in
 * the dataset that is over a given length, generates a string containing the
 * list of play names in which that word appears, and saves this information
 * to a bigquery table.
 *
 * <p>Concepts: the Combine.perKey transform, which lets you combine the values in a
 * key-grouped Collection, and how to use an Aggregator to track information in the
 * Monitoring UI.
 *
 * <p> Note: Before running this example, you must create a BigQuery dataset to contain your output
 * table.
 *
 * <p> To execute this pipeline locally, specify general pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 * }
 * </pre>
 * and the BigQuery table for the output:
 * <pre>{@code
 *   --output=YOUR_PROJECT_ID:DATASET_ID.TABLE_ID
 * }</pre>
 *
 * <p> To execute this pipeline using the Dataflow service, specify pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --stagingLocation=gs://<STAGING DIRECTORY>
 *   --runner=BlockingDataflowPipelineRunner
 * }
 * </pre>
 * and the BigQuery table for the output:
 * <pre>{@code
 *   --output=YOUR_PROJECT_ID:DATASET_ID.TABLE_ID
 * }</pre>
 *
 * <p> The BigQuery input table defaults to {@code publicdata:samples.shakespeare} and can
 * be overridden with {@code --input}.
 */
public class CombinePerKeyExamples {
  // Use the shakespeare public BigQuery sample
  private static final String SHAKESPEARE_TABLE =
      "publicdata:samples.shakespeare";
  // We'll track words >= this word length across all plays in the table.
  private static final int MIN_WORD_LENGTH = 9;

  /**
   * Examines each row in the input table. If the word is greater than or equal to MIN_WORD_LENGTH,
   * outputs word, play_name.
   */
  static class ExtractLargeWordsFn extends DoFn<TableRow, KV<String, String>> {
    private static final long serialVersionUID = 0;

    private final Aggregator<Long, Long> smallerWords =
        createAggregator("smallerWords", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext c){
      TableRow row = c.element();
      String playName = (String) row.get("corpus");
      String word = (String) row.get("word");
      if (word.length() >= MIN_WORD_LENGTH) {
        c.output(KV.of(word, playName));
      } else {
        // Track how many smaller words we're not including. This information will be
        // visible in the Monitoring UI.
        smallerWords.addValue(1L);
      }
    }
  }


  /**
   * Prepares the data for writing to BigQuery by building a TableRow object
   * containing a word with a string listing the plays in which it appeared.
   */
  static class FormatShakespeareOutputFn extends DoFn<KV<String, String>, TableRow> {
    private static final long serialVersionUID = 0;

    @Override
    public void processElement(ProcessContext c) {
      TableRow row = new TableRow()
          .set("word", c.element().getKey())
          .set("all_plays", c.element().getValue());
      c.output(row);
    }
  }

  /**
   * Reads the public 'Shakespeare' data, and for each word in the dataset
   * over a given length, generates a string containing the list of play names
   * in which that word appears. It does this via the Combine.perKey
   * transform, with the ConcatWords combine function.
   *
   * <p> Combine.perKey is similar to a GroupByKey followed by a ParDo, but
   * has more restricted semantics that allow it to be executed more
   * efficiently. These records are then formatted as BQ table rows.
   */
  static class PlaysForWord
      extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {
    private static final long serialVersionUID = 0;

    @Override
    public PCollection<TableRow> apply(PCollection<TableRow> rows) {

      // row... => <word, play_name> ...
      PCollection<KV<String, String>> words = rows.apply(
          ParDo.of(new ExtractLargeWordsFn()));

      // word, play_name => word, all_plays ...
      PCollection<KV<String, String>> wordAllPlays =
          words.apply(Combine.<String, String>perKey(
              new ConcatWords()));

      // <word, all_plays>... => row...
      PCollection<TableRow> results = wordAllPlays.apply(
          ParDo.of(new FormatShakespeareOutputFn()));

      return results;
    }
  }

  /**
   * A 'combine function' used with the Combine.perKey transform. Builds a
   * comma-separated string of all input items.  So, it will build a string
   * containing all the different Shakespeare plays in which the given input
   * word has appeared.
   */
  public static class ConcatWords implements SerializableFunction<Iterable<String>, String> {
    private static final long serialVersionUID = 0;

    @Override
    public String apply(Iterable<String> input) {
      StringBuilder all = new StringBuilder();
      for (String item : input) {
        if (!item.isEmpty()) {
          if (all.length() == 0) {
            all.append(item);
          } else {
            all.append(",");
            all.append(item);
          }
        }
      }
      return all.toString();
    }
  }

  /**
   * Options supported by {@link CombinePerKeyExamples}.
   * <p>
   * Inherits standard configuration options.
   */
  private static interface Options extends PipelineOptions {
    @Description("Table to read from, specified as "
        + "<project_id>:<dataset_id>.<table_id>")
    @Default.String(SHAKESPEARE_TABLE)
    String getInput();
    void setInput(String value);

    @Description("Table to write to, specified as "
        + "<project_id>:<dataset_id>.<table_id>. "
        + "The dataset_id must already exist")
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
    fields.add(new TableFieldSchema().setName("word").setType("STRING"));
    fields.add(new TableFieldSchema().setName("all_plays").setType("STRING"));
    TableSchema schema = new TableSchema().setFields(fields);

    p.apply(BigQueryIO.Read.from(options.getInput()))
     .apply(new PlaysForWord())
     .apply(BigQueryIO.Write
        .to(options.getOutput())
        .withSchema(schema)
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    p.run();
  }
}
