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
package org.apache.beam.examples.complete;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.util.Collections;
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
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * A streaming Beam Example using BigQuery output.
 *
 * <p>This pipeline example reads lines of the input text file, splits each line into individual
 * words, capitalizes those words, and writes the output to a BigQuery table.
 *
 * <p>The example is configured to use the default BigQuery table from the example common package
 * (there are no defaults for a general Beam pipeline). You can override them by using the {@literal
 * --bigQueryDataset}, and {@literal --bigQueryTable} options. If the BigQuery table do not exist,
 * the example will try to create them.
 *
 * <p>The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C)
 * and then exits.
 */
public class StreamingWordExtract {

  /** A {@link DoFn} that tokenizes lines of text into individual words. */
  static class ExtractWords extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      String[] words = c.element().split(ExampleUtils.TOKENIZER_PATTERN, -1);

      for (String word : words) {
        if (!word.isEmpty()) {
          c.output(word);
        }
      }
    }
  }

  /** A {@link DoFn} that uppercases a word. */
  static class Uppercase extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element().toUpperCase());
    }
  }

  /** Converts strings into BigQuery rows. */
  static class StringToRowConverter extends DoFn<String, TableRow> {
    /** In this example, put the whole string into single BigQuery field. */
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(new TableRow().set("string_field", c.element()));
    }

    static TableSchema getSchema() {
      return new TableSchema()
          .setFields(
              Collections.singletonList(
                  new TableFieldSchema().setName("string_field").setType("STRING")));
    }
  }

  /**
   * Options supported by {@link StreamingWordExtract}.
   *
   * <p>Inherits standard configuration options.
   */
  public interface StreamingWordExtractOptions
      extends ExampleOptions, ExampleBigQueryTableOptions, StreamingOptions {
    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();

    void setInputFile(String value);
  }

  /**
   * Sets up and starts streaming pipeline.
   *
   * @throws IOException if there is a problem setting up resources
   */
  public static void main(String[] args) throws IOException {
    StreamingWordExtractOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(StreamingWordExtractOptions.class);
    options.setStreaming(true);

    options.setBigQuerySchema(StringToRowConverter.getSchema());
    ExampleUtils exampleUtils = new ExampleUtils(options);
    exampleUtils.setup();

    Pipeline pipeline = Pipeline.create(options);

    String tableSpec =
        new StringBuilder()
            .append(options.getProject())
            .append(":")
            .append(options.getBigQueryDataset())
            .append(".")
            .append(options.getBigQueryTable())
            .toString();
    pipeline
        .apply("ReadLines", TextIO.read().from(options.getInputFile()))
        .apply(ParDo.of(new ExtractWords()))
        .apply(ParDo.of(new Uppercase()))
        .apply(ParDo.of(new StringToRowConverter()))
        .apply(
            BigQueryIO.writeTableRows().to(tableSpec).withSchema(StringToRowConverter.getSchema()));

    PipelineResult result = pipeline.run();

    // ExampleUtils will try to cancel the pipeline before the program exists.
    exampleUtils.waitToFinish(result);
  }
}
