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

import org.apache.beam.examples.common.DataflowExampleUtils;
import org.apache.beam.examples.common.ExampleBigQueryTableOptions;
import org.apache.beam.examples.common.ExamplePubsubTopicOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.BigQueryIO;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

import java.io.IOException;
import java.util.ArrayList;

/**
 * A streaming Dataflow Example using BigQuery output.
 *
 * <p>This pipeline example reads lines of text from a PubSub topic, splits each line
 * into individual words, capitalizes those words, and writes the output to
 * a BigQuery table.
 *
 * <p>By default, the example will run a separate pipeline to inject the data from the default
 * {@literal --inputFile} to the Pub/Sub {@literal --pubsubTopic}. It will make it available for
 * the streaming pipeline to process. You may override the default {@literal --inputFile} with the
 * file of your choosing. You may also set {@literal --inputFile} to an empty string, which will
 * disable the automatic Pub/Sub injection, and allow you to use separate tool to control the input
 * to this example.
 *
 * <p>The example is configured to use the default Pub/Sub topic and the default BigQuery table
 * from the example common package (there are no defaults for a general Dataflow pipeline).
 * You can override them by using the {@literal --pubsubTopic}, {@literal --bigQueryDataset}, and
 * {@literal --bigQueryTable} options. If the Pub/Sub topic or the BigQuery table do not exist,
 * the example will try to create them.
 *
 * <p>The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C)
 * and then exits.
 */
public class StreamingWordExtract {

  /** A DoFn that tokenizes lines of text into individual words. */
  static class ExtractWords extends DoFn<String, String> {
    @Override
    public void processElement(ProcessContext c) {
      String[] words = c.element().split("[^a-zA-Z']+");
      for (String word : words) {
        if (!word.isEmpty()) {
          c.output(word);
        }
      }
    }
  }

  /** A DoFn that uppercases a word. */
  static class Uppercase extends DoFn<String, String> {
    @Override
    public void processElement(ProcessContext c) {
      c.output(c.element().toUpperCase());
    }
  }

  /**
   * Converts strings into BigQuery rows.
   */
  static class StringToRowConverter extends DoFn<String, TableRow> {
    /**
     * In this example, put the whole string into single BigQuery field.
     */
    @Override
    public void processElement(ProcessContext c) {
      c.output(new TableRow().set("string_field", c.element()));
    }

    static TableSchema getSchema() {
      return new TableSchema().setFields(new ArrayList<TableFieldSchema>() {
            // Compose the list of TableFieldSchema from tableSchema.
            {
              add(new TableFieldSchema().setName("string_field").setType("STRING"));
            }
      });
    }
  }

  /**
   * Options supported by {@link StreamingWordExtract}.
   *
   * <p>Inherits standard configuration options.
   */
  private interface StreamingWordExtractOptions
      extends ExamplePubsubTopicOptions, ExampleBigQueryTableOptions {
    @Description("Input file to inject to Pub/Sub topic")
    @Default.String("gs://dataflow-samples/shakespeare/kinglear.txt")
    String getInputFile();
    void setInputFile(String value);
  }

  /**
   * Sets up and starts streaming pipeline.
   *
   * @throws IOException if there is a problem setting up resources
   */
  public static void main(String[] args) throws IOException {
    StreamingWordExtractOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(StreamingWordExtractOptions.class);
    options.setStreaming(true);

    options.setBigQuerySchema(StringToRowConverter.getSchema());
    DataflowExampleUtils dataflowUtils = new DataflowExampleUtils(options);
    dataflowUtils.setup();

    Pipeline pipeline = Pipeline.create(options);

    String tableSpec = new StringBuilder()
        .append(options.getProject()).append(":")
        .append(options.getBigQueryDataset()).append(".")
        .append(options.getBigQueryTable())
        .toString();
    pipeline
        .apply(PubsubIO.Read.topic(options.getPubsubTopic()))
        .apply(ParDo.of(new ExtractWords()))
        .apply(ParDo.of(new Uppercase()))
        .apply(ParDo.of(new StringToRowConverter()))
        .apply(BigQueryIO.Write.to(tableSpec)
            .withSchema(StringToRowConverter.getSchema()));

    PipelineResult result = pipeline.run();

    if (!options.getInputFile().isEmpty()) {
      // Inject the data into the Pub/Sub topic with a Dataflow batch pipeline.
      dataflowUtils.runInjectorPipeline(options.getInputFile(), options.getPubsubTopic());
    }

    // dataflowUtils will try to cancel the pipeline and the injector before the program exists.
    dataflowUtils.waitToFinish(result);
  }
}
