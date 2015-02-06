/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.examples;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

import java.util.ArrayList;

/**
 * A streaming Dataflow Example using BigQuery output.
 *
 * <p> This pipeline example lines of text from a PubSub topic, splits each line
 * into individual words, capitalizes those words, and writes the output to
 * a BigQuery table. </p>
 *
 * <p> To run this example using the Dataflow service, you must provide an input
 * pubsub topic and an output BigQuery table, using the {@literal --inputTopic}
 * {@literal --dataset} and {@literal --table} options. Since this is a streaming
 * pipeline that never completes, select the non-blocking pipeline runner
 * {@literal --runner=DataflowPipelineRunner}.
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
   * Command line parameter options.
   */
  private interface StreamingWordExtractOptions extends PipelineOptions {
    @Description("Input Pubsub topic")
    @Validation.Required
    String getInputTopic();
    void setInputTopic(String value);

    @Description("BigQuery dataset name")
    @Validation.Required
    String getDataset();
    void setDataset(String value);

    @Description("BigQuery table name")
    @Validation.Required
    String getTable();
    void setTable(String value);
  }

  /**
   * Sets up and starts streaming pipeline.
   */
  public static void main(String[] args) {
    StreamingWordExtractOptions options = PipelineOptionsFactory.fromArgs(args)
        .withValidation()
        .as(StreamingWordExtractOptions.class);
    DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    dataflowOptions.setStreaming(true);

    Pipeline pipeline = Pipeline.create(options);

    String tableSpec = new StringBuilder()
        .append(dataflowOptions.getProject()).append(":")
        .append(options.getDataset()).append(".")
        .append(options.getTable())
        .toString();
    pipeline
        .apply(PubsubIO.Read.topic(options.getInputTopic()))
        .apply(ParDo.of(new ExtractWords()))
        .apply(ParDo.of(new Uppercase()))
        .apply(ParDo.of(new StringToRowConverter()))
        .apply(BigQueryIO.Write.to(tableSpec)
            .withSchema(StringToRowConverter.getSchema()));

    pipeline.run();
  }
}
