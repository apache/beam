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
package org.apache.beam.examples.templates;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.PubsubIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.Transport;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import java.io.IOException;

/**
 * A simple template that allows for writing a Pub/Sub stream to a BigQuery table.
 *
 * <p>The key parameters for this are --outputTableSpec and --inputTopic.
 *
 * <p>The output table for this must already exist.
 */
public class PubSubToBigQuery {
  /**
   * Options supported by {@link PubSubToBigQuery}.
   */
  public static interface Options extends PipelineOptions {
    @Description("Table spec to write the output to")
    ValueProvider<String> getOutputTableSpec();
    void setOutputTableSpec(ValueProvider<String> value);

    @Description("Pub/Sub topic to read the input from")
    ValueProvider<String> getInputTopic();
    void setInputTopic(ValueProvider<String> value);
  }

  public static void main(String[] args) {
    final Options options = PipelineOptionsFactory.fromArgs(args).withValidation()
      .as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("ReadPubsub", PubsubIO.Read.topic(options.getInputTopic().get()))
        .apply("ConvertToRow", MapElements.via(
            new SimpleFunction<String, TableRow>() {
              @Override
              public TableRow apply(String input) {
                try {
                  return Transport.getJsonFactory().fromString(input, TableRow.class);
                } catch (IOException e) {
                  throw new RuntimeException("Unable to parse input", e);
                }
              }
            }))
        .apply("WriteBigQuery", BigQueryIO.Write.to(
            new SerializableFunction<BoundedWindow, String>() {
              @Override
              public String apply(BoundedWindow value) {
                return options.getOutputTableSpec().get();
              }
            }));

    pipeline.run();
  }
}
