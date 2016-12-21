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

package org.apache.beam.runners.gearpump.examples;

import org.apache.beam.runners.gearpump.GearpumpPipelineOptions;
import org.apache.beam.runners.gearpump.GearpumpRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.apache.gearpump.cluster.client.ClientContext;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * streaming word count example on Gearpump runner.
 */
public class StreamingWordCount {

  static class ExtractWordsFn extends DoFn<String, String> {

    @ProcessElement
    public void process(ProcessContext c) {
      // Split the line into words.
      String[] words = c.element().split("[^a-zA-Z']+");

      // Output each word encountered into the output PCollection.
      for (String word : words) {
        if (!word.isEmpty()) {
          c.output(word);
        }
      }
    }
  }

  static class FormatAsStringFn extends DoFn<KV<String, Long>, String> {
    private static final Logger LOG = LoggerFactory.getLogger(FormatAsStringFn.class);

    @ProcessElement
    public void process(ProcessContext c) {
      String row = c.element().getKey()
          + " - " + c.element().getValue()
          + " @ " + c.timestamp().toString();
      LOG.debug("output {}", row);
      c.output(row);
    }
  }


  public static void main(String[] args) {
    GearpumpPipelineOptions options = PipelineOptionsFactory
            .fromArgs(args).as(GearpumpPipelineOptions.class);
    options.setRunner(GearpumpRunner.class);
    options.setApplicationName("StreamingWordCount");
    options.setParallelism(1);

    Pipeline p = Pipeline.create(options);

    PCollection<KV<String, Long>> wordCounts =
        p.apply(Read.from(new UnboundedTextSource()))
            .apply(ParDo.of(new ExtractWordsFn()))
            .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(10))))
            .apply(Count.<String>perElement());

    wordCounts.apply(ParDo.of(new FormatAsStringFn()));

    p.run();

    ClientContext clientContext = options.getClientContext();
    clientContext.close();

  }
}
