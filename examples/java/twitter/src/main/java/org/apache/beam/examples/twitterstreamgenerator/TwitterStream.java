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
package org.apache.beam.examples.twitterstreamgenerator;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link TwitterStream} pipeline is a streaming pipeline which ingests data in JSON format from
 * Twitter, and outputs the resulting records to console. Stream configurations are specified by the
 * user as template parameters. <br>
 *
 * <p>Concepts: API connectors and streaming; splittable Dofn and watermarking ; logging
 *
 * <p>To execute this pipeline locally, specify key, secret, token, token-secret and filters to
 * filter stream with, for your twitter streaming app.You can also set number of tweets ( use set
 * TweetsCount - default Long.MAX_VALUE ) you wish to stream and/or the number of minutes to run the
 * pipeline ( use set MinutesToRun: default Integer.MAX_VALUE ) :
 *
 * <pre>{@code
 * new TwitterConfig
 *        .Builder()
 *        .setKey("")
 *        .setSecret("")
 *        .setToken("")
 *        .setTokenSecret("")
 *        .setFilters(Arrays.asList("", "")).build()
 * }</pre>
 *
 * <p>To change the runner( does not works on Dataflow ), specify:
 *
 * <pre>{@code
 * --runner=YOUR_SELECTED_RUNNER
 * }</pre>
 *
 * See examples/java/README.md for instructions about how to configure different runners.
 */
public class TwitterStream {

  private static final Logger LOG = LoggerFactory.getLogger(TwitterStream.class);

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args) {
    Pipeline pipeline = Pipeline.create();
    Window.configure()
        .triggering(
            Repeatedly.forever(
                AfterFirst.of(
                    AfterPane.elementCountAtLeast(10),
                    AfterProcessingTime.pastFirstElementInPane()
                        .plusDelayOf(Duration.standardMinutes(2)))));
    PCollection<String> tweetStream =
        pipeline
            .apply(
                "Create Twitter Connection Configuration",
                TwitterIO.readStandardStream(
                    Arrays.asList(
                        new TwitterConfig.Builder()
                            .setKey("")
                            .setSecret("")
                            .setToken("")
                            .setTokenSecret("")
                            .setFilters(Arrays.asList("", ""))
                            .setLanguage("en")
                            .setTweetsCount(10L)
                            .setMinutesToRun(1)
                            .build())))
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))));
    tweetStream.apply(
        "Output Tweets to console",
        ParDo.of(
            new DoFn<String, String>() {
              @ProcessElement
              public void processElement(@Element String element, OutputReceiver<String> receiver) {
                LOG.debug("Output tweets: {}", element);
                receiver.output(element);
              }
            }));

    pipeline.run();
  }
}
