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
package org.apache.beam.examples.complete.TwitterStreamGenerator;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.Status;

/**
 * The {@link TwitterStream} pipeline is a streaming pipeline which ingests data in JSON format from
 * Twitter, and outputs the resulting records to console. Stream configurations are specified by the
 * user as template parameters. <br>
 */
public class TwitterStream {

  /* Logger for class.*/
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
    PCollection<Status> tweetStream =
        pipeline
            .apply(
                "Create Twitter Connection Configuration",
                TwitterIO.readStandardStream(
                    "", "", "", "", Arrays.asList("", ""), "", Long.MAX_VALUE))
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))));
    tweetStream.apply(
        "Output Tweets to console",
        ParDo.of(
            new DoFn<Status, Status>() {
              @ProcessElement
              public void processElement(@Element Status element, OutputReceiver<Status> receiver) {
                LOG.info("Output tweets: " + element.getText());
                receiver.output(element);
              }
            }));

    pipeline.run();
  }
}
