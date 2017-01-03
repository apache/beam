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
package org.apache.beam.runners.flink.examples.streaming;

import java.io.IOException;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.UnboundedSocketSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To run the example, first open a socket on a terminal by executing the command:
 * <ul>
 *   <li><code>nc -lk 9999</code>
 * </ul>
 * and then launch the example. Now whatever you type in the terminal is going to be
 * the input to the program.
 * */
public class WindowedWordCount {

  private static final Logger LOG = LoggerFactory.getLogger(WindowedWordCount.class);

  static final long WINDOW_SIZE = 10;  // Default window duration in seconds
  static final long SLIDE_SIZE = 5;  // Default window slide in seconds

  static class FormatAsStringFn extends DoFn<KV<String, Long>, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      String row = c.element().getKey() + " - " + c.element().getValue() + " @ "
          + c.timestamp().toString();
      c.output(row);
    }
  }

  static class ExtractWordsFn extends DoFn<String, String> {
    private final Aggregator<Long, Long> emptyLines =
        createAggregator("emptyLines", Sum.ofLongs());

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.element().trim().isEmpty()) {
        emptyLines.addValue(1L);
      }

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

  /**
   * Pipeline options.
   */
  public interface StreamingWordCountOptions
      extends org.apache.beam.runners.flink.examples.WordCount.Options {
    @Description("Sliding window duration, in seconds")
    @Default.Long(WINDOW_SIZE)
    Long getWindowSize();

    void setWindowSize(Long value);

    @Description("Window slide, in seconds")
    @Default.Long(SLIDE_SIZE)
    Long getSlide();

    void setSlide(Long value);
  }

  public static void main(String[] args) throws IOException {
    StreamingWordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(StreamingWordCountOptions.class);
    options.setStreaming(true);
    options.setWindowSize(10L);
    options.setSlide(5L);
    options.setCheckpointingInterval(1000L);
    options.setNumberOfExecutionRetries(5);
    options.setExecutionRetryDelay(3000L);
    options.setRunner(FlinkRunner.class);

    LOG.info("Windpwed WordCount with Sliding Windows of " + options.getWindowSize()
        + " sec. and a slide of " + options.getSlide());

    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> words = pipeline
        .apply("StreamingWordCount",
            Read.from(new UnboundedSocketSource<>("localhost", 9999, '\n', 3)))
        .apply(ParDo.of(new ExtractWordsFn()))
        .apply(Window.<String>into(SlidingWindows.of(
            Duration.standardSeconds(options.getWindowSize()))
            .every(Duration.standardSeconds(options.getSlide())))
            .triggering(AfterWatermark.pastEndOfWindow()).withAllowedLateness(Duration.ZERO)
            .discardingFiredPanes());

    PCollection<KV<String, Long>> wordCounts =
        words.apply(Count.<String>perElement());

    wordCounts.apply(ParDo.of(new FormatAsStringFn()))
        .apply(TextIO.Write.to("./outputWordCount.txt"));

    pipeline.run();
  }
}
