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

package com.google.cloud.dataflow.examples;

import com.google.cloud.dataflow.examples.WordCount.WordCountOptions;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;


/**
 * An example that verifies word counts in Shakespeare and includes Dataflow best practices.
 *
 * <p> This class, {@link DebuggingWordCount}, is the third in a series of four successively more
 * detailed 'word count' examples. You may first want to take a look at {@link MinimalWordCount}
 * and {@link WordCount}. After you've looked at this example, then see the
 * {@link WindowedWordCount} pipeline, for introduction of additional concepts.
 *
 * <p> Basic concepts, also in the MinimalWordCount and WordCount examples:
 * Reading text files; counting a PCollection; executing a Pipeline both locally
 * and using the Dataflow service; defining DoFns.
 *
 * <p> New Concepts:
 * <pre>
 *   1. Logging to Cloud Logging
 *   2. Controlling Dataflow worker log levels
 *   3. Creating a custom aggregator
 *   4. Testing your Pipeline via DataflowAssert
 * </pre>
 *
 * <p> To execute this pipeline locally, specify general pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 * }
 * </pre>
 *
 * <p> To execute this pipeline using the Dataflow service, specify pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --stagingLocation=gs://YOUR_STAGING_DIRECTORY
 *   --runner=BlockingDataflowPipelineRunner
 * }
 * </pre>
 *
 * <p> Concept #2: Dataflow workers which execute user code are configured to log to Cloud
 * Logging by default at "INFO" log level and higher. One may override log levels for specific
 * logging namespaces by specifying:
 * <pre><code>
 *   --workerLogLevelOverrides={"Name1":"Level1","Name2":"Level2",...}
 * </code></pre>
 * For example, by specifying:
 * <pre><code>
 *   --workerLogLevelOverrides={"com.google.cloud.dataflow.examples":"DEBUG"}
 * </code></pre>
 * when executing this pipeline using the Dataflow service, Cloud Logging would contain only
 * "DEBUG" or higher level logs for the {@code com.google.cloud.dataflow.examples} package in
 * addition to the default "INFO" or higher level logs. In addition, the default Dataflow worker
 * logging configuration can be overridden by specifying
 * {@code --defaultWorkerLogLevel=<one of TRACE, DEBUG, INFO, WARN, ERROR>}. For example,
 * by specifying {@code --defaultWorkerLogLevel=DEBUG} when executing this pipeline with
 * the Dataflow service, Cloud Logging would contain all "DEBUG" or higher level logs. Note
 * that changing the default worker log level to TRACE or DEBUG will significantly increase
 * the amount of logs output.
 *
 * <p> The input file defaults to {@code gs://dataflow-samples/shakespeare/kinglear.txt} and can be
 * overridden with {@code --inputFile}.
 */
public class DebuggingWordCount {
  /** A DoFn that filters for a specific key based upon a regular expression. */
  public static class FilterTextFn extends DoFn<KV<String, Long>, KV<String, Long>> {
    private static final long serialVersionUID = 0;
    /**
     * Concept #1: The logger below uses the fully qualified class name of FilterTextFn
     * as the logger. All log statements emitted by this logger will be referenced by this name
     * and will be visible in the Cloud Logging UI. Learn more at https://cloud.google.com/logging
     * about the Cloud Logging UI.
     */
    private static final Logger LOG = LoggerFactory.getLogger(FilterTextFn.class);

    private final Pattern filter;
    public FilterTextFn(String pattern) {
      filter = Pattern.compile(pattern);
    }

    /**
     * Concept #3: A custom aggregator can track values in your pipeline as it runs. Those
     * values will be displayed in the Dataflow Monitoring UI when this pipeline is run using the
     * Dataflow service. These aggregators below track the number of matched and unmatched words.
     * Learn more at https://cloud.google.com/dataflow/pipelines/dataflow-monitoring-intf about
     * the Dataflow Monitoring UI.
     */
    private final Aggregator<Long, Long> matchedWords =
        createAggregator("matchedWords", new Sum.SumLongFn());
    private final Aggregator<Long, Long> unmatchedWords =
        createAggregator("umatchedWords", new Sum.SumLongFn());

    @Override
    public void processElement(ProcessContext c) {
      if (filter.matcher(c.element().getKey()).matches()) {
        // Log at the "DEBUG" level each element that we match. When executing this pipeline
        // using the Dataflow service, these log lines will appear in the Cloud Logging UI
        // only if the log level is set to "DEBUG" or lower.
        LOG.debug("Matched: " + c.element().getKey());
        matchedWords.addValue(1L);
        c.output(c.element());
      } else {
        // Log at the "TRACE" level each element that is not matched. Different log levels
        // can be used to control the verbosity of logging providing an effective mechanism
        // to filter less important information.
        LOG.trace("Did not match: " + c.element().getKey());
        unmatchedWords.addValue(1L);
      }
    }
  }

  public static void main(String[] args) {
    WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
      .as(WordCountOptions.class);
    Pipeline p = Pipeline.create(options);

    PCollection<KV<String, Long>> filteredWords =
        p.apply(TextIO.Read.named("ReadLines").from(options.getInputFile()))
         .apply(new WordCount.CountWords())
         .apply(ParDo.of(new FilterTextFn("Flourish|stomach")));

    /**
     * Concept #4: DataflowAssert is a set of convenient PTransforms in the style of
     * Hamcrest's collection matchers that can be used when writing Pipeline level tests
     * to validate the contents of PCollections. DataflowAssert is best used in unit tests
     * with small data sets but is demonstrated here as a teaching tool.
     *
     * <p> Below we verify that the set of filtered words matches our expected counts. Note
     * that DataflowAssert does not provide any output and that successful completion of the
     * Pipeline implies that the expectations were met. Learn more at
     * https://cloud.google.com/dataflow/pipelines/testing-your-pipeline on how to test
     * your Pipeline and see {@link DebuggingWordCountTest} for an example unit test.
     */
    List<KV<String, Long>> expectedResults = Arrays.asList(
        KV.of("Flourish", 3L),
        KV.of("stomach", 1L));
    DataflowAssert.that(filteredWords).containsInAnyOrder(expectedResults);

    p.run();
  }
}
