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
package org.apache.beam.runners.apex.examples;

import java.io.File;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.ApexRunner;
import org.apache.beam.runners.apex.ApexRunnerResult;
import org.apache.beam.runners.apex.TestApexRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

/** Windowed word count example on Apex runner. */
public class WordCountTest {

  static class FormatAsStringFn extends DoFn<KV<String, Long>, String> {
    private static final long serialVersionUID = 1L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      String row =
          c.element().getKey() + " - " + c.element().getValue() + " @ " + c.timestamp().toString();
      c.output(row);
    }
  }

  static class ExtractWordsFn extends DoFn<String, String> {
    private static final long serialVersionUID = 1L;
    private final Counter emptyLines = Metrics.counter("main", "emptyLines");

    @SuppressWarnings("StringSplitter")
    @ProcessElement
    public void processElement(ProcessContext c) {
      if (c.element().trim().isEmpty()) {
        emptyLines.inc(1);
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

  /** Options for word count example. */
  public interface WordCountOptions extends ApexPipelineOptions {
    @Description("Path of the file to read from")
    @Validation.Required
    String getInputFile();

    void setInputFile(String value);

    @Description("Path of the file to write to")
    @Validation.Required
    String getOutput();

    void setOutput(String value);
  }

  static void runWordCount(WordCountOptions options) {
    Pipeline p = Pipeline.create(options);
    p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
        .apply(ParDo.of(new ExtractWordsFn()))
        .apply(Count.perElement())
        .apply(ParDo.of(new FormatAsStringFn()))
        .apply("WriteCounts", TextIO.write().to(options.getOutput()));
    p.run().waitUntilFinish();
  }

  public static void main(String[] args) {
    WordCountOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);

    runWordCount(options);
  }

  @Test
  public void testWordCountExample() throws Exception {
    PipelineOptionsFactory.register(WordCountOptions.class);
    WordCountOptions options = TestPipeline.testingPipelineOptions().as(WordCountOptions.class);
    options.setRunner(TestApexRunner.class);
    options.setApplicationName("StreamingWordCount");
    String inputFile = WordCountTest.class.getResource("/words.txt").getFile();
    options.setInputFile(new File(inputFile).getAbsolutePath());
    String outputFilePrefix = "target/wordcountresult.txt";
    options.setOutput(outputFilePrefix);

    File outFile1 = new File(outputFilePrefix + "-00000-of-00002");
    File outFile2 = new File(outputFilePrefix + "-00001-of-00002");
    Assert.assertTrue(!outFile1.exists() || outFile1.delete());
    Assert.assertTrue(!outFile2.exists() || outFile2.delete());

    WordCountTest.runWordCount(options);

    Assert.assertTrue("result files exist", outFile1.exists() && outFile2.exists());
    HashSet<String> results = new HashSet<>();
    results.addAll(Files.readAllLines(outFile1.toPath()));
    results.addAll(Files.readAllLines(outFile2.toPath()));
    HashSet<String> expectedOutput =
        Sets.newHashSet(
            "foo - 5 @ 294247-01-09T04:00:54.775Z", "bar - 5 @ 294247-01-09T04:00:54.775Z");
    Assert.assertEquals("expected output", expectedOutput, results);
  }

  static class CollectResultsFn extends DoFn<KV<String, Long>, String> {
    static final ConcurrentHashMap<String, Long> RESULTS = new ConcurrentHashMap<>();

    @ProcessElement
    public void processElement(ProcessContext c) {
      RESULTS.put(c.element().getKey(), c.element().getValue());
    }
  }

  @Test
  public void testWindowedWordCount() throws Exception {
    String[] args = new String[] {"--runner=" + ApexRunner.class.getName()};
    ApexPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(ApexPipelineOptions.class);
    options.setApplicationName("StreamingWordCount");
    Pipeline p = Pipeline.create(options);

    PCollection<KV<String, Long>> wordCounts =
        p.apply(Read.from(new UnboundedTextSource()))
            .apply(ParDo.of(new ExtractWordsFn()))
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))
            .apply(Count.perElement());

    wordCounts.apply(ParDo.of(new CollectResultsFn()));

    ApexRunnerResult result = (ApexRunnerResult) p.run();
    Assert.assertNotNull(result.getApexDAG().getOperatorMeta("Read(UnboundedTextSource)"));
    long timeout = System.currentTimeMillis() + 30000;
    while (System.currentTimeMillis() < timeout) {
      if (CollectResultsFn.RESULTS.containsKey("foo")
          && CollectResultsFn.RESULTS.containsKey("bar")) {
        break;
      }
      result.waitUntilFinish(Duration.millis(1000));
    }
    result.cancel();
    Assert.assertTrue(
        CollectResultsFn.RESULTS.containsKey("foo") && CollectResultsFn.RESULTS.containsKey("bar"));
    CollectResultsFn.RESULTS.clear();
  }
}
