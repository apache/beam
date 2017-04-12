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
package org.apache.beam.examples;

import static org.hamcrest.Matchers.equalTo;

import com.google.api.client.util.Sleeper;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.examples.common.WriteOneFilePerWindow.PerWindowFiles;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.FileChecksumMatcher;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.apache.beam.sdk.testing.StreamingIT;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.ExplicitShardedFile;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.NumberedShardedFile;
import org.apache.beam.sdk.util.ShardedFile;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** End-to-end integration test of {@link WindowedWordCount}. */
@RunWith(JUnit4.class)
public class WindowedWordCountIT {

  @Rule public TestName testName = new TestName();

  private static final String DEFAULT_INPUT =
      "gs://apache-beam-samples/shakespeare/sonnets.txt";
  static final int MAX_READ_RETRIES = 4;
  static final Duration DEFAULT_SLEEP_DURATION = Duration.standardSeconds(10L);
  static final FluentBackoff BACK_OFF_FACTORY =
      FluentBackoff.DEFAULT
          .withInitialBackoff(DEFAULT_SLEEP_DURATION)
          .withMaxRetries(MAX_READ_RETRIES);

  /** Options for the {@link WindowedWordCount} Integration Test. */
  public interface WindowedWordCountITOptions
      extends WindowedWordCount.Options, TestPipelineOptions, StreamingOptions {}

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
  }

  @Test
  public void testWindowedWordCountInBatch() throws Exception {
    testWindowedWordCountPipeline(defaultOptions());
  }

  @Test
  @Category(StreamingIT.class)
  public void testWindowedWordCountInStreaming() throws Exception {
    testWindowedWordCountPipeline(streamingOptions());
  }

  private WindowedWordCountITOptions defaultOptions() throws Exception {
    WindowedWordCountITOptions options =
        TestPipeline.testingPipelineOptions().as(WindowedWordCountITOptions.class);
    options.setInputFile(DEFAULT_INPUT);
    options.setTestTimeoutSeconds(1200L);

    options.setMinTimestampMillis(0L);
    options.setMinTimestampMillis(Duration.standardHours(1).getMillis());
    options.setWindowSize(10);

    options.setOutput(
        IOChannelUtils.resolve(
            options.getTempRoot(),
            String.format(
                "WindowedWordCountIT.%s-%tFT%<tH:%<tM:%<tS.%<tL+%s",
                testName.getMethodName(), new Date(), ThreadLocalRandom.current().nextInt()),
            "output",
            "results"));
    return options;
  }

  private WindowedWordCountITOptions streamingOptions() throws Exception {
    WindowedWordCountITOptions options = defaultOptions();
    options.setStreaming(true);
    return options;
  }

  private WindowedWordCountITOptions batchOptions() throws Exception {
    WindowedWordCountITOptions options = defaultOptions();
    // This is the default value, but make it explicit
    options.setStreaming(false);
    return options;
  }

  private void testWindowedWordCountPipeline(WindowedWordCountITOptions options) throws Exception {

    String outputPrefix = options.getOutput();

    PerWindowFiles filenamePolicy = new PerWindowFiles(outputPrefix);

    List<ShardedFile> expectedOutputFiles = Lists.newArrayListWithCapacity(6);

    for (int startMinute : ImmutableList.of(0, 10, 20, 30, 40, 50)) {
      final Instant windowStart =
          new Instant(options.getMinTimestampMillis()).plus(Duration.standardMinutes(startMinute));
      expectedOutputFiles.add(
          new NumberedShardedFile(
              filenamePolicy.filenamePrefixForWindow(
                  new IntervalWindow(
                      windowStart, windowStart.plus(Duration.standardMinutes(10)))) + "*"));
    }

    ShardedFile inputFile = new ExplicitShardedFile(Collections.singleton(options.getInputFile()));

    // For this integration test, input is tiny and we can build the expected counts
    SortedMap<String, Long> expectedWordCounts = new TreeMap<>();
    for (String line :
        inputFile.readFilesWithRetries(Sleeper.DEFAULT, BACK_OFF_FACTORY.backoff())) {
      String[] words = line.split("[^a-zA-Z']+");

      for (String word : words) {
        if (!word.isEmpty()) {
          expectedWordCounts.put(
              word, MoreObjects.firstNonNull(expectedWordCounts.get(word), 0L) + 1L);
        }
      }
    }

    options.setOnSuccessMatcher(
        new WordCountsMatcher(expectedWordCounts, expectedOutputFiles));

    WindowedWordCount.main(TestPipeline.convertToArgs(options));
  }

  /**
   * A matcher that bakes in expected word counts, so they can be read directly via some other
   * mechanism, and compares a sharded output file with the result.
   */
  private static class WordCountsMatcher extends TypeSafeMatcher<PipelineResult>
      implements SerializableMatcher<PipelineResult> {

    private static final Logger LOG = LoggerFactory.getLogger(FileChecksumMatcher.class);

    private final SortedMap<String, Long> expectedWordCounts;
    private final List<ShardedFile> outputFiles;
    private SortedMap<String, Long> actualCounts;

    public WordCountsMatcher(
        SortedMap<String, Long> expectedWordCounts, List<ShardedFile> outputFiles) {
      this.expectedWordCounts = expectedWordCounts;
      this.outputFiles = outputFiles;
    }

    @Override
    public boolean matchesSafely(PipelineResult pipelineResult) {
      try {
        // Load output data
        List<String> outputLines = new ArrayList<>();
        for (ShardedFile outputFile : outputFiles) {
          outputLines.addAll(
              outputFile.readFilesWithRetries(Sleeper.DEFAULT, BACK_OFF_FACTORY.backoff()));
        }

        // Since the windowing is nondeterministic we only check the sums
        actualCounts = new TreeMap<>();
        for (String line : outputLines) {
          String[] splits = line.split(": ");
          String word = splits[0];
          long count = Long.parseLong(splits[1]);

          Long current = actualCounts.get(word);
          if (current == null) {
            actualCounts.put(word, count);
          } else {
            actualCounts.put(word, current + count);
          }
        }

        return actualCounts.equals(expectedWordCounts);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to read from sharded output: %s due to exception",
                outputFiles), e);
      }
    }

    @Override
    public void describeTo(Description description) {
      equalTo(expectedWordCounts).describeTo(description);
    }

    @Override
    public void describeMismatchSafely(PipelineResult pResult, Description description) {
      equalTo(expectedWordCounts).describeMismatch(actualCounts, description);
    }
  }
}
