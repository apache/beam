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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.examples.common.WriteOneFilePerWindow.PerWindowFiles;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.apache.beam.sdk.testing.StreamingIT;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.ExplicitShardedFile;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.NumberedShardedFile;
import org.apache.beam.sdk.util.ShardedFile;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
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

/** End-to-end integration test of {@link WindowedWordCount}. */
@RunWith(JUnit4.class)
public class WindowedWordCountIT {

  @Rule public TestName testName = new TestName();

  private static final String DEFAULT_INPUT = "gs://apache-beam-samples/shakespeare/sonnets.txt";
  static final int MAX_READ_RETRIES = 4;
  static final Duration DEFAULT_SLEEP_DURATION = Duration.standardSeconds(10L);
  static final FluentBackoff BACK_OFF_FACTORY =
      FluentBackoff.DEFAULT
          .withInitialBackoff(DEFAULT_SLEEP_DURATION)
          .withMaxRetries(MAX_READ_RETRIES);

  public static WordCountsMatcher containsWordCounts(SortedMap<String, Long> expectedWordCounts) {
    return new WordCountsMatcher(expectedWordCounts);
  }

  /** Options for the {@link WindowedWordCount} Integration Test. */
  public interface WindowedWordCountITOptions
      extends WindowedWordCount.Options, TestPipelineOptions, StreamingOptions {}

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
  }

  @Test
  public void testWindowedWordCountInBatchDynamicSharding() throws Exception {
    WindowedWordCountITOptions options = batchOptions();
    // This is the default value, but make it explicit.
    options.setNumShards(null);
    testWindowedWordCountPipeline(options);
  }

  @Test
  public void testWindowedWordCountInBatchStaticSharding() throws Exception {
    WindowedWordCountITOptions options = batchOptions();
    options.setNumShards(3);
    testWindowedWordCountPipeline(options);
  }

  // TODO: add a test with streaming and dynamic sharding after resolving
  // https://issues.apache.org/jira/browse/BEAM-1438

  @Test
  @Category(StreamingIT.class)
  public void testWindowedWordCountInStreamingStaticSharding() throws Exception {
    WindowedWordCountITOptions options = streamingOptions();
    options.setNumShards(3);
    testWindowedWordCountPipeline(options);
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
        FileSystems.matchNewResource(options.getTempRoot(), true)
            .resolve(
                String.format(
                    "WindowedWordCountIT.%s-%tFT%<tH:%<tM:%<tS.%<tL+%s",
                    testName.getMethodName(), new Date(), ThreadLocalRandom.current().nextInt()),
                StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("output", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("results", StandardResolveOptions.RESOLVE_FILE)
            .toString());
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

    ResourceId output = FileBasedSink.convertToFileResourceIfPossible(options.getOutput());
    PerWindowFiles filenamePolicy = new PerWindowFiles(output);

    List<ShardedFile> expectedOutputFiles = Lists.newArrayListWithCapacity(6);

    for (int startMinute : ImmutableList.of(0, 10, 20, 30, 40, 50)) {
      final Instant windowStart =
          new Instant(options.getMinTimestampMillis()).plus(Duration.standardMinutes(startMinute));
      String filePrefix =
          filenamePolicy.filenamePrefixForWindow(
              new IntervalWindow(windowStart, windowStart.plus(Duration.standardMinutes(10))));
      expectedOutputFiles.add(
          new NumberedShardedFile(
              output
                      .getCurrentDirectory()
                      .resolve(filePrefix, StandardResolveOptions.RESOLVE_FILE)
                      .toString()
                  + "*"));
    }

    ShardedFile inputFile = new ExplicitShardedFile(Collections.singleton(options.getInputFile()));

    // For this integration test, input is tiny and we can build the expected counts
    SortedMap<String, Long> expectedWordCounts = new TreeMap<>();
    for (String line :
        inputFile.readFilesWithRetries(Sleeper.DEFAULT, BACK_OFF_FACTORY.backoff())) {
      String[] words = line.split(ExampleUtils.TOKENIZER_PATTERN, -1);

      for (String word : words) {
        if (!word.isEmpty()) {
          expectedWordCounts.put(
              word, MoreObjects.firstNonNull(expectedWordCounts.get(word), 0L) + 1L);
        }
      }
    }

    WindowedWordCount.runWindowedWordCount(options);

    assertThat(expectedOutputFiles, containsWordCounts(expectedWordCounts));
  }

  /**
   * A matcher that bakes in expected word counts, so they can be read directly via some other
   * mechanism, and compares a sharded output file with the result.
   */
  private static class WordCountsMatcher extends TypeSafeMatcher<List<ShardedFile>>
      implements SerializableMatcher<List<ShardedFile>> {

    private final SortedMap<String, Long> expectedWordCounts;
    private SortedMap<String, Long> actualCounts;

    private WordCountsMatcher(SortedMap<String, Long> expectedWordCounts) {
      this.expectedWordCounts = expectedWordCounts;
    }

    @Override
    public boolean matchesSafely(List<ShardedFile> outputFiles) {
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
          String[] splits = line.split(": ", -1);
          String word = splits[0];
          long count = Long.parseLong(splits[1]);
          actualCounts.merge(word, count, (a, b) -> a + b);
        }

        return actualCounts.equals(expectedWordCounts);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to read from sharded output: %s due to exception", outputFiles),
            e);
      }
    }

    @Override
    public void describeTo(Description description) {
      equalTo(expectedWordCounts).describeTo(description);
    }

    @Override
    public void describeMismatchSafely(List<ShardedFile> shardedFiles, Description description) {
      equalTo(expectedWordCounts).describeMismatch(actualCounts, description);
    }
  }
}
