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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import org.apache.beam.examples.WindowedWordCount.Options;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.FileChecksumMatcher;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.apache.beam.sdk.testing.StreamingIT;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.util.IOChannelFactory;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** End-to-end integration test of {@link WindowedWordCount}. */
@RunWith(JUnit4.class)
public class WindowedWordCountIT {

  private static final String DEFAULT_OUTPUT_CHECKSUM = "b1f28225ba009c5f9bf57f8b387783ccd653c52c";

  /** Options for the {@link WindowedWordCount} Integration Test. */
  public interface WindowedWordCountITOptions
      extends Options, TestPipelineOptions, StreamingOptions {}

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
    options.setOutput(
        IOChannelUtils.resolve(
            options.getTempRoot(),
            String.format("WordCountIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date()),
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

  private void testWindowedWordCountPipeline(WindowedWordCountITOptions options)
      throws IOException {

    options.setOnSuccessMatcher(
        new CountFilesChecksumMatcher(DEFAULT_OUTPUT_CHECKSUM, options.getOutput() + "*"));

    WindowedWordCount.main(TestPipeline.convertToArgs(options));
  }

  /**
   * A checksum matcher that reads a bunch of text files, splits the lines
   * on colons, adds up the wordcounts, and then checksums the results.
   */
  private static class CountFilesChecksumMatcher extends TypeSafeMatcher<PipelineResult>
      implements SerializableMatcher<PipelineResult> {

    private static final Logger LOG = LoggerFactory.getLogger(FileChecksumMatcher.class);

    private final String expectedChecksum;
    private final String filePath;
    private String actualChecksum;

    public CountFilesChecksumMatcher(String checksum, String filePath) {
      checkArgument(
          !Strings.isNullOrEmpty(checksum), "Expected valid checksum, but received %s", checksum);
      checkArgument(
          !Strings.isNullOrEmpty(filePath), "Expected valid file path, but received %s", filePath);

      this.expectedChecksum = checksum;
      this.filePath = filePath;
    }

    @Override
    public boolean matchesSafely(PipelineResult pipelineResult) {
      try {
        // Load output data
        List<String> lines = readLines(filePath);

        // Since the windowing is nondeterministic we only check the sums
        SortedMap<String, Long> counts = Maps.newTreeMap();
        for (String line : lines) {
          String[] splits = line.split(": ");
          String word = splits[0];
          long count = Long.parseLong(splits[1]);

          Long current = counts.get(word);
          if (current == null) {
            counts.put(word, count);
          } else {
            counts.put(word, current + count);
          }
        }

        // Verify outputs. Checksum is computed using SHA-1 algorithm
        actualChecksum = hashing(counts);
        LOG.info("Generated checksum for output data: {}", actualChecksum);

        return actualChecksum.equals(expectedChecksum);
      } catch (IOException e) {
        throw new RuntimeException(String.format("Failed to read from path: %s", filePath));
      }
    }

    private List<String> readLines(String path) throws IOException {
      List<String> readData = new ArrayList<>();
      IOChannelFactory factory = IOChannelUtils.getFactory(path);

      // Match inputPath which may contains glob
      Collection<String> files = factory.match(path);

      // Read data from file paths
      int i = 0;
      for (String file : files) {
        try (Reader reader =
            Channels.newReader(factory.open(file), StandardCharsets.UTF_8.name())) {
          List<String> lines = CharStreams.readLines(reader);
          readData.addAll(lines);
          LOG.info(
              "[{} of {}] Read {} lines from file: {}", i, files.size() - 1, lines.size(), file);
        }
        i++;
      }
      return readData;
    }

    private String hashing(SortedMap<String, Long> counts) {
      List<HashCode> hashCodes = new ArrayList<>();
      for (Map.Entry<String, Long> entry : counts.entrySet()) {
        hashCodes.add(Hashing.sha1().hashString(entry.getKey(), StandardCharsets.UTF_8));
        hashCodes.add(Hashing.sha1().hashLong(entry.getValue()));
      }
      return Hashing.combineOrdered(hashCodes).toString();
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("Expected checksum is (").appendText(expectedChecksum).appendText(")");
    }

    @Override
    public void describeMismatchSafely(PipelineResult pResult, Description description) {
      description.appendText("was (").appendText(actualChecksum).appendText(")");
    }
  }
}
