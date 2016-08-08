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

import org.apache.beam.examples.WordCount.WordCountOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.util.IOChannelFactory;
import org.apache.beam.sdk.util.IOChannelUtils;

import com.google.common.base.Strings;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.CharStreams;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

/**
 * End-to-end tests of WordCount.
 */
@RunWith(JUnit4.class)
public class WordCountIT {

  /**
   * Options for the WordCount Integration Test.
   */
  public interface WordCountITOptions extends TestPipelineOptions, WordCountOptions {
  }

  @Test
  public void testE2EWordCount() throws Exception {
    PipelineOptionsFactory.register(WordCountITOptions.class);
    WordCountITOptions options = TestPipeline.testingPipelineOptions().as(WordCountITOptions.class);

    options.setOutput(IOChannelUtils.resolve(
        options.getTempRoot(),
        String.format("WordCountIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date()),
        "output",
        "results"));
    options.setOnSuccessMatcher(new WordCountOnSuccessMatcher(options.getOutput() + "*"));

    WordCount.main(TestPipeline.convertToArgs(options));
  }

  /**
   * Matcher for verifying WordCount output data.
   */
  static class WordCountOnSuccessMatcher extends TypeSafeMatcher<PipelineResult>
      implements SerializableMatcher<PipelineResult> {

    private static final Logger LOG = LoggerFactory.getLogger(WordCountOnSuccessMatcher.class);

    private static final String EXPECTED_CHECKSUM = "8ae94f799f97cfd1cb5e8125951b32dfb52e1f12";
    private String actualChecksum;

    private final String outputPath;

    WordCountOnSuccessMatcher(String outputPath) {
      checkArgument(
          !Strings.isNullOrEmpty(outputPath),
          "Expected valid output path, but received %s", outputPath);

      this.outputPath = outputPath;
    }

    @Override
    protected boolean matchesSafely(PipelineResult pResult) {
      try {
        // Load output data
        List<String> outputs = readLines(outputPath);

        // Verify outputs. Checksum is computed using SHA-1 algorithm
        actualChecksum = hashing(outputs);
        LOG.info("Generated checksum for output data: {}", actualChecksum);

        return actualChecksum.equals(EXPECTED_CHECKSUM);
      } catch (IOException e) {
        throw new RuntimeException(
            String.format("Failed to read from path: %s", outputPath));
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

    private String hashing(List<String> strs) {
      List<HashCode> hashCodes = new ArrayList<>();
      for (String str : strs) {
        hashCodes.add(Hashing.sha1().hashString(str, StandardCharsets.UTF_8));
      }
      return Hashing.combineUnordered(hashCodes).toString();
    }

    @Override
    public void describeTo(Description description) {
      description
          .appendText("Expected checksum is (")
          .appendText(EXPECTED_CHECKSUM)
          .appendText(")");
    }

    @Override
    protected void describeMismatchSafely(PipelineResult pResult, Description description) {
      description
          .appendText("was (")
          .appendText(actualChecksum)
          .appendText(")");
    }
  }
}
