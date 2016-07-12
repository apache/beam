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

import static org.junit.Assert.fail;

import org.apache.beam.examples.WordCount.WordCountOptions;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.util.IOChannelFactory;
import org.apache.beam.sdk.util.IOChannelUtils;

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
import java.util.Collections;
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

    IOChannelUtils.registerStandardIOFactories(options);
    options.setOutput(
            IOChannelUtils.resolve(
                    options.getTempRoot(),
                    IOChannelUtils.resolve(
                            String.format("WordCountIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date()),
                            IOChannelUtils.resolve("output", "results"))));
    options.setOnSuccessMatcher(new WordCountOnSuccessMatcher(options.getOutput() + "*"));

    WordCount.main(TestPipeline.convertToArgs(options));
  }

  /**
   * Matcher for verifying WordCount output data.
   */
  static class WordCountOnSuccessMatcher extends TypeSafeMatcher<PipelineResult>
      implements SerializableMatcher<PipelineResult> {

    private static final Logger LOG = LoggerFactory.getLogger(WordCountOnSuccessMatcher.class);

    private static final String EXPECTED_CHECKSUM = "0723901e2878b2913c548b3a497b9c86b71a5708";
    private String actualChecksum;

    private final String outputPath;

    WordCountOnSuccessMatcher(String outputPath) {
      this.outputPath = outputPath;
    }

    @Override
    protected boolean matchesSafely(PipelineResult pResult) {
      if (outputPath == null || outputPath.isEmpty()) {
        fail(String.format("Expected valid output path, but received %s", outputPath));
      }

      try {
        // Load output data
        LOG.info("Loading from path: {}", outputPath);
        List<String> outputs = readLines(outputPath);

        // Verify outputs. Checksum is computed using SHA-1 algorithm
        Collections.sort(outputs);
        actualChecksum =
                Hashing.sha1().hashString(outputs.toString(), StandardCharsets.UTF_8).toString();
        LOG.info("Generate checksum for output data: {}", actualChecksum);

        return actualChecksum.equals(EXPECTED_CHECKSUM);
      } catch (IOException e) {
        throw new RuntimeException(
                String.format("Fail to read from path: %s", outputPath));
      }
    }

    private List<String> readLines(String path) throws IOException {
      List<String> readData = new ArrayList<>();

        IOChannelFactory factory = IOChannelUtils.getFactory(path);

        // Match inputPath which may contains glob
        Collection<String> files = factory.match(path);

        // Read data from file paths
        for (String file : files) {
          try (Reader reader =
                       Channels.newReader(factory.open(file), StandardCharsets.UTF_8.name())) {
            readData.addAll(CharStreams.readLines(reader));
          }
        }

      return readData;
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
