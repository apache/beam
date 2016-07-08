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

import com.google.common.base.Joiner;
import com.google.common.io.LineReader;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
    options.setOutput(
        Joiner.on("/")
            .join(
                new String[] {
                  options.getTempRoot(),
                  String.format("WordCountIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date()),
                  "output",
                  "results"
                }));
    options.setOnSuccessMatcher(new WordCountOnSuccessMatcher(options.getOutput() + "*"));

    WordCount.main(TestPipeline.convertToArgs(options));
  }

  /**
   * Matcher for verifying WordCount output data.
   */
  static class WordCountOnSuccessMatcher extends BaseMatcher<PipelineResult>
      implements SerializableMatcher<PipelineResult> {

    private static final Logger LOG = LoggerFactory.getLogger(WordCountOnSuccessMatcher.class);

    private static final String EXPECTED_CHECKSUM = "C780E9466B8635AF1D11B74BBD35233A82908A02";

    private final String outputPath;

    WordCountOnSuccessMatcher(String outputPath) {
      this.outputPath = outputPath;
    }

    @Override
    public boolean matches(Object o) {
      if (o == null || !(o instanceof PipelineResult)) {
        fail(String.format("Expected PipelineResult but received %s", o));
      }

      if (outputPath == null || outputPath.isEmpty()) {
        fail(String.format("Expected valid output path, but received %s", outputPath));
      }

      //Load output data
      LOG.info("Loading actual from path: {}", outputPath);
      List<String> outputs = read(outputPath);

      // Verify checksum of outputs
      Collections.sort(outputs);
      try {
        String checksum = generateChecksum(outputs);
        LOG.info("Generate checksum for output data: {}", checksum);

        return checksum.equals(EXPECTED_CHECKSUM);
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
      }

      return false;
    }

    private List<String> read(String path) {
      List<String> readData = new ArrayList<>();

      try {
        IOChannelFactory factory = IOChannelUtils.getFactory(path);

        // Match inputPath which may contains glob
        Collection<String> files = factory.match(path);

        // Read data from file paths
        for (String file : files) {
          Reader reader = Channels.newReader(factory.open(file), StandardCharsets.UTF_8.name());
          LineReader lineReader = new LineReader(reader);

          String line;
          while ((line = lineReader.readLine()) != null) {
            readData.add(line);
          }

          reader.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

      return readData;
    }

    /**
     * Generate checksum of a string collection using SHA-1 algorithm.
     */
    private String generateChecksum(List<String> lines) throws NoSuchAlgorithmException {
      MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");

      for (String line : lines) {
        messageDigest.update(line.getBytes(StandardCharsets.UTF_8));
      }

      return convertDecimalToHex(messageDigest.digest());
    }

    private String convertDecimalToHex(byte[] numbers) {
      char[] digits = "0123456789ABCDEF".toCharArray();
      StringBuilder hexNum = new StringBuilder();

      for (int i = 0; i < numbers.length; i++) {
        int num = numbers[i] & 0xFF;
        hexNum.append(digits[num >>> 4]);
        hexNum.append(digits[num & 0xF]);
      }

      return hexNum.toString();
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("Expect output checksum should be ").appendValue(EXPECTED_CHECKSUM);
    }
  }
}
