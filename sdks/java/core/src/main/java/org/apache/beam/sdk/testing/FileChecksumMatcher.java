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

package org.apache.beam.sdk.testing;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.util.IOChannelFactory;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Matcher to verify file checksum in E2E test.
 *
 * <p>For example:
 * <pre>{@code [
 *   assertTrue(job, new FileChecksumMatcher(checksumString, filePath));
 * ]}</pre>
 */
public class FileChecksumMatcher extends TypeSafeMatcher<PipelineResult>
    implements SerializableMatcher<PipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(FileChecksumMatcher.class);

  private final String expectedChecksum;
  private final String filePath;
  private String actualChecksum;

  public FileChecksumMatcher(String checksum, String filePath) {
    checkArgument(
        !Strings.isNullOrEmpty(checksum),
        "Expected valid checksum, but received %s", checksum);
    checkArgument(
        !Strings.isNullOrEmpty(filePath),
        "Expected valid file path, but received %s", filePath);

    this.expectedChecksum = checksum;
    this.filePath = filePath;
  }

  @Override
  public boolean matchesSafely(PipelineResult pipelineResult) {
    try {
      // Load output data
      List<String> outputs = readLines(filePath);

      // Verify outputs. Checksum is computed using SHA-1 algorithm
      actualChecksum = hashing(outputs);
      LOG.info("Generated checksum for output data: {}", actualChecksum);

      return actualChecksum.equals(expectedChecksum);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to read from path: %s", filePath));
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
        .appendText(expectedChecksum)
        .appendText(")");
  }

  @Override
  public void describeMismatchSafely(PipelineResult pResult, Description description) {
    description
        .appendText("was (")
        .appendText(actualChecksum)
        .appendText(")");
  }
}
