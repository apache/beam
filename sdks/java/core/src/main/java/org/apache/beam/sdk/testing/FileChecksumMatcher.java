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

import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.IOChannelFactory;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Matcher to verify file checksum in E2E test.
 *
 * <p>For example:
 * <pre>{@code
 *   assertThat(job, new FileChecksumMatcher(checksumString, filePath));
 * }</pre>
 * or
 * <pre>{@code
 *   assertThat(job, new FileChecksumMatcher(checksumString, filePath, shardTemplate));
 * }</pre>
 *
 * <p>Checksum of outputs is generated based on SHA-1 algorithm. If output file is empty,
 * SHA-1 hash of empty string (da39a3ee5e6b4b0d3255bfef95601890afd80709) is used as expected.
 */
public class FileChecksumMatcher extends TypeSafeMatcher<PipelineResult>
    implements SerializableMatcher<PipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(FileChecksumMatcher.class);

  static final int MAX_READ_RETRIES = 4;
  static final Duration DEFAULT_SLEEP_DURATION = Duration.standardSeconds(10L);
  static final FluentBackoff BACK_OFF_FACTORY =
      FluentBackoff.DEFAULT
          .withInitialBackoff(DEFAULT_SLEEP_DURATION)
          .withMaxRetries(MAX_READ_RETRIES);

  private static final String DEFAULT_SHARD_TEMPLATE = "\\S*\\d+-of-(\\d+)$";

  private final String expectedChecksum;
  private final String filePath;
  private final Pattern shardTemplate;
  private String actualChecksum;

  public FileChecksumMatcher(String checksum, String filePath) {
    this(checksum, filePath, DEFAULT_SHARD_TEMPLATE);
  }

  public FileChecksumMatcher(String checksum, String filePath, String shardTemplate) {
    checkArgument(
        !Strings.isNullOrEmpty(checksum),
        "Expected valid checksum, but received %s", checksum);
    checkArgument(
        !Strings.isNullOrEmpty(filePath),
        "Expected valid file path, but received %s", filePath);

    this.expectedChecksum = checksum;
    this.filePath = filePath;
    this.shardTemplate =
        Pattern.compile(shardTemplate == null ? DEFAULT_SHARD_TEMPLATE : shardTemplate);
  }

  @Override
  public boolean matchesSafely(PipelineResult pipelineResult) {
    // Load output data
    List<String> outputs;
    try {
      outputs = readFilesWithRetries(Sleeper.DEFAULT, BACK_OFF_FACTORY.backoff());
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to read from: %s", filePath), e);
    }

    // Verify outputs. Checksum is computed using SHA-1 algorithm
    actualChecksum = computeHash(outputs);
    LOG.info("Generated checksum: {}", actualChecksum);

    return actualChecksum.equals(expectedChecksum);
  }

  @VisibleForTesting
  List<String> readFilesWithRetries(Sleeper sleeper, BackOff backOff)
      throws IOException, InterruptedException {
    IOChannelFactory factory = IOChannelUtils.getFactory(filePath);
    IOException lastException = null;

    do {
      try {
        // Match inputPath which may contains glob
        Collection<String> files = factory.match(filePath);
        LOG.info("Found {} file(s) by matching the path: {}", files.size(), filePath);

        if (files.isEmpty() || !checkTotalNumOfFiles(files)) {
          continue;
        }

        // Read data from file paths
        return readLines(files, factory);
      } catch (IOException e) {
        // Ignore and retry
        lastException = e;
        LOG.warn("Error in file reading. Ignore and retry.");
      }
    } while(BackOffUtils.next(sleeper, backOff));
    // Failed after max retries
    throw new IOException(
        String.format("Unable to read file(s) after retrying %d times", MAX_READ_RETRIES),
        lastException);
  }

  @VisibleForTesting
  List<String> readLines(Collection<String> files, IOChannelFactory factory) throws IOException {
    List<String> allLines = Lists.newArrayList();
    int i = 1;
    for (String file : files) {
      try (Reader reader =
               Channels.newReader(factory.open(file), StandardCharsets.UTF_8.name())) {
        List<String> lines = CharStreams.readLines(reader);
        allLines.addAll(lines);
        LOG.info(
            "[{} of {}] Read {} lines from file: {}", i, files.size(), lines.size(), file);
      }
      i++;
    }
    return allLines;
  }

  /**
   * Check if total number of files is correct by comparing with the number that
   * is parsed from shard name using a name template. If no template is specified,
   * "SSSS-of-NNNN" will be used as default, and "NNNN" will be the expected total
   * number of files.
   */
  @VisibleForTesting
  boolean checkTotalNumOfFiles(Collection<String> files) {
    for (String filePath : files.toArray(new String[0])) {
      Path fileName = Paths.get(filePath).getFileName();
      if (fileName == null) {
        // this path has zero elements
        continue;
      }
      Matcher matcher = shardTemplate.matcher(fileName.toString());
      if (!matcher.matches()) {
        // shard name doesn't match the pattern, check with the next shard
        continue;
      }
      // once match, extract total number of shards and compare to file list
      return files.size() == Integer.parseInt(matcher.group(1));
    }
    LOG.warn("No name matches the shard template: {}", shardTemplate);
    return false;
  }

  private String computeHash(@Nonnull List<String> strs) {
    if (strs.isEmpty()) {
      return Hashing.sha1().hashString("", StandardCharsets.UTF_8).toString();
    }

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
