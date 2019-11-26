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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.NumberedShardedFile;
import org.apache.beam.sdk.util.ShardedFile;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.HashCode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Matcher to verify file checksum in E2E test.
 *
 * <p>For example:
 *
 * <pre>{@code
 * assertThat(job, new FileChecksumMatcher(checksumString, filePath));
 * }</pre>
 *
 * or
 *
 * <pre>{@code
 * assertThat(job, new FileChecksumMatcher(checksumString, filePath, shardTemplate));
 * }</pre>
 *
 * <p>Checksum of outputs is generated based on SHA-1 algorithm. If output file is empty, SHA-1 hash
 * of empty string (da39a3ee5e6b4b0d3255bfef95601890afd80709) is used as expected.
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

  private static final Pattern DEFAULT_SHARD_TEMPLATE =
      Pattern.compile("(?x) \\S* (?<shardnum> \\d+) -of- (?<numshards> \\d+)");

  private final String expectedChecksum;
  private final ShardedFile shardedFile;

  /** Access via {@link #getActualChecksum()}. */
  @Nullable private String actualChecksum;

  /**
   * Constructor that uses default shard template.
   *
   * @param checksum expected checksum string used to verify file content.
   * @param filePath path of files that's to be verified.
   */
  public FileChecksumMatcher(String checksum, String filePath) {
    this(checksum, filePath, DEFAULT_SHARD_TEMPLATE);
  }

  /**
   * Constructor using a custom shard template.
   *
   * @param checksum expected checksum string used to verify file content.
   * @param filePath path of files that's to be verified.
   * @param shardTemplate template of shard name to parse out the total number of shards which is
   *     used in I/O retry to avoid inconsistency of filesystem. Customized template should assign
   *     name "numshards" to capturing group - total shard number.
   */
  public FileChecksumMatcher(String checksum, String filePath, Pattern shardTemplate) {
    checkArgument(
        !Strings.isNullOrEmpty(checksum), "Expected valid checksum, but received %s", checksum);
    checkArgument(
        !Strings.isNullOrEmpty(filePath), "Expected valid file path, but received %s", filePath);
    checkNotNull(
        shardTemplate,
        "Expected non-null shard pattern. "
            + "Please call the other constructor to use default pattern: %s",
        DEFAULT_SHARD_TEMPLATE);

    this.expectedChecksum = checksum;
    this.shardedFile = new NumberedShardedFile(filePath, shardTemplate);
  }

  /**
   * Constructor using an entirely custom {@link ShardedFile} implementation.
   *
   * <p>For internal use only.
   */
  public FileChecksumMatcher(String expectedChecksum, ShardedFile shardedFile) {
    this.expectedChecksum = expectedChecksum;
    this.shardedFile = shardedFile;
  }

  @Override
  public boolean matchesSafely(PipelineResult pipelineResult) {
    return getActualChecksum().equals(expectedChecksum);
  }

  /**
   * Computes a checksum of the sharded file specified in the constructor. Not safe to call until
   * the writing is complete.
   */
  private String getActualChecksum() {
    if (actualChecksum == null) {
      // Load output data
      List<String> outputs;
      try {
        outputs = shardedFile.readFilesWithRetries(Sleeper.DEFAULT, BACK_OFF_FACTORY.backoff());
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to read from: %s", shardedFile), e);
      }

      // Verify outputs. Checksum is computed using SHA-1 algorithm
      actualChecksum = computeHash(outputs);
      LOG.debug("Generated checksum: {}", actualChecksum);
    }

    return actualChecksum;
  }

  private static String computeHash(@Nonnull List<String> strs) {
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
    description.appendText("Expected checksum is (").appendText(expectedChecksum).appendText(")");
  }

  @Override
  public void describeMismatchSafely(PipelineResult pResult, Description description) {
    description.appendText("was (").appendText(getActualChecksum()).appendText(")");
  }
}
