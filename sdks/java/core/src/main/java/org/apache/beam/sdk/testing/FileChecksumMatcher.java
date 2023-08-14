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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.ShardedFile;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.HashCode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Matcher to verify checksum of the contents of an {@link ShardedFile} in E2E test.
 *
 * <p>For example:
 *
 * <pre>{@code
 * assertThat(new NumberedShardedFile(filePath), fileContentsHaveChecksum(checksumString));
 * }</pre>
 *
 * or
 *
 * <pre>{@code
 * assertThat(new NumberedShardedFile(filePath, shardTemplate), fileContentsHaveChecksum(checksumString));
 * }</pre>
 *
 * <p>Checksum of outputs is generated based on SHA-1 algorithm. If output file is empty, SHA-1 hash
 * of empty string (da39a3ee5e6b4b0d3255bfef95601890afd80709) is used as expected.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class FileChecksumMatcher extends TypeSafeMatcher<ShardedFile>
    implements SerializableMatcher<ShardedFile> {

  private static final Logger LOG = LoggerFactory.getLogger(FileChecksumMatcher.class);

  static final int MAX_READ_RETRIES = 4;
  static final Duration DEFAULT_SLEEP_DURATION = Duration.standardSeconds(10L);
  static final FluentBackoff BACK_OFF_FACTORY =
      FluentBackoff.DEFAULT
          .withInitialBackoff(DEFAULT_SLEEP_DURATION)
          .withMaxRetries(MAX_READ_RETRIES);

  private final String expectedChecksum;
  private String actualChecksum;

  private FileChecksumMatcher(String checksum) {
    checkArgument(
        !Strings.isNullOrEmpty(checksum), "Expected valid checksum, but received %s", checksum);
    this.expectedChecksum = checksum;
  }

  public static FileChecksumMatcher fileContentsHaveChecksum(String checksum) {
    return new FileChecksumMatcher(checksum);
  }

  @Override
  public boolean matchesSafely(ShardedFile shardedFile) {
    return getActualChecksum(shardedFile).equals(expectedChecksum);
  }

  /**
   * Computes a checksum of the given sharded file. Not safe to call until the writing is complete.
   */
  private String getActualChecksum(ShardedFile shardedFile) {
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
  public void describeMismatchSafely(ShardedFile shardedFile, Description description) {
    description.appendText("was (").appendText(actualChecksum).appendText(")");
  }
}
