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

package org.apache.beam.sdk.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for working with sharded files. For internal use only; many parameters
 * are just hardcoded to allow existing uses to work OK.
 */
public class NumberedShardedFile implements ShardedFile {

  private static final Logger LOG = LoggerFactory.getLogger(NumberedShardedFile.class);

  static final int MAX_READ_RETRIES = 4;
  static final Duration DEFAULT_SLEEP_DURATION = Duration.standardSeconds(10L);
  static final FluentBackoff BACK_OFF_FACTORY =
      FluentBackoff.DEFAULT
          .withInitialBackoff(DEFAULT_SLEEP_DURATION)
          .withMaxRetries(MAX_READ_RETRIES);

  private static final Pattern DEFAULT_SHARD_TEMPLATE =
      Pattern.compile("(?x) \\S* (?<shardnum> \\d+) -of- (?<numshards> \\d+)");

  private final String filePattern;
  private final Pattern shardTemplate;

  /**
   * Constructor that uses default shard template.
   *
   * @param filePattern path or glob of files to include
   */
  public NumberedShardedFile(String filePattern) {
    this(filePattern, DEFAULT_SHARD_TEMPLATE);
  }

  /**
   * Constructor.
   *
   * @param filePattern path or glob of files to include
   * @param shardTemplate template of shard name to parse out the total number of shards
   *                      which is used in I/O retry to avoid inconsistency of filesystem.
   *                      Customized template should assign name "numshards" to capturing
   *                      group - total shard number.
   */
  public NumberedShardedFile(String filePattern, Pattern shardTemplate) {
    checkArgument(
        !Strings.isNullOrEmpty(filePattern),
        "Expected valid file path, but received %s", filePattern);
    checkNotNull(
        shardTemplate,
        "Expected non-null shard pattern. "
            + "Please call the other constructor to use default pattern: %s",
        DEFAULT_SHARD_TEMPLATE);

    this.filePattern = filePattern;
    this.shardTemplate = shardTemplate;
  }

  public String getFilePattern() {
    return filePattern;
  }

  /**
   * Discovers all shards of this file using the provided {@link Sleeper} and {@link BackOff}.
   *
   * <p>Because of eventual consistency, reads may discover no files or fewer files than
   * the shard template implies. In this case, the read is considered to have failed.
   */
  @Override
  public List<String> readFilesWithRetries(Sleeper sleeper, BackOff backOff)
      throws IOException, InterruptedException {
    IOChannelFactory factory = IOChannelUtils.getFactory(filePattern);
    IOException lastException = null;

    do {
      try {
        // Match inputPath which may contains glob
        Collection<String> files = factory.match(filePattern);
        LOG.debug("Found {} file(s) by matching the path: {}", files.size(), filePattern);

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

  /**
   * Discovers all shards of this file using the provided {@link Sleeper} and {@link BackOff}.
   *
   * <p>Because of eventual consistency, reads may discover no files or fewer files than
   * the shard template implies. In this case, the read is considered to have failed.
   */
  public List<String> readFilesWithRetries()
      throws IOException, InterruptedException {
    return readFilesWithRetries(Sleeper.DEFAULT, BACK_OFF_FACTORY.backoff());
  }

  @Override
  public String toString() {
    return String.format("%s with shard template '%s'", filePattern, shardTemplate);
  }

  /**
   * Reads all the lines of all the files.
   *
   * <p>Not suitable for use except in testing of small data, since the data size may be far more
   * than can be reasonably processed serially, in-memory, by a single thread.
   */
  @VisibleForTesting
  List<String> readLines(Collection<String> files, IOChannelFactory factory) throws IOException {
    List<String> allLines = Lists.newArrayList();
    int i = 1;
    for (String file : files) {
      try (Reader reader =
               Channels.newReader(factory.open(file), StandardCharsets.UTF_8.name())) {
        List<String> lines = CharStreams.readLines(reader);
        allLines.addAll(lines);
        LOG.debug(
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
   *
   * @return {@code true} if at least one shard name matches template and total number
   * of given files equals the number that is parsed from shard name.
   */
  @VisibleForTesting
  boolean checkTotalNumOfFiles(Collection<String> files) {
    for (String filePath : files) {
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
      return files.size() == Integer.parseInt(matcher.group("numshards"));
    }
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
}
