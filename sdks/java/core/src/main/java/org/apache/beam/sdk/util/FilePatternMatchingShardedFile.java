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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CharStreams;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A sharded file which matches a given file pattern. Note that the file pattern must match at least
 * one file.
 *
 * <p>Note that file matching should only occur once the file system is in a stable state and
 * guaranteed to provide a consistent result during file pattern matching.
 */
@Internal
public class FilePatternMatchingShardedFile implements ShardedFile {

  private static final Logger LOG = LoggerFactory.getLogger(FilePatternMatchingShardedFile.class);

  private static final int MAX_READ_RETRIES = 4;
  private static final Duration DEFAULT_SLEEP_DURATION = Duration.standardSeconds(10L);
  static final FluentBackoff BACK_OFF_FACTORY =
      FluentBackoff.DEFAULT
          .withInitialBackoff(DEFAULT_SLEEP_DURATION)
          .withMaxRetries(MAX_READ_RETRIES);

  private final String filePattern;

  /**
   * Constructs an {@link FilePatternMatchingShardedFile} for the given file pattern. Note that the
   * file pattern must match at least one file.
   *
   * <p>Note that file matching should only occur once the file system is in a stable state and
   * guaranteed to provide a consistent result during file pattern matching.
   */
  public FilePatternMatchingShardedFile(String filePattern) {
    checkArgument(
        !Strings.isNullOrEmpty(filePattern),
        "Expected valid file path, but received %s",
        filePattern);
    this.filePattern = filePattern;
  }

  /** Discovers all shards of this file using the provided {@link Sleeper} and {@link BackOff}. */
  @Override
  public List<String> readFilesWithRetries(Sleeper sleeper, BackOff backOff)
      throws IOException, InterruptedException {
    IOException lastException = null;

    do {
      try {
        Collection<Metadata> files = FileSystems.match(filePattern).metadata();
        LOG.debug(
            "Found file(s) {} by matching the path: {}",
            files.stream()
                .map(Metadata::resourceId)
                .map(ResourceId::getFilename)
                .collect(Collectors.joining(",")),
            filePattern);
        if (files.isEmpty()) {
          continue;
        }
        // Read data from file paths
        return readLines(files);
      } catch (IOException e) {
        // Ignore and retry
        lastException = e;
        LOG.warn("Error in file reading. Ignore and retry.");
      }
    } while (BackOffUtils.next(sleeper, backOff));
    // Failed after max retries
    throw new IOException(
        String.format("Unable to read file(s) after retrying %d times", MAX_READ_RETRIES),
        lastException);
  }

  /** Discovers all shards of this file. */
  public List<String> readFilesWithRetries() throws IOException, InterruptedException {
    return readFilesWithRetries(Sleeper.DEFAULT, BACK_OFF_FACTORY.backoff());
  }

  @Override
  public String toString() {
    return String.format("sharded file matching pattern: %s", filePattern);
  }

  /**
   * Reads all the lines of all the files.
   *
   * <p>Not suitable for use except in testing of small data, since the data size may be far more
   * than can be reasonably processed serially, in-memory, by a single thread.
   */
  @VisibleForTesting
  List<String> readLines(Collection<Metadata> files) throws IOException {
    List<String> allLines = Lists.newArrayList();
    int i = 1;
    for (Metadata file : files) {
      try (Reader reader =
          Channels.newReader(FileSystems.open(file.resourceId()), StandardCharsets.UTF_8.name())) {
        List<String> lines = CharStreams.readLines(reader);
        allLines.addAll(lines);
        LOG.debug("[{} of {}] Read {} lines from file: {}", i, files.size(), lines.size(), file);
      }
      i++;
    }
    return allLines;
  }
}
