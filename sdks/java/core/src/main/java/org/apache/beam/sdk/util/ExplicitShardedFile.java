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

import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.CharStreams;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A sharded file where the file names are simply provided. */
@Internal
public class ExplicitShardedFile implements ShardedFile {

  private static final Logger LOG = LoggerFactory.getLogger(ExplicitShardedFile.class);

  private static final int MAX_READ_RETRIES = 4;
  private static final Duration DEFAULT_SLEEP_DURATION = Duration.standardSeconds(10L);
  static final FluentBackoff BACK_OFF_FACTORY =
      FluentBackoff.DEFAULT
          .withInitialBackoff(DEFAULT_SLEEP_DURATION)
          .withMaxRetries(MAX_READ_RETRIES);

  private final List<Metadata> files;

  /** Constructs an {@link ExplicitShardedFile} for the given files. */
  public ExplicitShardedFile(Collection<String> files) throws IOException {
    this.files = new ArrayList<>();
    for (String file : files) {
      this.files.add(FileSystems.matchSingleFileSpec(file));
    }
  }

  /**
   * Discovers all shards of this file using the provided {@link Sleeper} and {@link BackOff}.
   *
   * <p>Because of eventual consistency, reads may discover no files or fewer files than the
   * explicit list of files implies. In this case, the read is considered to have failed.
   */
  @Override
  public List<String> readFilesWithRetries(Sleeper sleeper, BackOff backOff)
      throws IOException, InterruptedException {
    if (files.isEmpty()) {
      return Collections.emptyList();
    }

    IOException lastException = null;

    do {
      try {
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

  /**
   * Discovers all shards of this file.
   *
   * <p>Because of eventual consistency, reads may discover no files or fewer files than the shard
   * template implies. In this case, the read is considered to have failed.
   */
  public List<String> readFilesWithRetries() throws IOException, InterruptedException {
    return readFilesWithRetries(Sleeper.DEFAULT, BACK_OFF_FACTORY.backoff());
  }

  @Override
  public String toString() {
    return String.format("explicit sharded file (%s)", Joiner.on(", ").join(files));
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
