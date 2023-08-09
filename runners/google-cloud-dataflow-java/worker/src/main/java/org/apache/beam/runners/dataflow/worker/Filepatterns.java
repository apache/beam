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
package org.apache.beam.runners.dataflow.worker;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/** Utilities for handling filepatterns. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class Filepatterns {
  private static final Pattern AT_N_SPEC = Pattern.compile("@(?<N>\\d+)");

  /**
   * Expands the filepattern containing an {@code @N} wildcard.
   *
   * <p>Returns N filenames with the wildcard replaced with a string of the form {@code
   * 0000i-of-0000N}. For example, for "gs://bucket/file@2.ism", returns an iterable of two elements
   * "gs://bucket/file-00000-of-00002.ism" and "gs://bucket/file-00001-of-00002.ism".
   *
   * <p>The sequence number and N are formatted with the same number of digits (prepended by zeros).
   * with at least 5 digits. N must be smaller than 1 billion.
   *
   * <p>If the filepattern contains no wildcards, returns the filepattern unchanged.
   *
   * @throws IllegalArgumentException if more than one wildcard is detected.
   */
  public static Iterable<String> expandAtNFilepattern(String filepattern) {
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    Matcher match = AT_N_SPEC.matcher(filepattern);
    if (!match.find()) {
      builder.add(filepattern);
    } else {
      int numShards = Integer.parseInt(match.group("N"));
      String formatString = "-%0" + getShardWidth(numShards, filepattern) + "d-of-%05d";
      for (int i = 0; i < numShards; ++i) {
        builder.add(
            AT_N_SPEC.matcher(filepattern).replaceAll(String.format(formatString, i, numShards)));
      }
      if (match.find()) {
        throw new IllegalArgumentException(
            "More than one @N wildcard found in filepattern: " + filepattern);
      }
    }
    return builder.build();
  }

  private static int getShardWidth(int numShards, String filepattern) {
    if (numShards < 100000) {
      return 5;
    }
    if (numShards < 1000000) {
      return 6;
    }
    if (numShards < 10000000) {
      return 7;
    }
    if (numShards < 100000000) {
      return 8;
    }
    if (numShards < 1000000000) {
      return 9;
    }
    throw new IllegalArgumentException(
        "Unsupported number of shards: " + numShards + " in filepattern: " + filepattern);
  }
}
