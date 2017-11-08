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
package org.apache.beam.sdk.io.common;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Used to pass values around within test pipelines for text file based IO.
 */
public abstract class TestTextLine {

  /**
   * Returns the value produced from the given seed.
   */
  private static String getNameForSeed(Long seed) {
    return String.format("IO IT Test line of text. Line seed: %s", seed);
  }

  /**
   * Uses the input Long values as seeds to produce text lines (Strings).
   */
  public static class DeterministicallyConstructTestTextLineFn extends DoFn<Long, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(getNameForSeed(c.element()));
    }
  }
  /**
   * Precalculated hashes for specific amounts of text lines.
   */
  private static final Map<Long, String> EXPECTED_HASHES = ImmutableMap.of(
      100_000L, "4c8bb3b99dcc59459b20fefba400d446",
      1_000_000L, "9796db06e7a7960f974d5a91164afff1"
  );

  public static String getExpectedHashForLineCount(Long lineCount) {
    String hash = EXPECTED_HASHES.get(lineCount);
    if (hash == null) {
      throw new UnsupportedOperationException(
          String.format("No hash for that line count: %s", lineCount)
      );
    }
    return hash;
  }
}
