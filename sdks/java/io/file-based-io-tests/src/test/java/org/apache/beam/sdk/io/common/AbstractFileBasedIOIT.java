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

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Abstract class for file based IO Integration tests.
 */
public abstract class AbstractFileBasedIOIT {

  protected static IOTestPipelineOptions readTestPipelineOptions() {
    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    return TestPipeline.testingPipelineOptions().as(IOTestPipelineOptions.class);
  }

  protected static String appendTimestampToPrefix(String filenamePrefix) {
    return String.format("%s_%s", filenamePrefix, new Date().getTime());
  }

  protected static Compression parseCompressionType(String compressionType) {
    try {
      return Compression.valueOf(compressionType.toUpperCase());
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException(
          String.format("Unsupported compression type: %s", compressionType));
    }
  }

  protected String getExpectedHashForLineCount(Long lineCount) {
    Map<Long, String> expectedHashes = ImmutableMap.of(
        100_000L, "4c8bb3b99dcc59459b20fefba400d446",
        1_000_000L, "9796db06e7a7960f974d5a91164afff1",
        100_000_000L, "6ce05f456e2fdc846ded2abd0ec1de95"
    );

    String hash = expectedHashes.get(lineCount);
    if (hash == null) {
      throw new UnsupportedOperationException(
          String.format("No hash for that line count: %s", lineCount)
      );
    }
    return hash;
  }

  /**
   * Constructs text lines in files used for testing.
   */
  public static class DeterministicallyConstructTestTextLineFn extends DoFn<Long, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(String.format("IO IT Test line of text. Line seed: %s", c.element()));
    }
  }

  /**
   * Deletes matching files using the FileSystems API.
   */
  public static class DeleteFileFn extends DoFn<String, Void> {

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      MatchResult match = Iterables
          .getOnlyElement(FileSystems.match(Collections.singletonList(c.element())));

      Collection<ResourceId> resourceIds = toResourceIds(match);

      FileSystems.delete(resourceIds);
    }
    private Collection<ResourceId> toResourceIds(MatchResult match) throws IOException {
      return FluentIterable.from(match.metadata())
          .transform(new Function<MatchResult.Metadata, ResourceId>() {
            @Override
            public ResourceId apply(MatchResult.Metadata metadata) {
              return metadata.resourceId();
            }
          }).toList();
    }
  }
}
