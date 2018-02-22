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

import static org.apache.beam.sdk.io.common.IOTestHelper.getHashForRecordCount;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Contains helper methods for file based IO Integration tests.
 */
public class FileBasedIOITHelper {

  private FileBasedIOITHelper() {
  }

  public static IOTestPipelineOptions readTestPipelineOptions() {
    PipelineOptionsFactory.register(IOTestPipelineOptions.class);
    IOTestPipelineOptions options = TestPipeline
        .testingPipelineOptions()
        .as(IOTestPipelineOptions.class);

    return PipelineOptionsValidator.validate(IOTestPipelineOptions.class, options);
  }

  public static String getExpectedHashForLineCount(int lineCount) {
    Map<Integer, String> expectedHashes = ImmutableMap.of(
        100_000, "4c8bb3b99dcc59459b20fefba400d446",
        1_000_000, "9796db06e7a7960f974d5a91164afff1",
        100_000_000, "6ce05f456e2fdc846ded2abd0ec1de95"
    );

    return getHashForRecordCount(lineCount, expectedHashes);
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

}
