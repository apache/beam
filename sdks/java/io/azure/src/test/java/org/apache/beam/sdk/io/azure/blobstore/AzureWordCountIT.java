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
package org.apache.beam.sdk.io.azure.blobstore;

import static org.apache.beam.sdk.testing.FileChecksumMatcher.fileContentsHaveChecksum;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Date;
import org.apache.beam.examples.WordCount;
import org.apache.beam.examples.WordCount.WordCountOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.azure.options.BlobstoreOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.util.NumberedShardedFile;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end tests of WordCount. */
@RunWith(JUnit4.class)
public class AzureWordCountIT {
  private static final String DEFAULT_INPUT = "azfs://ettasaccount/test-container/test-blob";
  // this comes from running with the input file sdks/java/io/azure/test/resources/in.txt
  private static final String DEFAULT_OUTPUT_CHECKSUM = "9725c097a55d3d3d7cc17936b0839c26dbb4991a";

  /**
   * Options for the WordCount Integration Test.
   *
   * <p>Define expected output file checksum to verify WordCount pipeline result with customized
   * input.
   */
  public interface AzureWordCountITOptions
      extends TestPipelineOptions, WordCountOptions, BlobstoreOptions {}

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
  }

  @Test
  public void testE2EWordCount() throws Exception {
    AzureWordCountITOptions options =
        TestPipeline.testingPipelineOptions().as(AzureWordCountITOptions.class);
    options.setAzureConnectionString(System.getenv("AZURE_STORAGE_CONNECTION_STRING"));

    options.setInputFile(DEFAULT_INPUT);
    options.setOutput(
        FileSystems.matchNewResource(options.getTempRoot(), true)
            .resolve(
                String.format("WordCountIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date()),
                StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("output", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("results", StandardResolveOptions.RESOLVE_FILE)
            .toString());
    WordCount.runWordCount(options);
    assertThat(
        new NumberedShardedFile(options.getOutput() + "*-of-*"),
        fileContentsHaveChecksum(DEFAULT_OUTPUT_CHECKSUM));
  }
}
