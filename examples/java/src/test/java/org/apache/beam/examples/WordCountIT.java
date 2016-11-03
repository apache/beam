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

package org.apache.beam.examples;

import com.google.common.base.Strings;
import java.util.Date;
import org.apache.beam.examples.WordCount.WordCountOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.FileChecksumMatcher;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-end tests of WordCount.
 */
@RunWith(JUnit4.class)
public class WordCountIT {

  private static final String DEFAULT_OUTPUT_CHECKSUM = "8ae94f799f97cfd1cb5e8125951b32dfb52e1f12";

  /**
   * Options for the WordCount Integration Test.
   *
   * <p>Define expected output file checksum to verify WordCount pipeline result
   * with customized input.
   */
  public interface WordCountITOptions extends TestPipelineOptions, WordCountOptions {
    String getChecksum();
    void setChecksum(String value);
  }

  @Test
  public void testE2EWordCount() throws Exception {
    PipelineOptionsFactory.register(WordCountITOptions.class);
    WordCountITOptions options = TestPipeline.testingPipelineOptions().as(WordCountITOptions.class);

    options.setOutput(IOChannelUtils.resolve(
        options.getTempRoot(),
        String.format("WordCountIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date()),
        "output",
        "results"));

    String outputChecksum =
        Strings.isNullOrEmpty(options.getChecksum())
            ? DEFAULT_OUTPUT_CHECKSUM
            : options.getChecksum();
    options.setOnSuccessMatcher(
        new FileChecksumMatcher(outputChecksum, options.getOutput() + "*"));

    WordCount.main(TestPipeline.convertToArgs(options));
  }
}
