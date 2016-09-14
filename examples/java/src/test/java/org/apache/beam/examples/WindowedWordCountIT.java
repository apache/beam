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

import java.io.IOException;
import org.apache.beam.examples.WindowedWordCount.Options;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.StreamingIT;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-end integration test of {@link WindowedWordCount}.
 */
@RunWith(JUnit4.class)
public class WindowedWordCountIT {

  /**
   * Options for the {@link WindowedWordCount} Integration Test.
   */
  public interface WindowedWordCountITOptions
      extends Options, TestPipelineOptions, StreamingOptions {
  }

  @Test
  public void testWindowedWordCountInBatch() throws IOException {
    testWindowedWordCountPipeline(false /* isStreaming */);
  }

  @Test
  @Category(StreamingIT.class)
  public void testWindowedWordCountInStreaming() throws IOException {
    testWindowedWordCountPipeline(true /* isStreaming */);
  }

  private void testWindowedWordCountPipeline(boolean isStreaming) throws IOException {
    PipelineOptionsFactory.register(WindowedWordCountITOptions.class);
    WindowedWordCountITOptions options =
        TestPipeline.testingPipelineOptions().as(WindowedWordCountITOptions.class);
    options.setStreaming(isStreaming);

    WindowedWordCount.main(TestPipeline.convertToArgs(options));
  }
}
