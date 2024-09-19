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
package org.apache.beam.runners.prism;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.function.Supplier;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.hamcrest.Matchers;
import org.joda.time.Duration;

/**
 * {@link TestPrismRunner} is the recommended {@link PipelineRunner} to use for tests that rely on
 * <a href="https://github.com/apache/beam/tree/master/sdks/go/cmd/prism">sdks/go/cmd/prism</a>. See
 * {@link PrismRunner} for more details.
 */
public class TestPrismRunner extends PipelineRunner<PipelineResult> {

  private final PrismRunner internal;
  private final TestPrismPipelineOptions prismPipelineOptions;

  /**
   * Invoked from {@link Pipeline#run} where {@link TestPrismRunner} instantiates using {@link
   * TestPrismPipelineOptions} configuration details.
   */
  public static TestPrismRunner fromOptions(PipelineOptions options) {
    TestPrismPipelineOptions prismPipelineOptions = options.as(TestPrismPipelineOptions.class);
    PrismRunner delegate = PrismRunner.fromOptions(options);
    return new TestPrismRunner(delegate, prismPipelineOptions);
  }

  private TestPrismRunner(PrismRunner internal, TestPrismPipelineOptions options) {
    this.internal = internal;
    this.prismPipelineOptions = options;
  }

  TestPrismPipelineOptions getTestPrismPipelineOptions() {
    return prismPipelineOptions;
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    PrismPipelineResult result = (PrismPipelineResult) internal.run(pipeline);
    try {
      PipelineResult.State state = getWaitUntilFinishRunnable(result).get();
      assertThat(
          "Pipeline did not succeed. Check Prism logs for further details.",
          state,
          Matchers.is(PipelineResult.State.DONE));
    } catch (RuntimeException e) {
      // This is a temporary workaround to close the Prism process.
      result.getCleanup().run();
      throw new AssertionError(e);
    }
    return result;
  }

  private Supplier<PipelineResult.State> getWaitUntilFinishRunnable(PipelineResult result) {
    if (prismPipelineOptions.getTestTimeoutSeconds() != null) {
      Long testTimeoutSeconds = checkStateNotNull(prismPipelineOptions.getTestTimeoutSeconds());
      return () -> result.waitUntilFinish(Duration.standardSeconds(testTimeoutSeconds));
    }
    return result::waitUntilFinish;
  }
}
