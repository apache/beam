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

package org.apache.beam.sdk.testing;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AggregatorRetrievalException;
import org.apache.beam.sdk.runners.AggregatorValues;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.transforms.Aggregator;

/**
 * A {@link PipelineRunner} that applies no overrides and throws an exception on calls to
 * {@link Pipeline#run()}. For use in {@link TestPipeline} to construct but not execute pipelines.
 */
public class CrashingRunner extends PipelineRunner<PipelineResult>{

  public static CrashingRunner fromOptions(PipelineOptions opts) {
    return new CrashingRunner();
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    throw new IllegalArgumentException(String.format("Cannot call #run(Pipeline) on an instance "
            + "of %s. %s should only be used as the default to construct a Pipeline "
            + "using %s, and cannot execute Pipelines. Instead, specify a %s "
            + "by providing PipelineOptions in the environment variable '%s'.",
        getClass().getSimpleName(),
        getClass().getSimpleName(),
        TestPipeline.class.getSimpleName(),
        PipelineRunner.class.getSimpleName(),
        TestPipeline.PROPERTY_BEAM_TEST_PIPELINE_OPTIONS));
  }

  private static class TestPipelineResult implements PipelineResult {
    private TestPipelineResult() {
      // Should never be instantiated by the enclosing class
      throw new UnsupportedOperationException(String.format("Forbidden to instantiate %s",
          getClass().getSimpleName()));
    }

    @Override
    public State getState() {
      throw new UnsupportedOperationException(String.format("Forbidden to instantiate %s",
          getClass().getSimpleName()));
    }

    @Override
    public <T> AggregatorValues<T> getAggregatorValues(Aggregator<?, T> aggregator)
        throws AggregatorRetrievalException {
      throw new AssertionError(String.format("Forbidden to instantiate %s",
          getClass().getSimpleName()));
    }
  }
}
