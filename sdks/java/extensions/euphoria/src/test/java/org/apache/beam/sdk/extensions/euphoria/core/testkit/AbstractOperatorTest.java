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
package org.apache.beam.sdk.extensions.euphoria.core.testkit;

import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.accumulators.SingleJvmAccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.accumulators.SnapshotProvider;
import org.apache.beam.sdk.extensions.euphoria.core.translate.EuphoriaOptions;
import org.apache.beam.sdk.extensions.kryo.KryoCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Base class for test description of a test case. */
public abstract class AbstractOperatorTest implements Serializable {

  /**
   * Run all tests with given runner.
   *
   * @param tc the test case to executeSync
   */
  @SuppressWarnings("unchecked")
  public <T> void execute(TestCase<T> tc) {

    final SingleJvmAccumulatorProvider.Factory accumulatorProvider =
        SingleJvmAccumulatorProvider.Factory.get();
    final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    final EuphoriaOptions euphoriaOptions = pipelineOptions.as(EuphoriaOptions.class);
    euphoriaOptions.setAccumulatorProviderFactory(accumulatorProvider);
    final Pipeline pipeline = TestPipeline.create(pipelineOptions);
    pipeline.getCoderRegistry().registerCoderForClass(Object.class, KryoCoder.of(pipelineOptions));
    final PCollection<T> output = tc.getOutput(pipeline);
    tc.validate(output);
    pipeline.run().waitUntilFinish();
    tc.validateAccumulators(accumulatorProvider);
  }

  /** A single test case. */
  protected interface TestCase<T> extends Serializable {

    /**
     * Retrieve flow to be run. Write outputs to given sink.
     *
     * @param pipeline the flow to attach the test logic to
     * @return the output data set representing the result of the test logic
     */
    PCollection<T> getOutput(Pipeline pipeline);

    /**
     * Retrieve expected outputs.
     *
     * <p>These outputs will be compared irrespective of order.
     *
     * @return list of expected outputs that will be compared irrespective of order
     */
    default List<T> getUnorderedOutput() {
      throw new UnsupportedOperationException(
          "Override either `getUnorderedOutput()`, or `validate`");
    }

    /**
     * Validate that the raw output is correct.
     *
     * @param outputs the raw outputs produced by sink
     * @throws AssertionError when the output is not correct
     */
    default void validate(PCollection<T> outputs) throws AssertionError {
      PAssert.that(outputs).containsInAnyOrder(getUnorderedOutput());
    }

    /**
     * Validate accumulators given a provider capturing the accumulated values.
     *
     * @param snapshots the provider of the accumulated values
     */
    default void validateAccumulators(SnapshotProvider snapshots) {}
  }

  /** Abstract {@code TestCase} to be extended by test classes. */
  public abstract static class AbstractTestCase<InputT, OutputT> implements TestCase<OutputT> {

    @Override
    public final PCollection<OutputT> getOutput(Pipeline pipeline) {
      final List<InputT> inputData = getInput();
      final PCollection<InputT> inputDataset =
          pipeline.apply("input", Create.of(inputData)).setTypeDescriptor(getInputType());
      return getOutput(inputDataset);
    }

    protected abstract TypeDescriptor<InputT> getInputType();

    protected abstract PCollection<OutputT> getOutput(PCollection<InputT> input);

    protected abstract List<InputT> getInput();
  }
}
