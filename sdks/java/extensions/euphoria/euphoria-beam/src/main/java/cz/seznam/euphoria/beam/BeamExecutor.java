/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cz.seznam.euphoria.beam;

import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.accumulators.VoidAccumulatorProvider;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.executor.AbstractExecutor;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Executor implementation using Apache Beam as a runtime.
 */
public class BeamExecutor extends AbstractExecutor{

  private final PipelineOptions options;

  private AccumulatorProvider.Factory accumulatorFactory =
      VoidAccumulatorProvider.Factory.get();

  public BeamExecutor(PipelineOptions options) {
    this.options = options;
  }

  protected Result execute(Flow flow) {
    final Pipeline pipeline = FlowTranslator.toPipeline(flow, accumulatorFactory, options);
    final PipelineResult result = pipeline.run();
    // @todo handle result
    result.waitUntilFinish();
    return new Result();
  }

  @Override
  public void setAccumulatorProvider(AccumulatorProvider.Factory accumulatorFactory) {
    this.accumulatorFactory = accumulatorFactory;
  }
}
