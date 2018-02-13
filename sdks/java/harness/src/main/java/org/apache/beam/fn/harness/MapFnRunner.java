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

package org.apache.beam.fn.harness;

import static com.google.common.collect.Iterables.getOnlyElement;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.data.MultiplexingFnDataReceiver;
import org.apache.beam.fn.harness.fn.ThrowingFunction;
import org.apache.beam.fn.harness.fn.ThrowingRunnable;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * A {@code PTransformRunner} which executes simple map functions.
 *
 * <p>Simple map functions are used in a large number of transforms, especially runner-managed
 * transforms, such as map_windows.
 *
 * <p>TODO: Add support for DoFns which are actually user supplied map/lambda functions instead
 * of using the {@link FnApiDoFnRunner} instance.
 */
public class MapFnRunner<InputT, OutputT> {

  public static <InputT, OutputT> PTransformRunnerFactory<?>
      createMapFnRunnerFactoryWith(
          CreateMapFunctionForPTransform<InputT, OutputT> fnFactory) {
    return new Factory<>(fnFactory);
  }

  /** A function factory which given a PTransform returns a map function. */
  public interface CreateMapFunctionForPTransform<InputT, OutputT> {
    ThrowingFunction<InputT, OutputT> createMapFunctionForPTransform(
        String ptransformId,
        PTransform pTransform) throws IOException;
  }

  /** A factory for {@link MapFnRunner}s. */
  static class Factory<InputT, OutputT>
      implements PTransformRunnerFactory<MapFnRunner<InputT, OutputT>> {

    private final CreateMapFunctionForPTransform<InputT, OutputT> fnFactory;

    Factory(CreateMapFunctionForPTransform<InputT, OutputT> fnFactory) {
      this.fnFactory = fnFactory;
    }

    @Override
    public MapFnRunner<InputT, OutputT> createRunnerForPTransform(
        PipelineOptions pipelineOptions,
        BeamFnDataClient beamFnDataClient,
        BeamFnStateClient beamFnStateClient,
        String pTransformId,
        PTransform pTransform,
        Supplier<String> processBundleInstructionId,
        Map<String, PCollection> pCollections,
        Map<String, RunnerApi.Coder> coders,
        Map<String, RunnerApi.WindowingStrategy> windowingStrategies,
        Multimap<String, FnDataReceiver<WindowedValue<?>>> pCollectionIdsToConsumers,
        Consumer<ThrowingRunnable> addStartFunction,
        Consumer<ThrowingRunnable> addFinishFunction) throws IOException {

      Collection<FnDataReceiver<WindowedValue<OutputT>>> consumers =
          (Collection) pCollectionIdsToConsumers.get(
              getOnlyElement(pTransform.getOutputsMap().values()));

      MapFnRunner<InputT, OutputT> runner = new MapFnRunner<>(
          fnFactory.createMapFunctionForPTransform(pTransformId, pTransform),
          MultiplexingFnDataReceiver.forConsumers(consumers));

      pCollectionIdsToConsumers.put(
          Iterables.getOnlyElement(pTransform.getInputsMap().values()),
          (FnDataReceiver) (FnDataReceiver<WindowedValue<InputT>>) runner::map);
      return runner;
    }
  }

  private final ThrowingFunction<InputT, OutputT> mapFunction;
  private final FnDataReceiver<WindowedValue<OutputT>> consumer;

  MapFnRunner(
      ThrowingFunction<InputT, OutputT> mapFunction,
      FnDataReceiver<WindowedValue<OutputT>> consumer) {
    this.mapFunction = mapFunction;
    this.consumer = consumer;
  }

  public void map(WindowedValue<InputT> element) throws Exception {
    WindowedValue<OutputT> output = element.withValue(mapFunction.apply(element.getValue()));
    consumer.accept(output);
  }
}
