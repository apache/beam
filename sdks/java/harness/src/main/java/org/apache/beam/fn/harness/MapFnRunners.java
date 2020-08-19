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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables.getOnlyElement;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.control.BundleSplitListener;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.data.BeamFnTimerClient;
import org.apache.beam.fn.harness.data.PCollectionConsumerRegistry;
import org.apache.beam.fn.harness.data.PTransformFunctionRegistry;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.function.ThrowingFunction;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

/**
 * Utilities to create {@code PTransformRunners} which execute simple map functions.
 *
 * <p>Simple map functions are used in a large number of transforms, especially runner-managed
 * transforms, such as map_windows.
 *
 * <p>TODO: Add support for DoFns which are actually user supplied map/lambda functions instead of
 * using the {@link FnApiDoFnRunner} instance.
 */
public abstract class MapFnRunners {

  /** Create a {@link MapFnRunners} where the map function consumes elements directly. */
  public static <InputT, OutputT> PTransformRunnerFactory<?> forValueMapFnFactory(
      ValueMapFnFactory<InputT, OutputT> fnFactory) {
    return new Factory<>(new CompressedValueOnlyMapperFactory<>(fnFactory));
  }

  /**
   * Create a {@link MapFnRunners} where the map function consumes {@link WindowedValue Windowed
   * Values} and produced {@link WindowedValue Windowed Values}.
   *
   * <p>Each {@link WindowedValue} provided to the function produced by the {@link
   * WindowedValueMapFnFactory} will be in exactly one {@link
   * org.apache.beam.sdk.transforms.windowing.BoundedWindow window}.
   */
  public static <InputT, OutputT> PTransformRunnerFactory<?> forWindowedValueMapFnFactory(
      WindowedValueMapFnFactory<InputT, OutputT> fnFactory) {
    return new Factory<>(new ExplodedWindowedValueMapperFactory<>(fnFactory));
  }

  /** A function factory which given a PTransform returns a map function. */
  public interface ValueMapFnFactory<InputT, OutputT> {
    ThrowingFunction<InputT, OutputT> forPTransform(String ptransformId, PTransform pTransform)
        throws IOException;
  }

  /**
   * A function factory which given a PTransform returns a map function over the entire {@link
   * WindowedValue} of input and output elements.
   *
   * <p>{@link WindowedValue Windowed Values} will only ever be in a single window.
   */
  public interface WindowedValueMapFnFactory<InputT, OutputT> {
    ThrowingFunction<WindowedValue<InputT>, WindowedValue<OutputT>> forPTransform(
        String ptransformId, PTransform ptransform) throws IOException;
  }

  /** A factory for {@link MapFnRunners}s. */
  private static class Factory<InputT, OutputT>
      implements PTransformRunnerFactory<Mapper<InputT, OutputT>> {

    private final MapperFactory mapperFactory;

    private Factory(MapperFactory<InputT, OutputT> mapperFactory) {
      this.mapperFactory = mapperFactory;
    }

    @Override
    public Mapper<InputT, OutputT> createRunnerForPTransform(
        PipelineOptions pipelineOptions,
        BeamFnDataClient beamFnDataClient,
        BeamFnStateClient beamFnStateClient,
        BeamFnTimerClient beamFnTimerClient,
        String pTransformId,
        PTransform pTransform,
        Supplier<String> processBundleInstructionId,
        Map<String, PCollection> pCollections,
        Map<String, RunnerApi.Coder> coders,
        Map<String, RunnerApi.WindowingStrategy> windowingStrategies,
        PCollectionConsumerRegistry pCollectionConsumerRegistry,
        PTransformFunctionRegistry startFunctionRegistry,
        PTransformFunctionRegistry finishFunctionRegistry,
        Consumer<ThrowingRunnable> addResetFunction,
        Consumer<ThrowingRunnable> tearDownFunctions,
        Consumer<ProgressRequestCallback> addProgressRequestCallback,
        BundleSplitListener splitListener,
        BundleFinalizer bundleFinalizer)
        throws IOException {

      FnDataReceiver<WindowedValue<InputT>> consumer =
          (FnDataReceiver)
              pCollectionConsumerRegistry.getMultiplexingConsumer(
                  getOnlyElement(pTransform.getOutputsMap().values()));

      Mapper<InputT, OutputT> mapper = mapperFactory.create(pTransformId, pTransform, consumer);

      pCollectionConsumerRegistry.register(
          Iterables.getOnlyElement(pTransform.getInputsMap().values()),
          pTransformId,
          (FnDataReceiver) (FnDataReceiver<WindowedValue<InputT>>) mapper::map);
      return mapper;
    }
  }

  @FunctionalInterface
  private interface MapperFactory<InputT, OutputT> {
    Mapper<InputT, OutputT> create(
        String ptransformId, PTransform ptransform, FnDataReceiver<WindowedValue<OutputT>> outputs)
        throws IOException;
  }

  private interface Mapper<InputT, OutputT> {
    void map(WindowedValue<InputT> input) throws Exception;
  }

  private static class ExplodedWindowedValueMapperFactory<InputT, OutputT>
      implements MapperFactory<InputT, OutputT> {
    private final WindowedValueMapFnFactory<InputT, OutputT> fnFactory;

    private ExplodedWindowedValueMapperFactory(
        WindowedValueMapFnFactory<InputT, OutputT> fnFactory) {
      this.fnFactory = fnFactory;
    }

    @Override
    public Mapper<InputT, OutputT> create(
        String ptransformId, PTransform ptransform, FnDataReceiver<WindowedValue<OutputT>> outputs)
        throws IOException {
      ThrowingFunction<WindowedValue<InputT>, WindowedValue<OutputT>> fn =
          fnFactory.forPTransform(ptransformId, ptransform);
      return input -> {
        for (WindowedValue<InputT> exploded : input.explodeWindows()) {
          outputs.accept(fn.apply(exploded));
        }
      };
    }
  }

  private static class CompressedValueOnlyMapperFactory<InputT, OutputT>
      implements MapperFactory<InputT, OutputT> {
    private final ValueMapFnFactory<InputT, OutputT> fnFactory;

    private CompressedValueOnlyMapperFactory(ValueMapFnFactory<InputT, OutputT> fnFactory) {
      this.fnFactory = fnFactory;
    }

    @Override
    public Mapper<InputT, OutputT> create(
        String ptransformId, PTransform ptransform, FnDataReceiver<WindowedValue<OutputT>> outputs)
        throws IOException {
      ThrowingFunction<InputT, OutputT> fn = fnFactory.forPTransform(ptransformId, ptransform);
      return input -> outputs.accept(input.withValue(fn.apply(input.getValue())));
    }
  }
}
