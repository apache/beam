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

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.control.BundleSplitListener;
import org.apache.beam.fn.harness.control.ProcessBundleHandler;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.data.BeamFnTimerClient;
import org.apache.beam.fn.harness.data.PCollectionConsumerRegistry;
import org.apache.beam.fn.harness.data.PTransformFunctionRegistry;
import org.apache.beam.fn.harness.state.BeamFnStateClient;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.ReadPayload;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.function.ThrowingRunnable;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source.Reader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/**
 * A runner which creates {@link Reader}s for each {@link BoundedSource} sent as an input and
 * executes the {@link Reader}s read loop.
 */
public class BoundedSourceRunner<InputT extends BoundedSource<OutputT>, OutputT> {

  /** A registrar which provides a factory to handle Java {@link BoundedSource}s. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(
          ProcessBundleHandler.JAVA_SOURCE_URN, new Factory(),
          PTransformTranslation.READ_TRANSFORM_URN, new Factory());
    }
  }

  /** A factory for {@link BoundedSourceRunner}. */
  static class Factory<InputT extends BoundedSource<OutputT>, OutputT>
      implements PTransformRunnerFactory<BoundedSourceRunner<InputT, OutputT>> {
    @Override
    public BoundedSourceRunner<InputT, OutputT> createRunnerForPTransform(
        PipelineOptions pipelineOptions,
        BeamFnDataClient beamFnDataClient,
        BeamFnStateClient beamFnStateClient,
        BeamFnTimerClient beamFnTimerClient,
        String pTransformId,
        PTransform pTransform,
        Supplier<String> processBundleInstructionId,
        Map<String, PCollection> pCollections,
        Map<String, Coder> coders,
        Map<String, RunnerApi.WindowingStrategy> windowingStrategies,
        PCollectionConsumerRegistry pCollectionConsumerRegistry,
        PTransformFunctionRegistry startFunctionRegistry,
        PTransformFunctionRegistry finishFunctionRegistry,
        Consumer<ThrowingRunnable> addResetFunction,
        Consumer<ThrowingRunnable> tearDownFunctions,
        Consumer<ProgressRequestCallback> addProgressRequestCallback,
        BundleSplitListener splitListener,
        BundleFinalizer bundleFinalizer) {
      ImmutableList.Builder<FnDataReceiver<WindowedValue<?>>> consumers = ImmutableList.builder();
      for (String pCollectionId : pTransform.getOutputsMap().values()) {
        consumers.add(pCollectionConsumerRegistry.getMultiplexingConsumer(pCollectionId));
      }

      @SuppressWarnings({"rawtypes", "unchecked"})
      BoundedSourceRunner<InputT, OutputT> runner =
          new BoundedSourceRunner(pipelineOptions, pTransform.getSpec(), consumers.build());

      // TODO: Remove and replace with source being sent across gRPC port
      startFunctionRegistry.register(pTransformId, runner::start);

      FnDataReceiver runReadLoop = (FnDataReceiver<WindowedValue<InputT>>) runner::runReadLoop;
      for (String pCollectionId : pTransform.getInputsMap().values()) {
        pCollectionConsumerRegistry.register(pCollectionId, pTransformId, runReadLoop);
      }

      return runner;
    }
  }

  private final PipelineOptions pipelineOptions;
  private final RunnerApi.FunctionSpec definition;
  private final Collection<FnDataReceiver<WindowedValue<OutputT>>> consumers;

  BoundedSourceRunner(
      PipelineOptions pipelineOptions,
      RunnerApi.FunctionSpec definition,
      Collection<FnDataReceiver<WindowedValue<OutputT>>> consumers) {
    this.pipelineOptions = pipelineOptions;
    this.definition = definition;
    this.consumers = consumers;
  }

  /**
   * @deprecated The runner harness is meant to send the source over the Beam Fn Data API which
   *     would be consumed by the {@link #runReadLoop}. Drop this method once the runner harness
   *     sends the source instead of unpacking it from the data block of the function specification.
   */
  @Deprecated
  public void start() throws Exception {
    try {
      // The representation here is defined as the java serialized representation of the
      // bounded source object in a ByteString wrapper.
      InputT boundedSource;
      if (definition.getUrn().equals(ProcessBundleHandler.JAVA_SOURCE_URN)) {
        byte[] bytes = definition.getPayload().toByteArray();
        @SuppressWarnings("unchecked")
        InputT boundedSource0 =
            (InputT) SerializableUtils.deserializeFromByteArray(bytes, definition.toString());
        boundedSource = boundedSource0;
      } else if (definition.getUrn().equals(PTransformTranslation.READ_TRANSFORM_URN)) {
        ReadPayload readPayload = ReadPayload.parseFrom(definition.getPayload());
        boundedSource = (InputT) ReadTranslation.boundedSourceFromProto(readPayload);
      } else {
        throw new IllegalArgumentException("Unknown source URN: " + definition.getUrn());
      }
      runReadLoop(WindowedValue.valueInGlobalWindow(boundedSource));
    } catch (InvalidProtocolBufferException e) {
      throw new IOException(String.format("Failed to decode %s", definition.getUrn()), e);
    }
  }

  /**
   * Creates a {@link Reader} for each {@link BoundedSource} and executes the {@link Reader}s read
   * loop. See {@link Reader} for further details of the read loop.
   *
   * <p>Propagates any exceptions caused during reading or processing via a consumer to the caller.
   */
  public void runReadLoop(WindowedValue<InputT> value) throws Exception {
    try (Reader<OutputT> reader = value.getValue().createReader(pipelineOptions)) {
      if (!reader.start()) {
        // Reader has no data, immediately return
        return;
      }
      do {
        // TODO: Should this use the input window as the window for all the outputs?
        WindowedValue<OutputT> nextValue =
            WindowedValue.timestampedValueInGlobalWindow(
                reader.getCurrent(), reader.getCurrentTimestamp());
        for (FnDataReceiver<WindowedValue<OutputT>> consumer : consumers) {
          consumer.accept(nextValue);
        }
      } while (reader.advance());
    }
  }

  @Override
  public String toString() {
    return definition.toString();
  }
}
