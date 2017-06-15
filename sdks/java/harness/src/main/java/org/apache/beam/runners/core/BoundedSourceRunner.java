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

package org.apache.beam.runners.core;

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.protobuf.BytesValue;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.fn.ThrowingConsumer;
import org.apache.beam.fn.harness.fn.ThrowingRunnable;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source.Reader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * A runner which creates {@link Reader}s for each {@link BoundedSource} sent as an input and
 * executes the {@link Reader}s read loop.
 */
public class BoundedSourceRunner<InputT extends BoundedSource<OutputT>, OutputT> {

  private static final String URN = "urn:org.apache.beam:source:java:0.1";

  /** A registrar which provides a factory to handle Java {@link BoundedSource}s. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements
      PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(URN, new Factory());
    }
  }

  /** A factory for {@link BoundedSourceRunner}. */
  static class Factory<InputT extends BoundedSource<OutputT>, OutputT>
      implements PTransformRunnerFactory<BoundedSourceRunner<InputT, OutputT>> {
    @Override
    public BoundedSourceRunner<InputT, OutputT> createRunnerForPTransform(
        PipelineOptions pipelineOptions,
        BeamFnDataClient beamFnDataClient,
        String pTransformId,
        RunnerApi.PTransform pTransform,
        Supplier<String> processBundleInstructionId,
        Map<String, RunnerApi.PCollection> pCollections,
        Map<String, RunnerApi.Coder> coders,
        Multimap<String, ThrowingConsumer<WindowedValue<?>>> pCollectionIdsToConsumers,
        Consumer<ThrowingRunnable> addStartFunction,
        Consumer<ThrowingRunnable> addFinishFunction) {

      ImmutableList.Builder<ThrowingConsumer<WindowedValue<?>>> consumers = ImmutableList.builder();
      for (String pCollectionId : pTransform.getOutputsMap().values()) {
        consumers.addAll(pCollectionIdsToConsumers.get(pCollectionId));
      }

      @SuppressWarnings({"rawtypes", "unchecked"})
      BoundedSourceRunner<InputT, OutputT> runner = new BoundedSourceRunner(
          pipelineOptions,
          pTransform.getSpec(),
          consumers.build());

      // TODO: Remove and replace with source being sent across gRPC port
      addStartFunction.accept(runner::start);

      ThrowingConsumer runReadLoop =
          (ThrowingConsumer<WindowedValue<InputT>>) runner::runReadLoop;
      for (String pCollectionId : pTransform.getInputsMap().values()) {
        pCollectionIdsToConsumers.put(
            pCollectionId,
            runReadLoop);
      }

      return runner;
    }
  }

  private final PipelineOptions pipelineOptions;
  private final RunnerApi.FunctionSpec definition;
  private final Collection<ThrowingConsumer<WindowedValue<OutputT>>> consumers;

  BoundedSourceRunner(
      PipelineOptions pipelineOptions,
      RunnerApi.FunctionSpec definition,
      Collection<ThrowingConsumer<WindowedValue<OutputT>>> consumers) {
    this.pipelineOptions = pipelineOptions;
    this.definition = definition;
    this.consumers = consumers;
  }

  /**
   * The runner harness is meant to send the source over the Beam Fn Data API which would be
   * consumed by the {@link #runReadLoop}. Drop this method once the runner harness sends the
   * source instead of unpacking it from the data block of the function specification.
   */
  @Deprecated
  public void start() throws Exception {
    try {
      // The representation here is defined as the java serialized representation of the
      // bounded source object packed into a protobuf Any using a protobuf BytesValue wrapper.
      byte[] bytes = definition.getParameter().unpack(BytesValue.class).getValue().toByteArray();
      @SuppressWarnings("unchecked")
      InputT boundedSource =
          (InputT) SerializableUtils.deserializeFromByteArray(bytes, definition.toString());
      runReadLoop(WindowedValue.valueInGlobalWindow(boundedSource));
    } catch (InvalidProtocolBufferException e) {
      throw new IOException(
          String.format("Failed to decode %s, expected %s",
              definition.getParameter().getTypeUrl(), BytesValue.getDescriptor().getFullName()),
          e);
    }
  }

  /**
   * Creates a {@link Reader} for each {@link BoundedSource} and executes the {@link Reader}s
   * read loop. See {@link Reader} for further details of the read loop.
   *
   * <p>Propagates any exceptions caused during reading or processing via a consumer to the
   * caller.
   */
  public void runReadLoop(WindowedValue<InputT> value) throws Exception {
    try (Reader<OutputT> reader = value.getValue().createReader(pipelineOptions)) {
      if (!reader.start()) {
        // Reader has no data, immediately return
        return;
      }
      do {
        // TODO: Should this use the input window as the window for all the outputs?
        WindowedValue<OutputT> nextValue = WindowedValue.timestampedValueInGlobalWindow(
            reader.getCurrent(), reader.getCurrentTimestamp());
        for (ThrowingConsumer<WindowedValue<OutputT>> consumer : consumers) {
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
