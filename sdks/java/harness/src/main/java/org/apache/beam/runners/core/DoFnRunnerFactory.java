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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.fake.FakeStepContext;
import org.apache.beam.fn.harness.fn.ThrowingConsumer;
import org.apache.beam.fn.harness.fn.ThrowingRunnable;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.dataflow.util.DoFnInfo;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Classes associated with converting {@link RunnerApi.PTransform}s to {@link DoFnRunner}s.
 *
 * <p>TODO: Move DoFnRunners into SDK harness and merge the methods below into it removing this
 * class.
 */
public class DoFnRunnerFactory {

  private static final String URN = "urn:org.apache.beam:dofn:java:0.1";

  /** A registrar which provides a factory to handle Java {@link DoFn}s. */
  @AutoService(PTransformRunnerFactory.Registrar.class)
  public static class Registrar implements
      PTransformRunnerFactory.Registrar {

    @Override
    public Map<String, PTransformRunnerFactory> getPTransformRunnerFactories() {
      return ImmutableMap.of(URN, new Factory());
    }
  }

  /** A factory for {@link DoFnRunner}s. */
  static class Factory<InputT, OutputT>
      implements PTransformRunnerFactory<DoFnRunner<InputT, OutputT>> {

    @Override
    public DoFnRunner<InputT, OutputT> createRunnerForPTransform(
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

      // For every output PCollection, create a map from output name to Consumer
      ImmutableMap.Builder<String, Collection<ThrowingConsumer<WindowedValue<?>>>>
          outputMapBuilder = ImmutableMap.builder();
      for (Map.Entry<String, String> entry : pTransform.getOutputsMap().entrySet()) {
        outputMapBuilder.put(
            entry.getKey(),
            pCollectionIdsToConsumers.get(entry.getValue()));
      }
      ImmutableMap<String, Collection<ThrowingConsumer<WindowedValue<?>>>> outputMap =
          outputMapBuilder.build();

      // Get the DoFnInfo from the serialized blob.
      ByteString serializedFn;
      try {
        serializedFn = pTransform.getSpec().getParameter().unpack(BytesValue.class).getValue();
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException(
            String.format("Unable to unwrap DoFn %s", pTransform.getSpec()), e);
      }
      DoFnInfo<?, ?> doFnInfo =
          (DoFnInfo<?, ?>)
              SerializableUtils.deserializeFromByteArray(serializedFn.toByteArray(), "DoFnInfo");

      // Verify that the DoFnInfo tag to output map matches the output map on the PTransform.
      checkArgument(
          Objects.equals(
              new HashSet<>(Collections2.transform(outputMap.keySet(), Long::parseLong)),
              doFnInfo.getOutputMap().keySet()),
          "Unexpected mismatch between transform output map %s and DoFnInfo output map %s.",
          outputMap.keySet(),
          doFnInfo.getOutputMap());

      ImmutableMultimap.Builder<TupleTag<?>,
          ThrowingConsumer<WindowedValue<OutputT>>> tagToOutput =
          ImmutableMultimap.builder();
      for (Map.Entry<Long, TupleTag<?>> entry : doFnInfo.getOutputMap().entrySet()) {
        @SuppressWarnings({"unchecked", "rawtypes"})
        Collection<ThrowingConsumer<WindowedValue<OutputT>>> consumers =
            (Collection) outputMap.get(Long.toString(entry.getKey()));
        tagToOutput.putAll(entry.getValue(), consumers);
      }

      @SuppressWarnings({"unchecked", "rawtypes"})
      Map<TupleTag<?>, Collection<ThrowingConsumer<WindowedValue<?>>>> tagBasedOutputMap =
          (Map) tagToOutput.build().asMap();

      OutputManager outputManager =
          new OutputManager() {
            Map<TupleTag<?>, Collection<ThrowingConsumer<WindowedValue<?>>>> tupleTagToOutput =
                tagBasedOutputMap;

            @Override
            public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
              try {
                Collection<ThrowingConsumer<WindowedValue<?>>> consumers =
                    tupleTagToOutput.get(tag);
                if (consumers == null) {
                    /* This is a normal case, e.g., if a DoFn has output but that output is not
                     * consumed. Drop the output. */
                  return;
                }
                for (ThrowingConsumer<WindowedValue<?>> consumer : consumers) {
                  consumer.accept(output);
                }
              } catch (Throwable t) {
                throw new RuntimeException(t);
              }
            }
          };

      @SuppressWarnings({"unchecked", "rawtypes", "deprecation"})
      DoFnRunner<InputT, OutputT> runner =
          DoFnRunners.simpleRunner(
              pipelineOptions,
              (DoFn) doFnInfo.getDoFn(),
              NullSideInputReader.empty(), /* TODO */
              outputManager,
              (TupleTag) doFnInfo.getOutputMap().get(doFnInfo.getMainOutput()),
              new ArrayList<>(doFnInfo.getOutputMap().values()),
              new FakeStepContext(),
              (WindowingStrategy) doFnInfo.getWindowingStrategy());

      // Register the appropriate handlers.
      addStartFunction.accept(runner::startBundle);
      for (String pcollectionId : pTransform.getInputsMap().values()) {
        pCollectionIdsToConsumers.put(
            pcollectionId,
            (ThrowingConsumer) (ThrowingConsumer<WindowedValue<InputT>>) runner::processElement);
      }
      addFinishFunction.accept(runner::finishBundle);
      return runner;
    }
  }
}
