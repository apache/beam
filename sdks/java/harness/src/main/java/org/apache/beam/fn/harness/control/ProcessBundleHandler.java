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

package org.apache.beam.fn.harness.control;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;

import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.fn.harness.data.BeamFnDataClient;
import org.apache.beam.fn.harness.fake.FakeAggregatorFactory;
import org.apache.beam.fn.harness.fake.FakeStepContext;
import org.apache.beam.fn.harness.fn.ThrowingConsumer;
import org.apache.beam.fn.harness.fn.ThrowingRunnable;
import org.apache.beam.fn.v1.BeamFnApi;
import org.apache.beam.runners.core.BeamFnDataReadRunner;
import org.apache.beam.runners.core.BeamFnDataWriteRunner;
import org.apache.beam.runners.core.BoundedSourceRunner;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.dataflow.util.DoFnInfo;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.NullSideInputReader;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes {@link org.apache.beam.fn.v1.BeamFnApi.ProcessBundleRequest}s by materializing
 * the set of required runners for each {@link org.apache.beam.fn.v1.BeamFnApi.FunctionSpec},
 * wiring them together based upon the {@code input} and {@code output} map definitions.
 *
 * <p>Finally executes the DAG based graph by starting all runners in reverse topological order,
 * and finishing all runners in forward topological order.
 */
public class ProcessBundleHandler {
  // TODO: What should the initial set of URNs be?
  private static final String DATA_INPUT_URN = "urn:org.apache.beam:source:runner:0.1";
  private static final String DATA_OUTPUT_URN = "urn:org.apache.beam:sink:runner:0.1";
  private static final String JAVA_DO_FN_URN = "urn:org.apache.beam:dofn:java:0.1";
  private static final String JAVA_SOURCE_URN = "urn:org.apache.beam:source:java:0.1";

  private static final Logger LOG = LoggerFactory.getLogger(ProcessBundleHandler.class);

  private final PipelineOptions options;
  private final Function<String, Message> fnApiRegistry;
  private final BeamFnDataClient beamFnDataClient;

  public ProcessBundleHandler(
      PipelineOptions options,
      Function<String, Message> fnApiRegistry,
      BeamFnDataClient beamFnDataClient) {
    this.options = options;
    this.fnApiRegistry = fnApiRegistry;
    this.beamFnDataClient = beamFnDataClient;
  }

  protected <InputT, OutputT> void createConsumersForPrimitiveTransform(
      BeamFnApi.PrimitiveTransform primitiveTransform,
      Supplier<String> processBundleInstructionId,
      Function<BeamFnApi.Target, Collection<ThrowingConsumer<WindowedValue<OutputT>>>> consumers,
      BiConsumer<BeamFnApi.Target, ThrowingConsumer<WindowedValue<InputT>>> addConsumer,
      Consumer<ThrowingRunnable> addStartFunction,
      Consumer<ThrowingRunnable> addFinishFunction) throws IOException {

    BeamFnApi.FunctionSpec functionSpec = primitiveTransform.getFunctionSpec();

    // For every output PCollection, create a map from output name to Consumer
    ImmutableMap.Builder<String, Collection<ThrowingConsumer<WindowedValue<OutputT>>>>
        outputMapBuilder = ImmutableMap.builder();
    for (Map.Entry<String, BeamFnApi.PCollection> entry :
        primitiveTransform.getOutputsMap().entrySet()) {
      outputMapBuilder.put(
          entry.getKey(),
          consumers.apply(
              BeamFnApi.Target.newBuilder()
                  .setPrimitiveTransformReference(primitiveTransform.getId())
                  .setName(entry.getKey())
                  .build()));
    }
    ImmutableMap<String, Collection<ThrowingConsumer<WindowedValue<OutputT>>>> outputMap =
        outputMapBuilder.build();

    // Based upon the function spec, populate the start/finish/consumer information.
    ThrowingConsumer<WindowedValue<InputT>> consumer;
    switch (functionSpec.getUrn()) {
      default:
        BeamFnApi.Target target;
        BeamFnApi.Coder coderSpec;
        throw new IllegalArgumentException(
            String.format("Unknown FunctionSpec %s", functionSpec));

      case DATA_OUTPUT_URN:
        target = BeamFnApi.Target.newBuilder()
            .setPrimitiveTransformReference(primitiveTransform.getId())
            .setName(getOnlyElement(primitiveTransform.getOutputsMap().keySet()))
            .build();
        coderSpec = (BeamFnApi.Coder) fnApiRegistry.apply(
            getOnlyElement(primitiveTransform.getOutputsMap().values()).getCoderReference());
        BeamFnDataWriteRunner<InputT> remoteGrpcWriteRunner =
            new BeamFnDataWriteRunner<>(
                functionSpec,
                processBundleInstructionId,
                target,
                coderSpec,
                beamFnDataClient);
        addStartFunction.accept(remoteGrpcWriteRunner::registerForOutput);
        consumer = remoteGrpcWriteRunner::consume;
        addFinishFunction.accept(remoteGrpcWriteRunner::close);
        break;

      case DATA_INPUT_URN:
        target = BeamFnApi.Target.newBuilder()
            .setPrimitiveTransformReference(primitiveTransform.getId())
            .setName(getOnlyElement(primitiveTransform.getInputsMap().keySet()))
            .build();
        coderSpec = (BeamFnApi.Coder) fnApiRegistry.apply(
            getOnlyElement(primitiveTransform.getOutputsMap().values()).getCoderReference());
        BeamFnDataReadRunner<OutputT> remoteGrpcReadRunner =
            new BeamFnDataReadRunner<>(
                functionSpec,
                processBundleInstructionId,
                target,
                coderSpec,
                beamFnDataClient,
                outputMap);
        addStartFunction.accept(remoteGrpcReadRunner::registerInputLocation);
        consumer = null;
        addFinishFunction.accept(remoteGrpcReadRunner::blockTillReadFinishes);
        break;

      case JAVA_DO_FN_URN:
        DoFnRunner<InputT, OutputT> doFnRunner = createDoFnRunner(functionSpec, outputMap);
        addStartFunction.accept(doFnRunner::startBundle);
        addFinishFunction.accept(doFnRunner::finishBundle);
        consumer = doFnRunner::processElement;
        break;

      case JAVA_SOURCE_URN:
        @SuppressWarnings({"unchecked", "rawtypes"})
        BoundedSourceRunner<BoundedSource<OutputT>, OutputT> sourceRunner =
            createBoundedSourceRunner(functionSpec, outputMap);
        @SuppressWarnings({"unchecked", "rawtypes"})
        ThrowingConsumer<WindowedValue<?>> sourceConsumer =
            (ThrowingConsumer)
                (ThrowingConsumer<WindowedValue<BoundedSource<OutputT>>>)
                    sourceRunner::runReadLoop;
        // TODO: Remove and replace with source being sent across gRPC port
        addStartFunction.accept(sourceRunner::start);
        consumer = (ThrowingConsumer) sourceConsumer;
        break;
    }

    if (consumer != null) {
      for (Map.Entry<String, BeamFnApi.Target.List> entry :
          primitiveTransform.getInputsMap().entrySet()) {
        for (BeamFnApi.Target target : entry.getValue().getTargetList()) {
          addConsumer.accept(target, consumer);
        }
      }
    }
  }

  public BeamFnApi.InstructionResponse.Builder processBundle(BeamFnApi.InstructionRequest request)
      throws Exception {
    BeamFnApi.InstructionResponse.Builder response =
        BeamFnApi.InstructionResponse.newBuilder()
            .setProcessBundle(BeamFnApi.ProcessBundleResponse.getDefaultInstance());

    String bundleId = request.getProcessBundle().getProcessBundleDescriptorReference();
    BeamFnApi.ProcessBundleDescriptor bundleDescriptor =
        (BeamFnApi.ProcessBundleDescriptor) fnApiRegistry.apply(bundleId);

    Multimap<BeamFnApi.Target,
             ThrowingConsumer<WindowedValue<Object>>> outputTargetToConsumer =
             HashMultimap.create();
    List<ThrowingRunnable> startFunctions = new ArrayList<>();
    List<ThrowingRunnable> finishFunctions = new ArrayList<>();
    // We process the primitive transform list in reverse order
    // because we assume that the runner provides it in topologically order.
    // This means that all the start/finish functions will be in reverse topological order.
    for (BeamFnApi.PrimitiveTransform primitiveTransform :
        Lists.reverse(bundleDescriptor.getPrimitiveTransformList())) {
      createConsumersForPrimitiveTransform(
          primitiveTransform,
          request::getInstructionId,
          outputTargetToConsumer::get,
          outputTargetToConsumer::put,
          startFunctions::add,
          finishFunctions::add);
    }

    // Already in reverse order so we don't need to do anything.
    for (ThrowingRunnable startFunction : startFunctions) {
      LOG.debug("Starting function {}", startFunction);
      startFunction.run();
    }

    // Need to reverse this since we want to call finish in topological order.
    for (ThrowingRunnable finishFunction : Lists.reverse(finishFunctions)) {
      LOG.debug("Finishing function {}", finishFunction);
      finishFunction.run();
    }

    return response;
  }

  /**
   * Converts a {@link org.apache.beam.fn.v1.BeamFnApi.FunctionSpec} into a {@link DoFnRunner}.
   */
  private <InputT, OutputT> DoFnRunner<InputT, OutputT> createDoFnRunner(
      BeamFnApi.FunctionSpec functionSpec,
      Map<String, Collection<ThrowingConsumer<WindowedValue<OutputT>>>> outputMap) {
    ByteString serializedFn;
    try {
      serializedFn = functionSpec.getData().unpack(BytesValue.class).getValue();
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(
          String.format("Unable to unwrap DoFn %s", functionSpec), e);
    }
    DoFnInfo<?, ?> doFnInfo =
        (DoFnInfo<?, ?>)
            SerializableUtils.deserializeFromByteArray(serializedFn.toByteArray(), "DoFnInfo");

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
      tagToOutput.putAll(entry.getValue(), outputMap.get(Long.toString(entry.getKey())));
    }
    @SuppressWarnings({"unchecked", "rawtypes"})
    final Map<TupleTag<?>, Collection<ThrowingConsumer<WindowedValue<?>>>> tagBasedOutputMap =
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
            PipelineOptionsFactory.create(), /* TODO */
            (DoFn) doFnInfo.getDoFn(),
            NullSideInputReader.empty(), /* TODO */
            outputManager,
            (TupleTag) doFnInfo.getOutputMap().get(doFnInfo.getMainOutput()),
            new ArrayList<>(doFnInfo.getOutputMap().values()),
            new FakeStepContext(),
            new FakeAggregatorFactory(),
            (WindowingStrategy) doFnInfo.getWindowingStrategy());
    return runner;
  }

  private <InputT extends BoundedSource<OutputT>, OutputT>
      BoundedSourceRunner<InputT, OutputT> createBoundedSourceRunner(
          BeamFnApi.FunctionSpec functionSpec,
          Map<String, Collection<ThrowingConsumer<WindowedValue<OutputT>>>> outputMap) {

    @SuppressWarnings({"rawtypes", "unchecked"})
    BoundedSourceRunner<InputT, OutputT> runner =
        new BoundedSourceRunner(options, functionSpec, outputMap);
    return runner;
  }
}
