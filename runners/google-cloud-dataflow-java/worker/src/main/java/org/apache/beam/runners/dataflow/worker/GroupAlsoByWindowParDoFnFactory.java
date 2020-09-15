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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.runners.dataflow.util.Structs.getBytes;
import static org.apache.beam.runners.dataflow.util.Structs.getObject;
import static org.apache.beam.runners.dataflow.util.Structs.getString;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.dataflow.model.SideInputInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.MessageWithComponents.RootCase;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.runners.core.construction.WindowingStrategyTranslation;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.util.CloudObjects;
import org.apache.beam.runners.dataflow.util.PropertyNames;
import org.apache.beam.runners.dataflow.worker.util.BatchGroupAlsoByWindowsDoFns;
import org.apache.beam.runners.dataflow.worker.util.WorkerPropertyNames;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.Context;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ParDoFnFactory} to create GroupAlsoByWindowsDoFn instances according to specifications
 * from the Dataflow service.
 */
class GroupAlsoByWindowParDoFnFactory implements ParDoFnFactory {

  private static final Logger LOG = LoggerFactory.getLogger(GroupAlsoByWindowParDoFnFactory.class);

  @Override
  public ParDoFn create(
      PipelineOptions options,
      CloudObject cloudUserFn,
      @Nullable List<SideInputInfo> sideInputInfos,
      TupleTag<?> mainOutputTag,
      Map<TupleTag<?>, Integer> outputTupleTagsToReceiverIndices,
      final DataflowExecutionContext<?> executionContext,
      DataflowOperationContext operationContext)
      throws Exception {
    Map.Entry<TupleTag<?>, Integer> entry =
        Iterables.getOnlyElement(outputTupleTagsToReceiverIndices.entrySet());
    checkArgument(
        entry.getKey().equals(mainOutputTag),
        "Output tags should reference only the main output tag: %s vs %s",
        entry.getKey(),
        mainOutputTag);
    checkArgument(
        entry.getValue() == 0,
        "There should be a single receiver, but using receiver index %s",
        entry.getValue());

    byte[] encodedWindowingStrategy = getBytes(cloudUserFn, PropertyNames.SERIALIZED_FN);
    WindowingStrategy windowingStrategy;
    try {
      windowingStrategy = deserializeWindowingStrategy(encodedWindowingStrategy);
    } catch (Exception e) {
      // Temporarily choose default windowing strategy if fn API is enabled.
      // TODO: Catch block disappears, becoming an error once Python SDK is compliant.
      if (DataflowRunner.hasExperiment(
          options.as(DataflowPipelineDebugOptions.class), "beam_fn_api")) {
        LOG.info("FnAPI: Unable to deserialize windowing strategy, assuming default", e);
        windowingStrategy = WindowingStrategy.globalDefault();
      } else {
        throw e;
      }
    }

    byte[] serializedCombineFn = getBytes(cloudUserFn, WorkerPropertyNames.COMBINE_FN, null);
    AppliedCombineFn<?, ?, ?, ?> combineFn = null;
    if (serializedCombineFn != null) {
      Object combineFnObj =
          SerializableUtils.deserializeFromByteArray(serializedCombineFn, "serialized combine fn");
      checkArgument(
          combineFnObj instanceof AppliedCombineFn,
          "unexpected kind of AppliedCombineFn: " + combineFnObj.getClass().getName());
      combineFn = (AppliedCombineFn<?, ?, ?, ?>) combineFnObj;
    }

    Map<String, Object> inputCoderObject = getObject(cloudUserFn, WorkerPropertyNames.INPUT_CODER);

    Coder<?> inputCoder = CloudObjects.coderFromCloudObject(CloudObject.fromSpec(inputCoderObject));
    checkArgument(
        inputCoder instanceof WindowedValueCoder,
        "Expected WindowedValueCoder for inputCoder, got: " + inputCoder.getClass().getName());
    @SuppressWarnings("unchecked")
    WindowedValueCoder<?> windowedValueCoder = (WindowedValueCoder<?>) inputCoder;

    Coder<?> elemCoder = windowedValueCoder.getValueCoder();
    checkArgument(
        elemCoder instanceof KvCoder,
        "Expected KvCoder for inputCoder, got: " + elemCoder.getClass().getName());
    @SuppressWarnings("unchecked")
    KvCoder<?, ?> kvCoder = (KvCoder<?, ?>) elemCoder;

    boolean isStreamingPipeline = options.as(StreamingOptions.class).isStreaming();

    SideInputReader sideInputReader = NullSideInputReader.empty();
    @Nullable AppliedCombineFn<?, ?, ?, ?> maybeMergingCombineFn = null;
    if (combineFn != null) {
      sideInputReader =
          executionContext.getSideInputReader(
              sideInputInfos, combineFn.getSideInputViews(), operationContext);

      String phase = getString(cloudUserFn, WorkerPropertyNames.PHASE, CombinePhase.ALL);
      checkArgument(
          phase.equals(CombinePhase.ALL) || phase.equals(CombinePhase.MERGE),
          "Unexpected phase: %s",
          phase);
      if (phase.equals(CombinePhase.MERGE)) {
        maybeMergingCombineFn = makeAppliedMergingFunction(combineFn);
      } else {
        maybeMergingCombineFn = combineFn;
      }
    }

    StateInternalsFactory<?> stateInternalsFactory =
        key -> executionContext.getStepContext(operationContext).stateInternals();

    // This will be a GABW Fn for either batch or streaming, with combiner in it or not
    GroupAlsoByWindowFn<?, ?> fn;

    // This will be a FakeKeyedWorkItemCoder for streaming or null for batch
    Coder<?> gabwInputCoder;

    // TODO: do not do this with mess of "if"
    if (isStreamingPipeline) {
      if (maybeMergingCombineFn == null) {
        fn =
            StreamingGroupAlsoByWindowsDoFns.createForIterable(
                windowingStrategy, stateInternalsFactory, ((KvCoder) kvCoder).getValueCoder());
        gabwInputCoder = WindmillKeyedWorkItem.FakeKeyedWorkItemCoder.of(kvCoder);
      } else {
        fn =
            StreamingGroupAlsoByWindowsDoFns.create(
                windowingStrategy,
                stateInternalsFactory,
                (AppliedCombineFn) maybeMergingCombineFn,
                ((KvCoder) kvCoder).getKeyCoder());
        gabwInputCoder =
            WindmillKeyedWorkItem.FakeKeyedWorkItemCoder.of(
                ((AppliedCombineFn) maybeMergingCombineFn).getKvCoder());
      }

    } else {
      if (maybeMergingCombineFn == null) {
        fn =
            BatchGroupAlsoByWindowsDoFns.createForIterable(
                windowingStrategy, stateInternalsFactory, ((KvCoder) kvCoder).getValueCoder());
        gabwInputCoder = null;
      } else {
        fn =
            BatchGroupAlsoByWindowsDoFns.create(
                windowingStrategy, (AppliedCombineFn) maybeMergingCombineFn);
        gabwInputCoder = null;
      }
    }

    // TODO: or anyhow related to it, do not do this with mess of "if"
    if (maybeMergingCombineFn != null) {
      return new GroupAlsoByWindowsParDoFn(
          options,
          fn,
          windowingStrategy,
          ((AppliedCombineFn) maybeMergingCombineFn).getSideInputViews(),
          gabwInputCoder,
          sideInputReader,
          mainOutputTag,
          executionContext.getStepContext(operationContext));
    } else {
      return new GroupAlsoByWindowsParDoFn(
          options,
          fn,
          windowingStrategy,
          null,
          gabwInputCoder,
          sideInputReader,
          mainOutputTag,
          executionContext.getStepContext(operationContext));
    }
  }

  static WindowingStrategy deserializeWindowingStrategy(byte[] encodedWindowingStrategy)
      throws InvalidProtocolBufferException {
    RunnerApi.MessageWithComponents strategyProto =
        RunnerApi.MessageWithComponents.parseFrom(encodedWindowingStrategy);
    checkArgument(
        strategyProto.getRootCase() == RootCase.WINDOWING_STRATEGY,
        "Invalid windowing strategy: %s",
        strategyProto);
    return WindowingStrategyTranslation.fromProto(
        strategyProto.getWindowingStrategy(),
        RehydratedComponents.forComponents(strategyProto.getComponents()));
  }

  private static <K, AccumT>
      AppliedCombineFn<K, AccumT, List<AccumT>, AccumT> makeAppliedMergingFunction(
          AppliedCombineFn<K, ?, AccumT, ?> appliedFn) {
    GlobalCombineFn<AccumT, List<AccumT>, AccumT> mergingCombineFn;
    if (appliedFn.getFn() instanceof CombineFnWithContext) {
      mergingCombineFn =
          new MergingKeyedCombineFnWithContext<>(
              (CombineFnWithContext<?, AccumT, ?>) appliedFn.getFn(),
              appliedFn.getAccumulatorCoder());
    } else {
      mergingCombineFn =
          new MergingCombineFn<>(
              (CombineFn<?, AccumT, ?>) appliedFn.getFn(), appliedFn.getAccumulatorCoder());
    }
    return AppliedCombineFn.<K, AccumT, List<AccumT>, AccumT>withAccumulatorCoder(
        mergingCombineFn,
        ListCoder.of(appliedFn.getAccumulatorCoder()),
        appliedFn.getSideInputViews(),
        KvCoder.of(appliedFn.getKvCoder().getKeyCoder(), appliedFn.getAccumulatorCoder()),
        appliedFn.getWindowingStrategy());
  }

  private static int MAX_ACCUMULATOR_BUFFER_SIZE = 10;

  static class MergingCombineFn<K, AccumT> extends CombineFn<AccumT, List<AccumT>, AccumT> {

    private final CombineFn<?, AccumT, ?> combineFn;
    private final Coder<AccumT> accumCoder;

    MergingCombineFn(CombineFn<?, AccumT, ?> combineFn, Coder<AccumT> accumCoder) {
      this.combineFn = combineFn;
      this.accumCoder = accumCoder;
    }

    @Override
    public List<AccumT> createAccumulator() {
      ArrayList<AccumT> result = new ArrayList<>();
      result.add(this.combineFn.createAccumulator());
      return result;
    }

    @Override
    public List<AccumT> addInput(List<AccumT> accumulator, AccumT input) {
      accumulator.add(input);
      if (accumulator.size() < MAX_ACCUMULATOR_BUFFER_SIZE) {
        return accumulator;
      } else {
        return mergeToSingleton(accumulator);
      }
    }

    @Override
    public List<AccumT> mergeAccumulators(Iterable<List<AccumT>> accumulators) {
      return mergeToSingleton(Iterables.concat(accumulators));
    }

    @Override
    public List<AccumT> compact(List<AccumT> accumulator) {
      return mergeToSingleton(accumulator);
    }

    @Override
    public AccumT extractOutput(List<AccumT> accumulator) {
      if (accumulator.isEmpty()) {
        return combineFn.createAccumulator();
      } else {
        return combineFn.mergeAccumulators(accumulator);
      }
    }

    private List<AccumT> mergeToSingleton(Iterable<AccumT> accumulators) {
      List<AccumT> singleton = new ArrayList<>();
      singleton.add(combineFn.mergeAccumulators(accumulators));
      return singleton;
    }

    @Override
    public Coder<List<AccumT>> getAccumulatorCoder(CoderRegistry registry, Coder<AccumT> inputCoder)
        throws CannotProvideCoderException {
      return ListCoder.of(accumCoder);
    }
  }

  static class MergingKeyedCombineFnWithContext<K, AccumT>
      extends CombineFnWithContext<AccumT, List<AccumT>, AccumT> {

    private final CombineFnWithContext<?, AccumT, ?> combineFnWithContext;
    private final Coder<AccumT> accumCoder;

    MergingKeyedCombineFnWithContext(
        CombineFnWithContext<?, AccumT, ?> combineFnWithContext, Coder<AccumT> accumCoder) {
      this.combineFnWithContext = combineFnWithContext;
      this.accumCoder = accumCoder;
    }

    @Override
    public List<AccumT> createAccumulator(Context c) {
      ArrayList<AccumT> result = new ArrayList<>();
      result.add(this.combineFnWithContext.createAccumulator(c));
      return result;
    }

    @Override
    public List<AccumT> addInput(List<AccumT> accumulator, AccumT input, Context c) {
      accumulator.add(input);
      if (accumulator.size() < MAX_ACCUMULATOR_BUFFER_SIZE) {
        return accumulator;
      } else {
        return mergeToSingleton(accumulator, c);
      }
    }

    @Override
    public List<AccumT> mergeAccumulators(Iterable<List<AccumT>> accumulators, Context c) {
      return mergeToSingleton(Iterables.concat(accumulators), c);
    }

    @Override
    public List<AccumT> compact(List<AccumT> accumulator, Context c) {
      return mergeToSingleton(accumulator, c);
    }

    @Override
    public AccumT extractOutput(List<AccumT> accumulator, Context c) {
      if (accumulator.isEmpty()) {
        return combineFnWithContext.createAccumulator(c);
      } else {
        return combineFnWithContext.mergeAccumulators(accumulator, c);
      }
    }

    private List<AccumT> mergeToSingleton(Iterable<AccumT> accumulators, Context c) {
      List<AccumT> singleton = new ArrayList<>();
      singleton.add(combineFnWithContext.mergeAccumulators(accumulators, c));
      return singleton;
    }

    @Override
    public Coder<List<AccumT>> getAccumulatorCoder(CoderRegistry registry, Coder<AccumT> inputCoder)
        throws CannotProvideCoderException {
      return ListCoder.of(accumCoder);
    }
  }
}
