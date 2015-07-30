/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.util.Structs.getBytes;
import static com.google.cloud.dataflow.sdk.util.Structs.getObject;
import static com.google.cloud.dataflow.sdk.util.Structs.getString;

import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.ListCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.StreamingOptions;
import com.google.cloud.dataflow.sdk.runners.worker.CombineValuesFn.CombinePhase;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.util.AppliedCombineFn;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.DoFnInfo;
import com.google.cloud.dataflow.sdk.util.GroupAlsoByWindowsDoFn;
import com.google.cloud.dataflow.sdk.util.NullSideInputReader;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.Serializer;
import com.google.cloud.dataflow.sdk.util.StreamingGroupAlsoByWindowsDoFn;
import com.google.cloud.dataflow.sdk.util.WindowedValue.WindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoFn;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * A wrapper around a GroupAlsoByWindowsDoFn.  This class is the same as
 * NormalParDoFn, except that it gets deserialized differently.
 */
class GroupAlsoByWindowsParDoFn extends ParDoFnBase {

  static GroupAlsoByWindowsParDoFn of(
      PipelineOptions options,
      DoFn<?, ?> groupAlsoByWindowsDoFn,
      String stepName,
      String transformName,
      DataflowExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator)
      throws Exception {
    return new GroupAlsoByWindowsParDoFn(options, groupAlsoByWindowsDoFn, stepName, transformName,
        executionContext, addCounterMutator);
  }

  /**
   * A {@link ParDoFnFactory} to create {@link GroupAlsoByWindowsParDoFn} instances according to
   * specifications from the Dataflow service.
   */
  static final class Factory implements ParDoFnFactory {
    @Override
    public ParDoFn create(
        PipelineOptions options,
        CloudObject cloudUserFn,
        String stepName,
        String transformName,
        @Nullable List<SideInputInfo> sideInputInfos,
        @Nullable List<MultiOutputInfo> multiOutputInfos,
        int numOutputs,
        DataflowExecutionContext executionContext,
        CounterSet.AddCounterMutator addCounterMutator,
        StateSampler stateSampler)
            throws Exception {
      Object windowingStrategyObj;
      byte[] encodedWindowingStrategy = getBytes(cloudUserFn, PropertyNames.SERIALIZED_FN);
      if (encodedWindowingStrategy.length == 0) {
        windowingStrategyObj = WindowingStrategy.globalDefault();
      } else {
        windowingStrategyObj =
          SerializableUtils.deserializeFromByteArray(
              encodedWindowingStrategy, "serialized windowing strategy");
        Preconditions.checkArgument(
          windowingStrategyObj instanceof WindowingStrategy,
          "unexpected kind of WindowingStrategy: " + windowingStrategyObj.getClass().getName());
      }
      @SuppressWarnings({"rawtypes", "unchecked"})
      WindowingStrategy windowingStrategy = (WindowingStrategy) windowingStrategyObj;

      byte[] serializedCombineFn = getBytes(cloudUserFn, PropertyNames.COMBINE_FN, null);
      AppliedCombineFn<?, ?, ?, ?> combineFn = null;
      if (serializedCombineFn != null) {
        Object combineFnObj = SerializableUtils.deserializeFromByteArray(
            serializedCombineFn, "serialized combine fn");
        Preconditions.checkArgument(
            combineFnObj instanceof AppliedCombineFn,
            "unexpected kind of AppliedCombineFn: " + combineFnObj.getClass().getName());
        combineFn = (AppliedCombineFn<?, ?, ?, ?>) combineFnObj;
      }

      Map<String, Object> inputCoderObject = getObject(cloudUserFn, PropertyNames.INPUT_CODER);

      Coder<?> inputCoder = Serializer.deserialize(inputCoderObject, Coder.class);
      Preconditions.checkArgument(
          inputCoder instanceof WindowedValueCoder,
          "Expected WindowedValueCoder for inputCoder, got: " + inputCoder.getClass().getName());
      @SuppressWarnings("unchecked")
      WindowedValueCoder<?> windowedValueCoder = (WindowedValueCoder<?>) inputCoder;

      Coder<?> elemCoder = windowedValueCoder.getValueCoder();
      Preconditions.checkArgument(
          elemCoder instanceof KvCoder,
          "Expected KvCoder for inputCoder, got: " + elemCoder.getClass().getName());
      @SuppressWarnings("unchecked")
      KvCoder<?, ?> kvCoder = (KvCoder<?, ?>) elemCoder;

      boolean isStreamingPipeline = options.as(StreamingOptions.class).isStreaming();

      @Nullable AppliedCombineFn<?, ?, ?, ?> maybeMergingCombineFn = null;
      if (combineFn != null) {
        String phase = getString(cloudUserFn, PropertyNames.PHASE, CombinePhase.ALL);
        Preconditions.checkArgument(
            phase.equals(CombinePhase.ALL) || phase.equals(CombinePhase.MERGE),
            "Unexpected phase: " + phase);
        if (phase.equals(CombinePhase.MERGE)) {
          maybeMergingCombineFn = makeAppliedMergingFunction(combineFn);
        } else {
          maybeMergingCombineFn = combineFn;
        }
      }

      DoFn<?, ?> groupAlsoByWindowsDoFn = getGroupAlsoByWindowsDoFn(
          isStreamingPipeline, windowingStrategy, kvCoder, maybeMergingCombineFn);

      return GroupAlsoByWindowsParDoFn.of(
          options,
          groupAlsoByWindowsDoFn,
          stepName,
          transformName,
          executionContext,
          addCounterMutator);
    }
  }

  @Override
  protected DoFnInfo<?, ?> getDoFnInfo() {
    return new DoFnInfo<>(groupAlsoByWindowsDoFn, null);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static DoFn<?, ?> getGroupAlsoByWindowsDoFn(
      boolean isStreamingPipeline,
      WindowingStrategy windowingStrategy,
      KvCoder kvCoder,
      @Nullable AppliedCombineFn maybeMergingCombineFn) {

    if (isStreamingPipeline) {
      if (maybeMergingCombineFn == null) {
        return StreamingGroupAlsoByWindowsDoFn.createForIterable(
            windowingStrategy, kvCoder.getValueCoder());
      } else {
        return StreamingGroupAlsoByWindowsDoFn.create(
            windowingStrategy, maybeMergingCombineFn, kvCoder.getKeyCoder());
      }
    } else {
      if (maybeMergingCombineFn == null) {
        return GroupAlsoByWindowsDoFn.createForIterable(
            windowingStrategy, kvCoder.getValueCoder());
      } else {
        return GroupAlsoByWindowsDoFn.create(
            windowingStrategy, maybeMergingCombineFn, kvCoder.getKeyCoder());
      }
    }
  }

  private static <K, AccumT> AppliedCombineFn<K, AccumT, List<AccumT>, AccumT>
  makeAppliedMergingFunction(AppliedCombineFn<K, ?, AccumT, ?> appliedFn) {
    MergingKeyedCombineFn<K, AccumT> mergingCombineFn = new MergingKeyedCombineFn<>(appliedFn);
    return AppliedCombineFn.<K, AccumT, List<AccumT>, AccumT>withAccumulatorCoder(
        mergingCombineFn, ListCoder.of(appliedFn.getAccumulatorCoder()));
  }

  static class MergingKeyedCombineFn<K, AccumT>
      extends KeyedCombineFn<K, AccumT, List<AccumT>, AccumT> {

    private static final long serialVersionUID = 0;

    final AppliedCombineFn<K, ?, AccumT, ?> appliedCombineFn;

    MergingKeyedCombineFn(AppliedCombineFn<K, ?, AccumT, ?> keyedCombineFn) {
      this.appliedCombineFn = keyedCombineFn;
    }
    @Override
    public List<AccumT> createAccumulator(K key) {
      return new ArrayList<>();
    }
    @Override
    public List<AccumT> addInput(K key, List<AccumT> accumulator, AccumT input) {
      accumulator.add(input);
      // TODO: Buffer more once we have compaction operation.
      if (accumulator.size() > 1) {
        return mergeToSingleton(key, accumulator);
      } else {
        return accumulator;
      }
    }
    @Override
    public List<AccumT> mergeAccumulators(K key, Iterable<List<AccumT>> accumulators) {
      return mergeToSingleton(key, Iterables.concat(accumulators));
    }
    @Override
    public AccumT extractOutput(K key, List<AccumT> accumulator) {
      if (accumulator.size() == 0) {
        return appliedCombineFn.getFn().createAccumulator(key);
      } else {
        return appliedCombineFn.getFn().mergeAccumulators(key, accumulator);
      }
    }
    private List<AccumT> mergeToSingleton(K key, Iterable<AccumT> accumulators) {
      List<AccumT> singleton = new ArrayList<>();
      singleton.add(appliedCombineFn.getFn().mergeAccumulators(key, accumulators));
      return singleton;
    }

    @Override
    public Coder<List<AccumT>> getAccumulatorCoder(CoderRegistry registry, Coder<K> keyCoder,
        Coder<AccumT> inputCoder) throws CannotProvideCoderException {
      return ListCoder.of(appliedCombineFn.getAccumulatorCoder());
    }
  }

  private final DoFn<?, ?> groupAlsoByWindowsDoFn;

  private GroupAlsoByWindowsParDoFn(
      PipelineOptions options,
      DoFn<?, ?> groupAlsoByWindowsDoFn,
      String stepName,
      String transformName,
      DataflowExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator) {
    super(
        options,
        NullSideInputReader.empty(),
        Arrays.asList("output"),
        stepName,
        transformName,
        executionContext,
        addCounterMutator);
    this.groupAlsoByWindowsDoFn = groupAlsoByWindowsDoFn;
  }
}
