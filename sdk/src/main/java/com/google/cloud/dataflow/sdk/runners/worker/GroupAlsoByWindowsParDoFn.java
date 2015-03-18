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

import com.google.api.client.util.Preconditions;
import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.StreamingOptions;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.DoFnInfo;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.GroupAlsoByWindowsDoFn;
import com.google.cloud.dataflow.sdk.util.PTuple;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.Serializer;
import com.google.cloud.dataflow.sdk.util.StreamingGroupAlsoByWindowsDoFn;
import com.google.cloud.dataflow.sdk.util.WindowedValue.WindowedValueCoder;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
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
class GroupAlsoByWindowsParDoFn extends NormalParDoFn {

  public static GroupAlsoByWindowsParDoFn create(
      PipelineOptions options,
      CloudObject cloudUserFn,
      String stepName,
      @Nullable List<SideInputInfo> sideInputInfos,
      @Nullable List<MultiOutputInfo> multiOutputInfos,
      Integer numOutputs,
      ExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator,
      StateSampler sampler /* unused */)
      throws Exception {
    final Object windowFnObj;
    byte[] encodedWindowFn = getBytes(cloudUserFn, PropertyNames.SERIALIZED_FN);
    if (encodedWindowFn.length == 0) {
      windowFnObj = new GlobalWindows();
    } else {
      windowFnObj =
        SerializableUtils.deserializeFromByteArray(encodedWindowFn, "serialized window fn");
      if (!(windowFnObj instanceof WindowFn)) {
        throw new Exception(
            "unexpected kind of WindowFn: " + windowFnObj.getClass().getName());
      }
    }
    final WindowFn windowFn = (WindowFn) windowFnObj;

    byte[] serializedCombineFn = getBytes(cloudUserFn, PropertyNames.COMBINE_FN, null);
    KeyedCombineFn combineFn;
    if (serializedCombineFn != null) {
      Object combineFnObj =
          SerializableUtils.deserializeFromByteArray(serializedCombineFn, "serialized combine fn");
      if (!(combineFnObj instanceof KeyedCombineFn)) {
        throw new Exception(
            "unexpected kind of KeyedCombineFn: " + combineFnObj.getClass().getName());
      }
      combineFn = (KeyedCombineFn) combineFnObj;
    } else {
      combineFn = null;
    }

    Map<String, Object> inputCoderObject = getObject(cloudUserFn, PropertyNames.INPUT_CODER);

    Coder inputCoder = Serializer.deserialize(inputCoderObject, Coder.class);
    if (!(inputCoder instanceof WindowedValueCoder)) {
      throw new Exception(
          "Expected WindowedValueCoder for inputCoder, got: "
          + inputCoder.getClass().getName());
    }
    final Coder elemCoder = ((WindowedValueCoder) inputCoder).getValueCoder();
    if (!(elemCoder instanceof KvCoder)) {
      throw new Exception(
          "Expected KvCoder for inputCoder, got: " + elemCoder.getClass().getName());
    }
    final KvCoder kvCoder = (KvCoder) elemCoder;

    boolean isStreamingPipeline = false;
    if (options instanceof StreamingOptions) {
      isStreamingPipeline = ((StreamingOptions) options).isStreaming();
    }

    boolean isMergingOnly = true;
    final KeyedCombineFn maybeMergingCombineFn;
    if (isMergingOnly && combineFn != null) {
      class MergingKeyedCombineFn<K, VA> extends KeyedCombineFn<K, VA, List<VA>, VA> {
        private static final long serialVersionUID = 0;
        final KeyedCombineFn<K, ?, VA, ?> combineFn;
        MergingKeyedCombineFn(KeyedCombineFn<K, ?, VA, ?> combineFn) {
          this.combineFn = combineFn;
        }
        public List<VA> createAccumulator(K key) {
          return new ArrayList<>();
        }
        public void addInput(K key, List<VA> accumulator, VA input) {
          accumulator.add(input);
          // TODO: Buffer more once we have compaction operation.
          if (accumulator.size() > 1) {
            VA all = combineFn.mergeAccumulators(key, accumulator);
            accumulator.clear();
            accumulator.add(all);
          }
        }
        public List<VA> mergeAccumulators(K key, Iterable<List<VA>> accumulators) {
          List<VA> singleton = new ArrayList<>();
          singleton.add(combineFn.mergeAccumulators(key, Iterables.concat(accumulators)));
          return singleton;
        }
        public VA extractOutput(K key, List<VA> accumulator) {
          if (accumulator.size() == 0) {
            return combineFn.createAccumulator(key);
          } else {
            return combineFn.mergeAccumulators(key, accumulator);
          }
        }
      };
      maybeMergingCombineFn = new MergingKeyedCombineFn(combineFn);
    } else {
      maybeMergingCombineFn = combineFn;
    }

    DoFnInfoFactory fnFactory;
    if (isStreamingPipeline) {
      fnFactory = new DoFnInfoFactory() {
        @Override
        public DoFnInfo createDoFnInfo() {
          return new DoFnInfo(
              StreamingGroupAlsoByWindowsDoFn.create(
                  windowFn,
                  maybeMergingCombineFn,
                  kvCoder.getKeyCoder(),
                  kvCoder.getValueCoder()),
              null);
        }
      };
    } else {
      //TODO: handle CombineFn in batch GroupAlsoByWindowsDoFn.
      Preconditions.checkArgument(
          combineFn == null,
          "combineFn is expected to be null in batch, but it is " + combineFn);
      fnFactory = new DoFnInfoFactory() {
        @Override
        public DoFnInfo createDoFnInfo() {
          return new DoFnInfo(
              GroupAlsoByWindowsDoFn.create(
                  windowFn,
                  maybeMergingCombineFn,
                  kvCoder.getKeyCoder(),
                  kvCoder.getValueCoder()),
              null);
        }
      };
    }
    return new GroupAlsoByWindowsParDoFn(
        options, fnFactory, stepName, executionContext, addCounterMutator);
  }

  private GroupAlsoByWindowsParDoFn(
      PipelineOptions options,
      DoFnInfoFactory fnFactory,
      String stepName,
      ExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator) {
    super(
        options,
        fnFactory,
        PTuple.empty(),
        Arrays.asList("output"),
        stepName,
        executionContext,
        addCounterMutator);
  }
}
