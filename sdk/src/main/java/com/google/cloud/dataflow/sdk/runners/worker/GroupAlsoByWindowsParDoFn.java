/*
 * Copyright (C) 2014 Google Inc.
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

import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.StreamingOptions;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
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
    final Object windowFn =
        SerializableUtils.deserializeFromByteArray(
            getBytes(cloudUserFn, PropertyNames.SERIALIZED_FN),
            "serialized window fn");
    if (!(windowFn instanceof WindowFn)) {
      throw new Exception(
          "unexpected kind of WindowFn: " + windowFn.getClass().getName());
    }

    byte[] serializedCombineFn = getBytes(cloudUserFn, PropertyNames.COMBINE_FN, null);
    Object combineFn = null;
    if (serializedCombineFn != null) {
      combineFn =
          SerializableUtils.deserializeFromByteArray(serializedCombineFn, "serialized combine fn");
      if (!(combineFn instanceof KeyedCombineFn)) {
        throw new Exception("unexpected kind of KeyedCombineFn: " + combineFn.getClass().getName());
      }
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

    boolean isStreamingPipeline = false;
    if (options instanceof StreamingOptions) {
      isStreamingPipeline = ((StreamingOptions) options).isStreaming();
    }

    DoFnInfoFactory fnFactory;
    if (isStreamingPipeline) {
      fnFactory = new DoFnInfoFactory() {
        @Override
        public DoFnInfo createDoFnInfo() {
          return new DoFnInfo(StreamingGroupAlsoByWindowsDoFn.create(
              (WindowFn) windowFn,
              ((KvCoder) elemCoder).getValueCoder()),
              null);
        }
      };
    } else {
      fnFactory = new DoFnInfoFactory() {
        @Override
        public DoFnInfo createDoFnInfo() {
          return new DoFnInfo(
              new GroupAlsoByWindowsDoFn((WindowFn) windowFn, elemCoder),
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
