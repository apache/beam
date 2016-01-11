/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.util.Structs.getBytes;

import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.DoFnInfo;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.SideInputReader;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoFn;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

/**
 * A wrapper around a decoded user {@link DoFn}.
 */
class NormalParDoFn extends ParDoFnBase {

  /**
   * Create a {@link NormalParDoFn}.
   */
  static NormalParDoFn of(
      PipelineOptions options,
      DoFnInfo<?, ?> doFnInfo,
      SideInputReader sideInputReader,
      List<String> outputTags,
      String stepName,
      String transformName,
      DataflowExecutionContext<?> executionContext,
      CounterSet.AddCounterMutator addCounterMutator,
      StateSampler stateSampler) {
    return new NormalParDoFn(
        options,
        doFnInfo,
        sideInputReader,
        outputTags,
        stepName,
        transformName,
        executionContext,
        addCounterMutator,
        stateSampler);
  }

  /**
   * A {@link ParDoFnFactory} to create instances of {@link NormalParDoFn} according to
   * specifications from the Dataflow service.
   */
  static final class Factory implements ParDoFnFactory {
    @Override
    public ParDoFn create(
        PipelineOptions options,
        final CloudObject cloudUserFn,
        String stepName,
        String transformName,
        @Nullable List<SideInputInfo> sideInputInfos,
        @Nullable List<MultiOutputInfo> multiOutputInfos,
        int numOutputs,
        DataflowExecutionContext<?> executionContext,
        CounterSet.AddCounterMutator addCounterMutator,
        StateSampler stateSampler)
            throws Exception {
      Object deserializedFnInfo =
          SerializableUtils.deserializeFromByteArray(
              getBytes(cloudUserFn, PropertyNames.SERIALIZED_FN),
              "serialized fn info");
      if (!(deserializedFnInfo instanceof DoFnInfo)) {
        throw new Exception(
            "unexpected kind of DoFnInfo: " + deserializedFnInfo.getClass().getName());
      }
      DoFnInfo<?, ?> doFnInfo = (DoFnInfo<?, ?>) deserializedFnInfo;

      Iterable<PCollectionView<?>> sideInputViews = doFnInfo.getSideInputViews();
      SideInputReader sideInputReader =
          executionContext.getSideInputReader(sideInputInfos, sideInputViews);

      List<String> outputTags = new ArrayList<>();
      if (multiOutputInfos != null) {
        for (MultiOutputInfo multiOutputInfo : multiOutputInfos) {
          outputTags.add(multiOutputInfo.getTag());
        }
      }
      if (outputTags.isEmpty()) {
        // Legacy support: assume there's a single output tag named "output".
        // (The output tag name will be ignored, for the main output.)
        outputTags.add("output");
      }
      if (numOutputs != outputTags.size()) {
        throw new AssertionError(
            "unexpected number of outputTags for DoFn");
      }

      return NormalParDoFn.of(
          options,
          doFnInfo,
          sideInputReader,
          outputTags,
          stepName,
          transformName,
          executionContext,
          addCounterMutator,
          stateSampler);
    }
  }

  private final byte[] serializedDoFn;
  private final DoFnInfo<?, ?> doFnInfo;

  private NormalParDoFn(
      PipelineOptions options,
      DoFnInfo<?, ?> doFnInfo,
      SideInputReader sideInputReader,
      List<String> outputTags,
      String stepName,
      String transformName,
      ExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator,
      StateSampler stateSampler) {
    super(options, sideInputReader, outputTags, stepName, transformName, executionContext,
        addCounterMutator, stateSampler);
    // The userDoFn is serialized because a fresh copy is provided each time it is accessed.
    this.serializedDoFn = SerializableUtils.serializeToByteArray(doFnInfo.getDoFn());
    this.doFnInfo = doFnInfo;
  }

  /**
   * Produces a fresh {@link DoFnInfo} containing the user's {@link DoFn}.
   */
  @Override
  protected DoFnInfo getDoFnInfo() {
    // This class write the serialized data in its own constructor, as a way of doing
    // a deep copy.
    @SuppressWarnings("unchecked")
    DoFn<?, ?> userDoFn = (DoFn<?, ?>) SerializableUtils.deserializeFromByteArray(
        serializedDoFn, "serialized user fun");
    return new DoFnInfo(
        userDoFn,
        doFnInfo.getWindowingStrategy(),
        doFnInfo.getSideInputViews(),
        doFnInfo.getInputCoder());
  }
}
