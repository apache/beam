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
import com.google.cloud.dataflow.sdk.util.PTuple;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoFn;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;

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
      DoFnInfo doFnInfo,
      PTuple sideInputValues,
      List<String> outputTags,
      String stepName,
      ExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator) {
    return new NormalParDoFn(
        options,
        doFnInfo,
        sideInputValues,
        outputTags,
        stepName,
        executionContext,
        addCounterMutator);
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
        @Nullable List<SideInputInfo> sideInputInfos,
        @Nullable List<MultiOutputInfo> multiOutputInfos,
        int numOutputs,
        ExecutionContext executionContext,
        CounterSet.AddCounterMutator addCounterMutator,
        StateSampler stateSampler /* ignored */)
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

      // If the side input data has already been computed, it will be in sideInputInfo.  Otherwise,
      // we need to look it up dynamically from the Views.
      PTuple sideInputValues = PTuple.empty();
      final Iterable<PCollectionView<?>> sideInputViews = doFnInfo.getSideInputViews();
      if (sideInputInfos != null && !sideInputInfos.isEmpty()) {
        for (SideInputInfo sideInputInfo : sideInputInfos) {
          Object sideInputValue = SideInputUtils.readSideInput(
              options, sideInputInfo, executionContext);
          TupleTag<Object> tag = new TupleTag<>(sideInputInfo.getTag());
          sideInputValues = sideInputValues.and(tag, sideInputValue);
        }
      } else if (sideInputViews != null) {
        for (PCollectionView<?> view : sideInputViews) {
          sideInputValues = sideInputValues.and(view.getTagInternal(), null);
        }
      }

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
          sideInputValues,
          outputTags,
          stepName,
          executionContext,
          addCounterMutator);
    }
  }

  private final byte[] serializedDoFn;
  private final DoFnInfo<?, ?> doFnInfo;

  private NormalParDoFn(
      PipelineOptions options,
      DoFnInfo<?, ?> doFnInfo,
      PTuple sideInputValues,
      List<String> outputTags,
      String stepName,
      ExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator) {
    super(options, sideInputValues, outputTags, stepName, executionContext, addCounterMutator);
    // The userDoFn is serialized because a fresh copy is provided each time it is accessed.
    this.serializedDoFn = SerializableUtils.serializeToByteArray(doFnInfo.getDoFn());
    this.doFnInfo = doFnInfo;
  }

  /**
   * Produces a fresh {@link DoFnInfo} containing the user's {@link DoFn}.
   */
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
