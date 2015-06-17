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

import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.AssignWindowsDoFn;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.DoFnInfo;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PTuple;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoFn;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

/**
 * A wrapper around an AssignWindowsDoFn.  This class is the same as
 * NormalParDoFn, except that it gets deserialized differently.
 */
class AssignWindowsParDoFn extends ParDoFnBase {

  static AssignWindowsParDoFn of(
      PipelineOptions options,
      AssignWindowsDoFn<?, ?> fn,
      String stepName,
      ExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator)
      throws Exception {
    return new AssignWindowsParDoFn(options, fn, stepName, executionContext, addCounterMutator);
  }

  /**
   * A {@link ParDoFnFactory} to create instances of {@link AssignWindowsParDoFn} according to
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

      final Object deserializedWindowingStrategy =
          SerializableUtils.deserializeFromByteArray(
              getBytes(cloudUserFn, PropertyNames.SERIALIZED_FN),
              "serialized windowing strategy");

      Preconditions.checkArgument(
          deserializedWindowingStrategy instanceof WindowingStrategy,
          "unexpected kind of WindowingStrategy: "
          + deserializedWindowingStrategy.getClass().getName());

      // We just checked the raw type, and the other types are simply required to be enforced
      // outside of this class
      @SuppressWarnings("unchecked")
      final WindowingStrategy<Object, BoundedWindow> windowingStrategy =
          (WindowingStrategy<Object, BoundedWindow>) deserializedWindowingStrategy;

      final AssignWindowsDoFn<Object, BoundedWindow> assignFn =
          new AssignWindowsDoFn<>(windowingStrategy.getWindowFn());

      return AssignWindowsParDoFn.of(
          options,
          assignFn,
          stepName,
          executionContext,
          addCounterMutator);
    }
  };

  @Override
  protected DoFnInfo<?, ?> getDoFnInfo() {
    return new DoFnInfo<>(fn, null);
  }

  private final AssignWindowsDoFn<?, ?> fn;

  private AssignWindowsParDoFn(
      PipelineOptions options,
      AssignWindowsDoFn<?, ?> fn,
      String stepName,
      ExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator) {
    super(
        options,
        PTuple.empty(),
        Arrays.asList("output"),
        stepName,
        executionContext,
        addCounterMutator);
    this.fn = fn;
  }
}
