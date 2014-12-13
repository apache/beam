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

import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowingFn;
import com.google.cloud.dataflow.sdk.util.AssignWindowsDoFn;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PTuple;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

/**
 * A wrapper around an AssignWindowsDoFn.  This class is the same as
 * NormalParDoFn, except that it gets deserialized differently.
 */
class AssignWindowsParDoFn extends NormalParDoFn {
  public static AssignWindowsParDoFn create(
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
    Object windowingFn =
        SerializableUtils.deserializeFromByteArray(
            getBytes(cloudUserFn, PropertyNames.SERIALIZED_FN),
            "serialized window fn");
    if (!(windowingFn instanceof WindowingFn)) {
      throw new Exception(
          "unexpected kind of WindowingFn: " + windowingFn.getClass().getName());
    }

    DoFn assignWindowsDoFn = new AssignWindowsDoFn((WindowingFn) windowingFn);

    return new AssignWindowsParDoFn(
        options, assignWindowsDoFn, stepName, executionContext, addCounterMutator);
  }

  private AssignWindowsParDoFn(
      PipelineOptions options,
      DoFn fn,
      String stepName,
      ExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator) {
    super(
        options,
        fn,
        PTuple.empty(),
        Arrays.asList("output"),
        stepName,
        executionContext,
        addCounterMutator);
  }
}
