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

import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.DoFnInfo;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PTuple;
import com.google.cloud.dataflow.sdk.util.ReifyTimestampAndWindowsDoFn;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.CounterSet.AddCounterMutator;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoFn;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

/**
 * A {@link ParDoFn} wrapping a {@link ReifyTimestampAndWindowsDoFn}.
 */
class ReifyTimestampAndWindowsParDoFn extends ParDoFnBase {

  static ReifyTimestampAndWindowsParDoFn of(
      PipelineOptions options,
      ReifyTimestampAndWindowsDoFn<?, ?> fn,
      String stepName,
      ExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator)
      throws Exception {

    return new ReifyTimestampAndWindowsParDoFn(
        options, fn, stepName, executionContext, addCounterMutator);
  }

  /**
   * A {@link ParDoFnFactory} to create instances of {@link ReifyTimestampAndWindowsParDoFn}
   * according to specifications from the Dataflow service.
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

      final ReifyTimestampAndWindowsDoFn<Object, Object> fn =
          new ReifyTimestampAndWindowsDoFn<Object, Object>();

      return ReifyTimestampAndWindowsParDoFn.of(
          options,
          fn,
          stepName,
          executionContext,
          addCounterMutator);
    }
  };

  @Override
  protected DoFnInfo<?, ?> getDoFnInfo() {
    return new DoFnInfo<>(fn, null);
  }

  private final ReifyTimestampAndWindowsDoFn<?, ?> fn;

  private ReifyTimestampAndWindowsParDoFn(
      PipelineOptions options,
      ReifyTimestampAndWindowsDoFn fn,
      String stepName,
      ExecutionContext executionContext,
      AddCounterMutator addCounterMutator) {

    super(options,
          PTuple.empty(),
          Arrays.asList("output"),
          stepName,
          executionContext,
          addCounterMutator);
    this.fn = fn;
  }
}
