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
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoFn;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;

import java.util.List;

/**
 * The interface of factories that create a worker-side {@link ParDoFn} from a {@link CloudObject}
 * specification provided by the Dataflow service.
 */
public interface ParDoFnFactory {

  /**
   * Creates a {@link ParDoFn} from standard parameters, corresponding to the specification
   * provided to the worker by the Dataflow service.
   */
  ParDoFn create(
      PipelineOptions options,
      CloudObject cloudUserFn,
      String stepName,
      String transformName,
      List<SideInputInfo> sideInputInfos,
      List<MultiOutputInfo> multiOutputInfos,
      int numOutputs,
      DataflowExecutionContext executionContext,
      CounterSet.AddCounterMutator addCounterMutator,
      StateSampler stateSampler)
      throws Exception;
}
