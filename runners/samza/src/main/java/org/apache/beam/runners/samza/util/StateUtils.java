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
package org.apache.beam.runners.samza.util;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;

/** Utils for determining stateful operators. */
public class StateUtils {

  public static boolean isStateful(DoFn<?, ?> doFn) {
    return DoFnSignatures.isStateful(doFn);
  }

  public static boolean isStateful(RunnerApi.ExecutableStagePayload stagePayload) {
    return stagePayload.getUserStatesCount() > 0 || stagePayload.getTimersCount() > 0;
  }

  public static boolean isStateful(ExecutableStage executableStage) {
    return executableStage.getUserStates().size() > 0 || executableStage.getTimers().size() > 0;
  }
}
