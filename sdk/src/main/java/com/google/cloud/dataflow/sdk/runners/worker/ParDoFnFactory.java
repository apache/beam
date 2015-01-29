/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.api.services.dataflow.model.MultiOutputInfo;
import com.google.api.services.dataflow.model.SideInputInfo;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.InstanceBuilder;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.ParDoFn;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Creates a ParDoFn from a CloudObject spec.
 *
 * A ParDoFnFactory concrete "subclass" should define a method with
 * the following signature:
 * <pre> {@code
 * static SomeParDoFnSubclass create(
 *     CloudObject spec,
 *     List<SideInputInfo> sideInputInfos,
 *     List<MultiOutputInfo> multiOutputInfos,
 *     int numOutputs,
 *     ExecutionContext executionContext);
 * } </pre>
 */
public class ParDoFnFactory {
  // Do not instantiate.
  private ParDoFnFactory() {}

  /**
   * A map from the short names of predefined ParDoFnFactories to their full
   * class names.
   */
  static Map<String, String> predefinedParDoFnFactories = new HashMap<>();

  static {
    predefinedParDoFnFactories.put("DoFn",
                                   NormalParDoFn.class.getName());
    predefinedParDoFnFactories.put("CombineValuesFn",
                                   CombineValuesFn.class.getName());
    // TODO: Remove outdated bindings once the services produces the right ones
    predefinedParDoFnFactories.put("MergeBucketsDoFn",
                                   GroupAlsoByWindowsParDoFn.class.getName());
    predefinedParDoFnFactories.put("AssignBucketsDoFn",
                                   AssignWindowsParDoFn.class.getName());
    predefinedParDoFnFactories.put("MergeWindowsDoFn",
                                   GroupAlsoByWindowsParDoFn.class.getName());
    predefinedParDoFnFactories.put("AssignWindowsDoFn",
                                   AssignWindowsParDoFn.class.getName());
    predefinedParDoFnFactories.put("ReifyTimestampAndWindowsDoFn",
                                   ReifyTimestampAndWindowsParDoFn.class.getName());
  }

  /**
   * Creates a ParDoFn from a CloudObject spec.
   *
   * @throws Exception if the CloudObject spec could not be
   * decoded and constructed.
   */
  public static ParDoFn create(PipelineOptions options,
                               CloudObject cloudUserFn,
                               String stepName,
                               List<SideInputInfo> sideInputInfos,
                               List<MultiOutputInfo> multiOutputInfos,
                               int numOutputs,
                               ExecutionContext executionContext,
                               CounterSet.AddCounterMutator addCounterMutator,
                               StateSampler stateSampler)
      throws Exception {
    String className = cloudUserFn.getClassName();
    String parDoFnFactoryClassName = predefinedParDoFnFactories.get(className);
    if (parDoFnFactoryClassName == null) {
      parDoFnFactoryClassName = className;
    }

    try {
      return InstanceBuilder.ofType(ParDoFn.class)
          .fromClassName(parDoFnFactoryClassName)
          .fromFactoryMethod("create")
          .withArg(PipelineOptions.class, options)
          .withArg(CloudObject.class, cloudUserFn)
          .withArg(String.class, stepName)
          .withArg(List.class, sideInputInfos)
          .withArg(List.class, multiOutputInfos)
          .withArg(Integer.class, numOutputs)
          .withArg(ExecutionContext.class, executionContext)
          .withArg(CounterSet.AddCounterMutator.class, addCounterMutator)
          .withArg(StateSampler.class, stateSampler)
          .build();

    } catch (ClassNotFoundException exn) {
      throw new Exception(
          "unable to create a ParDoFn from " + cloudUserFn, exn);
    }
  }
}
