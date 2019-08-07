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
package org.apache.beam.runners.dataflow.worker;

import com.google.api.services.dataflow.model.SourceOperationRequest;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

/** Creates a SourceOperationExecutor from a SourceOperation. */
public class SourceOperationExecutorFactory {
  public static SourceOperationExecutor create(
      PipelineOptions options,
      SourceOperationRequest request,
      CounterSet counters,
      DataflowExecutionContext<?> executionContext,
      String stageName)
      throws Exception {
    boolean beamFnApi =
        DataflowRunner.hasExperiment(options.as(DataflowPipelineDebugOptions.class), "beam_fn_api");

    Preconditions.checkNotNull(request, "SourceOperationRequest must be non-null");
    Preconditions.checkNotNull(executionContext, "executionContext must be non-null");

    // Disable splitting when fn api is enabled.
    // TODO: Fix this once source splitting is supported.
    if (beamFnApi) {
      return new NoOpSourceOperationExecutor(request);
    } else {
      DataflowOperationContext operationContext =
          executionContext.createOperationContext(
              NameContext.create(
                  stageName,
                  request.getOriginalName(),
                  request.getSystemName(),
                  request.getName()));

      return new WorkerCustomSourceOperationExecutor(
          options, request, counters, executionContext, operationContext);
    }
  }
}
