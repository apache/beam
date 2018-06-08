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
package org.apache.beam.runners.flink.translation.functions;

import java.util.concurrent.CompletionStage;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse.Builder;
import org.apache.beam.runners.core.construction.graph.ExecutableStage;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.flink.api.common.functions.RuntimeContext;

// TODO: https://issues.apache.org/jira/browse/BEAM-4285 Implement batch state handler.
/** State request handler for flink batch runner. */
public class FlinkBatchStateRequestHandler implements StateRequestHandler {

  private FlinkBatchStateRequestHandler() {}

  public static FlinkBatchStateRequestHandler forStage(
      ExecutableStage stage, RuntimeContext runtimeContext) {
    return new FlinkBatchStateRequestHandler();
  }

  @Override
  public CompletionStage<Builder> handle(StateRequest request) throws Exception {
    throw new UnsupportedOperationException();
  }
}
