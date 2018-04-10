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
package org.apache.beam.runners.fnexecution.state;

import java.util.concurrent.CompletionStage;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;

/**
 * Handler for {@link org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest StateRequests}.
 */
public interface StateRequestHandler {
  /**
   * Handle a {@link org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest} asynchronously.
   *
   * <p>The handler is allowed to complete the future within the callers thread if it can be
   * completed without blocking. Otherwise the caller should delegate to another thread to perform
   * any blocking work completing the future when able.
   *
   * <p>Throwing an error during handling will complete the handler result {@link CompletionStage}
   * exceptionally.
   */
  CompletionStage<BeamFnApi.StateResponse.Builder> handle(BeamFnApi.StateRequest request)
      throws Exception;
}
