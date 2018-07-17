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
package org.apache.beam.fn.harness.state;

import java.util.concurrent.CompletableFuture;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateResponse;

/**
 * The {@link BeamFnStateClient} is able to forward state requests to a handler which returns a
 * corresponding response or error if completed unsuccessfully.
 */
public interface BeamFnStateClient {

  /**
   * Consumes a state request populating a unique id returning a future to the response.
   *
   * @param requestBuilder A partially completed state request. The id will be populated the client.
   * @param response A future containing a corresponding {@link StateResponse} for the supplied
   *     request.
   */
  void handle(
      BeamFnApi.StateRequest.Builder requestBuilder, CompletableFuture<StateResponse> response);
}
