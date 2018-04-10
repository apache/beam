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

import org.apache.beam.model.fnexecution.v1.BeamFnApi.StateRequest;

/**
 * The {@link StateDelegator} is able to delegate {@link StateRequest}s to a set of registered
 * handlers. Any request for an unregistered process bundle instruction id is automatically failed.
 */
public interface StateDelegator {
  /**
   * Registers the supplied handler for the given process bundle instruction id for all {@link
   * StateRequest}s with a matching id. A handle is returned which allows one to deregister from
   * this {@link StateDelegator}.
   */
  Registration registerForProcessBundleInstructionId(
      String processBundleInstructionId, StateRequestHandler handler);

  /**
   * Allows callers to deregister from receiving further state requests.
   */
  interface Registration {
    /**
     * De-registers the handler for all future requests for state for the registered process
     * bundle instruction id.
     */
    void deregister();

    /**
     * De-registers the handler for all future requests for state for the registered process
     * bundle instruction id. Aborts all in-flight state requests.
     */
    void abort();
  }
}
