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
package org.apache.beam.runners.fnexecution.control;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.FinalizeBundleRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;

/** Utility methods for creating {@link BundleFinalizationHandler}s. */
public class BundleFinalizationHandlers {

  /**
   * A bundle finalizer that stores all bundle finalization requests in memory. After the runner
   * durably persists the output, the runner is responsible for invoking {@link
   * InMemoryFinalizer#finalizeAllOutstandingBundles()}.
   */
  public static InMemoryFinalizer inMemoryFinalizer(InstructionRequestHandler fnApiControlClient) {
    return new InMemoryFinalizer(fnApiControlClient);
  }

  /** See {@link #inMemoryFinalizer(InstructionRequestHandler)} for details. */
  public static class InMemoryFinalizer implements BundleFinalizationHandler {
    private final InstructionRequestHandler fnApiControlClient;
    private final List<String> bundleIds;

    private InMemoryFinalizer(InstructionRequestHandler fnApiControlClient) {
      this.fnApiControlClient = fnApiControlClient;
      this.bundleIds = new ArrayList<>();
    }

    /** All finalization requests will be sent without waiting for the responses. */
    public synchronized void finalizeAllOutstandingBundles() {
      for (String bundleId : bundleIds) {
        InstructionRequest request =
            InstructionRequest.newBuilder()
                .setFinalizeBundle(
                    FinalizeBundleRequest.newBuilder().setInstructionId(bundleId).build())
                .build();
        fnApiControlClient.handle(request);
      }
      bundleIds.clear();
    }

    @Override
    public synchronized void requestsFinalization(String bundleId) {
      bundleIds.add(bundleId);
    }
  }
}
