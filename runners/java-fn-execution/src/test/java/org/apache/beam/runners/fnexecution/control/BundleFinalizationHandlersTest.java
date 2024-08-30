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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import org.apache.beam.model.fnexecution.v1.BeamFnApi.FinalizeBundleRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.runners.fnexecution.control.BundleFinalizationHandlers.InMemoryFinalizer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BundleFinalizationHandlers}. */
@RunWith(JUnit4.class)
public class BundleFinalizationHandlersTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);

  @Test
  public void testInMemoryFinalizer() {
    InstructionRequestHandler mockHandler = mock(InstructionRequestHandler.class);
    InMemoryFinalizer finalizer = BundleFinalizationHandlers.inMemoryFinalizer(mockHandler);

    finalizer.finalizeAllOutstandingBundles();
    verifyZeroInteractions(mockHandler);

    finalizer.requestsFinalization("A");
    finalizer.requestsFinalization("B");
    verifyZeroInteractions(mockHandler);

    finalizer.finalizeAllOutstandingBundles();
    verify(mockHandler).handle(requestFor("A"));
    verify(mockHandler).handle(requestFor("B"));
  }

  private static InstructionRequest requestFor(String bundleId) {
    return InstructionRequest.newBuilder()
        .setFinalizeBundle(FinalizeBundleRequest.newBuilder().setInstructionId(bundleId).build())
        .build();
  }
}
