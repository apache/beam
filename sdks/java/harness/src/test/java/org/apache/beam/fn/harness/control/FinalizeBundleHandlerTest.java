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
package org.apache.beam.fn.harness.control;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.fn.harness.control.FinalizeBundleHandler.CallbackRegistration;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.FinalizeBundleRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.FinalizeBundleResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FinalizeBundleHandler}. */
@RunWith(JUnit4.class)
public class FinalizeBundleHandlerTest {
  private static final String INSTRUCTION_ID = "instructionId";
  private static final InstructionResponse SUCCESSFUL_RESPONSE =
      InstructionResponse.newBuilder()
          .setFinalizeBundle(FinalizeBundleResponse.getDefaultInstance())
          .build();

  @Test
  public void testRegistrationAndCallback() throws Exception {
    AtomicBoolean wasCalled1 = new AtomicBoolean();
    AtomicBoolean wasCalled2 = new AtomicBoolean();
    List<CallbackRegistration> callbacks = new ArrayList<>();
    callbacks.add(
        CallbackRegistration.create(
            Instant.now().plus(Duration.standardHours(1)), () -> wasCalled1.set(true)));
    callbacks.add(
        CallbackRegistration.create(
            Instant.now().plus(Duration.standardHours(1)), () -> wasCalled2.set(true)));

    FinalizeBundleHandler handler = new FinalizeBundleHandler(Executors.newCachedThreadPool());
    handler.registerCallbacks("test", callbacks);
    assertEquals(SUCCESSFUL_RESPONSE, handler.finalizeBundle(requestFor("test")).build());
    assertTrue(wasCalled1.get());
    assertTrue(wasCalled2.get());
  }

  @Test
  public void testFinalizationIgnoresMissingBundleIds() throws Exception {
    FinalizeBundleHandler handler = new FinalizeBundleHandler(Executors.newCachedThreadPool());
    assertEquals(SUCCESSFUL_RESPONSE, handler.finalizeBundle(requestFor("test")).build());
  }

  @Test
  public void testFinalizationContinuesToNextCallbackEvenInFailure() throws Exception {
    List<CallbackRegistration> callbacks = new ArrayList<>();
    AtomicBoolean wasCalled1 = new AtomicBoolean();
    AtomicBoolean wasCalled2 = new AtomicBoolean();
    callbacks.add(
        CallbackRegistration.create(
            Instant.now().plus(Duration.standardHours(1)),
            () -> {
              wasCalled1.set(true);
              throw new Exception("testException1");
            }));
    callbacks.add(
        CallbackRegistration.create(
            Instant.now().plus(Duration.standardHours(1)),
            () -> {
              wasCalled2.set(true);
              throw new Exception("testException2");
            }));

    FinalizeBundleHandler handler = new FinalizeBundleHandler(Executors.newCachedThreadPool());
    handler.registerCallbacks("test", callbacks);

    try {
      handler.finalizeBundle(requestFor("test"));
      fail();
    } catch (Exception e) {
      assertThat(e.getMessage(), containsString("Failed to handle bundle finalization for bundle"));
      assertEquals(2, e.getSuppressed().length);
      assertTrue(wasCalled1.get());
      assertTrue(wasCalled2.get());
    }
  }

  private static InstructionRequest requestFor(String bundleId) {
    return InstructionRequest.newBuilder()
        .setInstructionId(INSTRUCTION_ID)
        .setFinalizeBundle(FinalizeBundleRequest.newBuilder().setInstructionId(bundleId).build())
        .build();
  }
}
