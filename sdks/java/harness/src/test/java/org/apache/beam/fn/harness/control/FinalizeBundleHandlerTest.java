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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.fn.harness.control.FinalizeBundleHandler.CallbackRegistration;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.FinalizeBundleRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.FinalizeBundleResponse;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionRequest;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.InstructionResponse;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
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

  @Test
  public void testCallbackExpiration() throws Exception {
    ExecutorService executor = Executors.newCachedThreadPool();
    FinalizeBundleHandler handler = new FinalizeBundleHandler(executor);
    BundleFinalizer.Callback callback = mock(BundleFinalizer.Callback.class);
    handler.registerCallbacks(
        "test",
        Collections.singletonList(
            CallbackRegistration.create(Instant.now().plus(Duration.standardHours(1)), callback)));
    assertEquals(1, handler.cleanupQueueSize());

    BundleFinalizer.Callback callback2 = mock(BundleFinalizer.Callback.class);
    handler.registerCallbacks(
        "test2",
        Collections.singletonList(
            CallbackRegistration.create(Instant.now().plus(Duration.millis(100)), callback2)));
    BundleFinalizer.Callback callback3 = mock(BundleFinalizer.Callback.class);
    handler.registerCallbacks(
        "test3",
        Collections.singletonList(
            CallbackRegistration.create(Instant.now().plus(Duration.millis(1)), callback3)));
    while (handler.cleanupQueueSize() > 1) {
      Thread.sleep(500);
    }
    // Just the "test" bundle should remain as "test2" and "test3" should have timed out.
    assertEquals(1, handler.cleanupQueueSize());
    // Completing test2 and test3 should have successful response but not invoke the callbacks
    // as they were cleaned up.
    assertEquals(SUCCESSFUL_RESPONSE, handler.finalizeBundle(requestFor("test2")).build());
    verifyNoMoreInteractions(callback2);
    assertEquals(SUCCESSFUL_RESPONSE, handler.finalizeBundle(requestFor("test3")).build());
    verifyNoMoreInteractions(callback3);
    // Completing "test" bundle should call the callback and remove it from cleanup queue.
    assertEquals(1, handler.cleanupQueueSize());
    assertEquals(SUCCESSFUL_RESPONSE, handler.finalizeBundle(requestFor("test")).build());
    verify(callback).onBundleSuccess();
    assertEquals(0, handler.cleanupQueueSize());
    // Verify that completing again is a no-op as it was cleaned up.
    assertEquals(SUCCESSFUL_RESPONSE, handler.finalizeBundle(requestFor("test")).build());
    verifyNoMoreInteractions(callback);
    executor.shutdownNow();
  }

  private static InstructionRequest requestFor(String bundleId) {
    return InstructionRequest.newBuilder()
        .setInstructionId(INSTRUCTION_ID)
        .setFinalizeBundle(FinalizeBundleRequest.newBuilder().setInstructionId(bundleId).build())
        .build();
  }
}
