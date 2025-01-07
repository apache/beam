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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.FinalizeBundleResponse;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A bundle finalization handler that expires entries after a specified amount of time.
 *
 * <p>Callers should register new callbacks via {@link #registerCallbacks} and fire existing
 * callbacks using {@link #finalizeBundle}.
 *
 * <p>See <a href="https://s.apache.org/beam-finalizing-bundles">Apache Beam Portability API: How to
 * Finalize Bundles</a> for further details.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class FinalizeBundleHandler {

  /** A {@link BundleFinalizer.Callback} and expiry time pair. */
  @AutoValue
  abstract static class CallbackRegistration {
    public static CallbackRegistration create(
        Instant expiryTime, BundleFinalizer.Callback callback) {
      return new AutoValue_FinalizeBundleHandler_CallbackRegistration(expiryTime, callback);
    }

    public abstract Instant getExpiryTime();

    public abstract BundleFinalizer.Callback getCallback();
  }

  private final ConcurrentMap<String, Collection<CallbackRegistration>> bundleFinalizationCallbacks;
  private final ScheduledExecutorService scheduledExecutorService;

  public FinalizeBundleHandler(ScheduledExecutorService scheduledExecutorService) {
    this.bundleFinalizationCallbacks = new ConcurrentHashMap<>();
    this.scheduledExecutorService = scheduledExecutorService;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  public void registerCallbacks(String bundleId, Collection<CallbackRegistration> callbacks) {
    if (callbacks.isEmpty()) {
      return;
    }

    @Nullable
    Collection<CallbackRegistration> priorCallbacks =
        bundleFinalizationCallbacks.putIfAbsent(bundleId, callbacks);
    checkState(
        priorCallbacks == null,
        "Expected to not have any past callbacks for bundle %s but found %s.",
        bundleId,
        priorCallbacks);
    long expiryDelayMillis = Long.MIN_VALUE;
    for (CallbackRegistration callback : callbacks) {
      expiryDelayMillis =
          Math.max(expiryDelayMillis, new Duration(null, callback.getExpiryTime()).getMillis());
    }
    scheduledExecutorService.schedule(
        () -> bundleFinalizationCallbacks.remove(bundleId),
        expiryDelayMillis,
        TimeUnit.MILLISECONDS);
  }

  public BeamFnApi.InstructionResponse.Builder finalizeBundle(BeamFnApi.InstructionRequest request)
      throws Exception {
    String bundleId = request.getFinalizeBundle().getInstructionId();

    Collection<CallbackRegistration> callbacks = bundleFinalizationCallbacks.remove(bundleId);

    if (callbacks == null) {
      // We have already processed the callbacks on a prior bundle finalization attempt
      return BeamFnApi.InstructionResponse.newBuilder()
          .setFinalizeBundle(FinalizeBundleResponse.getDefaultInstance());
    }

    Collection<Exception> failures = new ArrayList<>();
    for (CallbackRegistration callback : callbacks) {
      try {
        callback.getCallback().onBundleSuccess();
      } catch (Exception e) {
        failures.add(e);
      }
    }
    if (!failures.isEmpty()) {
      Exception e =
          new Exception(
              String.format("Failed to handle bundle finalization for bundle %s.", bundleId));
      for (Exception failure : failures) {
        e.addSuppressed(failure);
      }
      throw e;
    }

    return BeamFnApi.InstructionResponse.newBuilder()
        .setFinalizeBundle(FinalizeBundleResponse.getDefaultInstance());
  }
}
