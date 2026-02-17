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
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.FinalizeBundleResponse;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
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
public class FinalizeBundleHandler {

  /** A {@link BundleFinalizer.Callback} and expiry time pair. */
  @AutoValue
  public abstract static class CallbackRegistration {
    public static CallbackRegistration create(
        Instant expiryTime, BundleFinalizer.Callback callback) {
      return new AutoValue_FinalizeBundleHandler_CallbackRegistration(expiryTime, callback);
    }

    public abstract Instant getExpiryTime();

    public abstract BundleFinalizer.Callback getCallback();
  }

  private static class FinalizationInfo {
    FinalizationInfo(
        String id, Instant expiryTimestamp, Collection<CallbackRegistration> callbacks) {
      this.id = id;
      this.expiryTimestamp = expiryTimestamp;
      this.callbacks = callbacks;
    }

    final String id;
    final Instant expiryTimestamp;
    final Collection<CallbackRegistration> callbacks;

    Instant getExpiryTimestamp() {
      return expiryTimestamp;
    }
  }

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition queueMinChanged = lock.newCondition();

  @GuardedBy("lock")
  private final HashMap<String, FinalizationInfo> bundleFinalizationCallbacks;

  @GuardedBy("lock")
  private final PriorityQueue<FinalizationInfo> cleanUpQueue;

  @SuppressWarnings("methodref.receiver.bound")
  public FinalizeBundleHandler(ExecutorService executorService) {
    this.bundleFinalizationCallbacks = new HashMap<>();
    this.cleanUpQueue =
        new PriorityQueue<>(11, Comparator.comparing(FinalizationInfo::getExpiryTimestamp));
    executorService.execute(this::cleanupThreadBody);
  }

  private void cleanupThreadBody() {
    lock.lock();
    try {
      while (true) {
        final @Nullable FinalizationInfo minValue = cleanUpQueue.peek();
        if (minValue == null) {
          // Wait for an element to be added and loop to re-examine the min.
          queueMinChanged.await();
          continue;
        }

        Instant now = Instant.now();
        Duration timeDifference = new Duration(now, minValue.expiryTimestamp);
        if (timeDifference.getMillis() < 0
            || (queueMinChanged.await(timeDifference.getMillis(), TimeUnit.MILLISECONDS)
                && cleanUpQueue.peek() == minValue)) {
          // The minimum element has an expiry time before now, either because it had elapsed when
          // we pulled it or because we awaited it and it is still the minimum.
          checkState(minValue == cleanUpQueue.poll());
          checkState(bundleFinalizationCallbacks.remove(minValue.id) == minValue);
        }
      }
    } catch (InterruptedException e) {
      // We're being shutdown.
    } finally {
      lock.unlock();
    }
  }

  public void registerCallbacks(String bundleId, Collection<CallbackRegistration> callbacks) {
    if (callbacks.isEmpty()) {
      return;
    }
    Instant maxExpiryTime = Instant.EPOCH;
    for (CallbackRegistration callback : callbacks) {
      Instant callbackExpiry = callback.getExpiryTime();
      if (callbackExpiry.isAfter(maxExpiryTime)) {
        maxExpiryTime = callbackExpiry;
      }
    }
    final FinalizationInfo info = new FinalizationInfo(bundleId, maxExpiryTime, callbacks);

    lock.lock();
    try {
      FinalizationInfo existingInfo = bundleFinalizationCallbacks.put(bundleId, info);
      if (existingInfo != null) {
        throw new IllegalStateException(
            "Expected to not have any past callbacks for bundle "
                + bundleId
                + " but had "
                + existingInfo.callbacks);
      }
      cleanUpQueue.add(info);
      @SuppressWarnings("ReferenceEquality")
      boolean newMin = cleanUpQueue.peek() == info;
      if (newMin) {
        queueMinChanged.signal();
      }
    } finally {
      lock.unlock();
    }
  }

  public BeamFnApi.InstructionResponse.Builder finalizeBundle(BeamFnApi.InstructionRequest request)
      throws Exception {
    String bundleId = request.getFinalizeBundle().getInstructionId();

    @Nullable FinalizationInfo info;
    lock.lock();
    try {
      info = bundleFinalizationCallbacks.remove(bundleId);
      if (info != null) {
        checkState(cleanUpQueue.remove(info));
      }
    } finally {
      lock.unlock();
    }
    if (info == null) {
      // We have already processed the callbacks on a prior bundle finalization attempt
      return BeamFnApi.InstructionResponse.newBuilder()
          .setFinalizeBundle(FinalizeBundleResponse.getDefaultInstance());
    }

    Collection<Exception> failures = new ArrayList<>();
    for (CallbackRegistration callback : info.callbacks) {
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

  int cleanupQueueSize() {
    lock.lock();
    try {
      return cleanUpQueue.size();
    } finally {
      lock.unlock();
    }
  }
}
