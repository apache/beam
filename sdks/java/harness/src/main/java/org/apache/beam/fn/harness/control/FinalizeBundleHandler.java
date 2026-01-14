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
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.FinalizeBundleResponse;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.values.TimestampedValue;
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
  abstract static class CallbackRegistration {
    public static CallbackRegistration create(
        Instant expiryTime, BundleFinalizer.Callback callback) {
      return new AutoValue_FinalizeBundleHandler_CallbackRegistration(expiryTime, callback);
    }

    public abstract Instant getExpiryTime();

    public abstract BundleFinalizer.Callback getCallback();
  }

  private final ConcurrentMap<String, Collection<CallbackRegistration>> bundleFinalizationCallbacks;
  private final ReentrantLock cleanupLock = new ReentrantLock();
  private final Condition queueMinChanged = cleanupLock.newCondition();

  @GuardedBy("cleanupLock")
  private final PriorityQueue<TimestampedValue<String>> cleanUpQueue;

  @SuppressWarnings("unused")
  private final Future<?> cleanUpResult;

  @SuppressWarnings("methodref.receiver.bound")
  public FinalizeBundleHandler(ExecutorService executorService) {
    this.bundleFinalizationCallbacks = new ConcurrentHashMap<>();
    this.cleanUpQueue =
        new PriorityQueue<>(11, Comparator.comparing(TimestampedValue::getTimestamp));

    cleanUpResult = executorService.submit(this::cleanupThreadBody);
  }

  private void cleanupThreadBody() {
    cleanupLock.lock();
    try {
      while (true) {
        final @Nullable TimestampedValue<String> minValue = cleanUpQueue.peek();
        if (minValue == null) {
          // Wait for an element to be added and loop to re-examine the min.
          queueMinChanged.await();
          continue;
        }

        Instant now = Instant.now();
        Duration timeDifference = new Duration(now, minValue.getTimestamp());
        if (timeDifference.getMillis() > 0
            && queueMinChanged.await(timeDifference.getMillis(), TimeUnit.MILLISECONDS)) {
          // If the time didn't elapse, loop to re-examine the min.
          continue;
        }

        // The minimum element has an expiry time before now.
        // It may or may not actually be present in the map if the finalization has already been
        // completed.
        bundleFinalizationCallbacks.remove(minValue.getValue());
      }
    } catch (InterruptedException e) {
      // We're being shutdown.
    } finally {
      cleanupLock.unlock();
    }
  }

  public void registerCallbacks(String bundleId, Collection<CallbackRegistration> callbacks) {
    if (callbacks.isEmpty()) {
      return;
    }

    Collection<CallbackRegistration> priorCallbacks =
        bundleFinalizationCallbacks.putIfAbsent(bundleId, callbacks);
    checkState(
        priorCallbacks == null,
        "Expected to not have any past callbacks for bundle %s but found %s.",
        bundleId,
        priorCallbacks);
    long expiryTimeMillis = Long.MIN_VALUE;
    for (CallbackRegistration callback : callbacks) {
      expiryTimeMillis = Math.max(expiryTimeMillis, callback.getExpiryTime().getMillis());
    }

    cleanupLock.lock();
    try {
      TimestampedValue<String> value = TimestampedValue.of(bundleId, new Instant(expiryTimeMillis));
      cleanUpQueue.offer(value);
      @SuppressWarnings("ReferenceEquality")
      boolean newMin = cleanUpQueue.peek() == value;
      if (newMin) {
        queueMinChanged.signal();
      }
    } finally {
      cleanupLock.unlock();
    }
  }

  public BeamFnApi.InstructionResponse.Builder finalizeBundle(BeamFnApi.InstructionRequest request)
      throws Exception {
    String bundleId = request.getFinalizeBundle().getInstructionId();

    final @Nullable Collection<CallbackRegistration> callbacks =
        bundleFinalizationCallbacks.remove(bundleId);

    if (callbacks == null) {
      // We have already processed the callbacks on a prior bundle finalization attempt
      return BeamFnApi.InstructionResponse.newBuilder()
          .setFinalizeBundle(FinalizeBundleResponse.getDefaultInstance());
    }
    // We don't bother removing from the cleanupQueue.

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
