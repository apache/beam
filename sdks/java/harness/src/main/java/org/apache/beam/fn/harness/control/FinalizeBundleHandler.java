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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.FinalizeBundleResponse;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(FinalizeBundleHandler.class);
  private final ConcurrentMap<String, Collection<CallbackRegistration>> bundleFinalizationCallbacks;
  private final PriorityQueue<TimestampedValue<String>> cleanUpQueue;
  private final Future<Void> cleanUpResult;

  public FinalizeBundleHandler(ExecutorService executorService) {
    this.bundleFinalizationCallbacks = new ConcurrentHashMap<>();
    this.cleanUpQueue =
        new PriorityQueue<>(11, Comparator.comparing(TimestampedValue::getTimestamp));
    this.cleanUpResult =
        executorService.submit(
            (Callable<Void>)
                () -> {
                  while (true) {
                    synchronized (cleanUpQueue) {
                      TimestampedValue<String> expiryTime = cleanUpQueue.peek();

                      // Wait until we have at least one element. We are notified on each element
                      // being added.
                      while (expiryTime == null) {
                        cleanUpQueue.wait();
                        expiryTime = cleanUpQueue.peek();
                      }

                      // Wait until the current time has past the expiry time for the head of the
                      // queue.
                      // We are notified on each element being added.
                      Instant now = Instant.now();
                      while (expiryTime.getTimestamp().isAfter(now)) {
                        Duration timeDifference = new Duration(now, expiryTime.getTimestamp());
                        cleanUpQueue.wait(timeDifference.getMillis());
                        expiryTime = cleanUpQueue.peek();
                        now = Instant.now();
                      }

                      bundleFinalizationCallbacks.remove(cleanUpQueue.poll().getValue());
                    }
                  }
                });
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
    synchronized (cleanUpQueue) {
      cleanUpQueue.offer(TimestampedValue.of(bundleId, new Instant(expiryTimeMillis)));
      cleanUpQueue.notify();
    }
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
