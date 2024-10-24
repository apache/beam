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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import static org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateUtil.encodeKey;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.dataflow.worker.WindmillTimeUtils;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Futures;
import org.joda.time.Instant;

@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class WindmillWatermarkHold extends WindmillState implements WatermarkHoldState {
  // The encoded size of an Instant.
  private static final int ENCODED_SIZE = 8;

  private final TimestampCombiner timestampCombiner;
  private final StateNamespace namespace;
  private final StateTag<WatermarkHoldState> address;
  private final ByteString stateKey;
  private final String stateFamily;

  private boolean cleared = false;
  /**
   * If non-{@literal null}, the known current hold value, or absent if we know there are no output
   * watermark holds. If {@literal null}, the current hold value could depend on holds in Windmill
   * we do not yet know.
   */
  private Optional<Instant> cachedValue = null;

  private Instant localAdditions = null;

  WindmillWatermarkHold(
      StateNamespace namespace,
      StateTag<WatermarkHoldState> address,
      String stateFamily,
      TimestampCombiner timestampCombiner,
      boolean isNewKey) {
    this.namespace = namespace;
    this.address = address;
    this.stateKey = encodeKey(namespace, address);
    this.stateFamily = stateFamily;
    this.timestampCombiner = timestampCombiner;
    if (isNewKey) {
      cachedValue = Optional.absent();
    }
  }

  @Override
  public void clear() {
    cleared = true;
    cachedValue = Optional.absent();
    localAdditions = null;
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public WindmillWatermarkHold readLater() {
    getFuture();
    return this;
  }

  @Override
  public Instant read() {
    try (Closeable scope = scopedReadState()) {
      Instant persistedHold = getFuture().get();
      if (persistedHold == null) {
        cachedValue = Optional.absent();
      } else {
        cachedValue = Optional.of(persistedHold);
      }
    } catch (InterruptedException | ExecutionException | IOException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException("Unable to read state", e);
    }

    if (localAdditions == null) {
      return cachedValue.orNull();
    } else if (!cachedValue.isPresent()) {
      return localAdditions;
    } else {
      return timestampCombiner.combine(localAdditions, cachedValue.get());
    }
  }

  @Override
  public ReadableState<Boolean> isEmpty() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void add(Instant outputTime) {
    localAdditions =
        (localAdditions == null)
            ? outputTime
            : timestampCombiner.combine(outputTime, localAdditions);
  }

  @Override
  public TimestampCombiner getTimestampCombiner() {
    return timestampCombiner;
  }

  @Override
  public Future<Windmill.WorkItemCommitRequest> persist(
      final WindmillStateCache.ForKeyAndFamily cache) {

    Future<Windmill.WorkItemCommitRequest> result;

    if (!cleared && localAdditions == null) {
      // No changes, so no need to update Windmill and no need to cache any value.
      return Futures.immediateFuture(Windmill.WorkItemCommitRequest.newBuilder().buildPartial());
    }

    if (cleared && localAdditions == null) {
      // Just clearing the persisted state; blind delete
      Windmill.WorkItemCommitRequest.Builder commitBuilder =
          Windmill.WorkItemCommitRequest.newBuilder();
      commitBuilder
          .addWatermarkHoldsBuilder()
          .setTag(stateKey)
          .setStateFamily(stateFamily)
          .setReset(true);

      result = Futures.immediateFuture(commitBuilder.buildPartial());
    } else if (cleared && localAdditions != null) {
      // Since we cleared before adding, we can do a blind overwrite of persisted state
      Windmill.WorkItemCommitRequest.Builder commitBuilder =
          Windmill.WorkItemCommitRequest.newBuilder();
      commitBuilder
          .addWatermarkHoldsBuilder()
          .setTag(stateKey)
          .setStateFamily(stateFamily)
          .setReset(true)
          .addTimestamps(WindmillTimeUtils.harnessToWindmillTimestamp(localAdditions));

      cachedValue = Optional.of(localAdditions);

      result = Futures.immediateFuture(commitBuilder.buildPartial());
    } else if (!cleared && localAdditions != null) {
      // Otherwise, we need to combine the local additions with the already persisted data
      result = combineWithPersisted();
    } else {
      throw new IllegalStateException("Unreachable condition");
    }

    final int estimatedByteSize = ENCODED_SIZE + stateKey.size();
    return Futures.lazyTransform(
        result,
        result1 -> {
          cleared = false;
          localAdditions = null;
          if (cachedValue != null) {
            cache.put(namespace, address, WindmillWatermarkHold.this, estimatedByteSize);
          }
          return result1;
        });
  }

  private Future<Instant> getFuture() {
    return cachedValue != null
        ? Futures.immediateFuture(cachedValue.orNull())
        : reader.watermarkFuture(stateKey, stateFamily);
  }

  /**
   * Combines local additions with persisted data and mutates the {@code commitBuilder} to write the
   * result.
   */
  private Future<Windmill.WorkItemCommitRequest> combineWithPersisted() {
    boolean windmillCanCombine = false;

    // If the combined output time depends only on the window, then we are just blindly adding
    // the same value that may or may not already be present. This depends on the state only being
    // used for one window.
    windmillCanCombine |= timestampCombiner.dependsOnlyOnWindow();

    // If the combined output time depends only on the earliest input timestamp, then because
    // assignOutputTime is monotonic, the hold only depends on the earliest output timestamp
    // (which is the value submitted as a watermark hold). The only way holds for later inputs
    // can be redundant is if the are later (or equal) to the earliest. So taking the MIN
    // implicitly, as Windmill does, has the desired behavior.
    windmillCanCombine |= timestampCombiner.dependsOnlyOnEarliestTimestamp();

    if (windmillCanCombine) {
      // We do a blind write and let Windmill take the MIN
      Windmill.WorkItemCommitRequest.Builder commitBuilder =
          Windmill.WorkItemCommitRequest.newBuilder();
      commitBuilder
          .addWatermarkHoldsBuilder()
          .setTag(stateKey)
          .setStateFamily(stateFamily)
          .addTimestamps(WindmillTimeUtils.harnessToWindmillTimestamp(localAdditions));

      if (cachedValue != null) {
        cachedValue =
            Optional.of(
                cachedValue.isPresent()
                    ? timestampCombiner.combine(cachedValue.get(), localAdditions)
                    : localAdditions);
      }

      return Futures.immediateFuture(commitBuilder.buildPartial());
    } else {
      // The non-fast path does a read-modify-write
      return Futures.lazyTransform(
          (cachedValue != null)
              ? Futures.immediateFuture(cachedValue.orNull())
              : reader.watermarkFuture(stateKey, stateFamily),
          priorHold -> {
            cachedValue =
                Optional.of(
                    (priorHold != null)
                        ? timestampCombiner.combine(priorHold, localAdditions)
                        : localAdditions);
            Windmill.WorkItemCommitRequest.Builder commitBuilder =
                Windmill.WorkItemCommitRequest.newBuilder();
            commitBuilder
                .addWatermarkHoldsBuilder()
                .setTag(stateKey)
                .setStateFamily(stateFamily)
                .setReset(true)
                .addTimestamps(WindmillTimeUtils.harnessToWindmillTimestamp(cachedValue.get()));

            return commitBuilder.buildPartial();
          });
    }
  }
}
