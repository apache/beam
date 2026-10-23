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
package org.apache.beam.runners.kafka.streams.translation;

import java.nio.charset.StandardCharsets;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.kafka.streams.state.KeyValueStore;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A {@link TimerInternals} for one key, backed by two Kafka Streams stores shared by a GroupByKey.
 *
 * <p>Kafka Streams has no per-key timer service, so timers are persisted like any other state, in
 * two stores serving the two ways a timer is looked up. The identity store, keyed by {@code key |
 * domain | timerFamily | timerId | namespace}, is how {@link #setTimer} overwrites and {@link
 * #deleteTimer} removes exactly one timer as the contract requires; its value is the index key, so
 * an overwritten timer's index entry can be removed without knowing what time it was set for. The
 * index store, keyed by {@code domain | fireTimestamp | identity}, is how due timers are found: the
 * timestamp is in the sortable form described on {@link StoreKeys}, so every event-time timer due
 * at a watermark is one range scan rather than a scan of every timer of every key, and its value is
 * the {@link TimerData} so firing needs no second lookup.
 *
 * <p>Firing is driven by {@link WindowedGroupByKeyProcessor}, which range-scans the index on a
 * watermark advance and replays due timers through {@link
 * org.apache.beam.runners.core.ReduceFnRunner#onTimers}. This instance only reports the times it
 * was constructed with; it never fires timers itself.
 */
class KafkaStreamsTimerInternals implements TimerInternals {

  private final byte[] encodedKey;
  private final KeyValueStore<byte[], byte[]> identityStore;
  private final KeyValueStore<byte[], byte[]> indexStore;
  private final TimerInternals.TimerDataCoderV2 timerCoder;
  private final Instant inputWatermarkTime;
  private final Instant outputWatermarkTime;
  private final Instant processingTime;

  KafkaStreamsTimerInternals(
      byte[] encodedKey,
      KeyValueStore<byte[], byte[]> identityStore,
      KeyValueStore<byte[], byte[]> indexStore,
      Coder<? extends BoundedWindow> windowCoder,
      Instant inputWatermarkTime,
      Instant outputWatermarkTime,
      Instant processingTime) {
    this.encodedKey = encodedKey;
    this.identityStore = identityStore;
    this.indexStore = indexStore;
    this.timerCoder = TimerInternals.TimerDataCoderV2.of(windowCoder);
    this.inputWatermarkTime = inputWatermarkTime;
    this.outputWatermarkTime = outputWatermarkTime;
    this.processingTime = processingTime;
  }

  @Override
  public void setTimer(
      StateNamespace namespace,
      String timerId,
      String timerFamilyId,
      Instant target,
      Instant outputTimestamp,
      TimeDomain timeDomain) {
    setTimer(TimerData.of(timerId, timerFamilyId, namespace, target, outputTimestamp, timeDomain));
  }

  @Override
  public void setTimer(TimerData timerData) {
    byte[] identityKey = identityKey(encodedKey, timerData);
    // Setting a timer that already exists replaces it, so drop the old index entry first —
    // otherwise the timer would still be due at the time it was originally set for.
    byte[] previousIndexKey = identityStore.get(identityKey);
    if (previousIndexKey != null) {
      indexStore.delete(previousIndexKey);
    }
    byte[] indexKey =
        indexKey(timerData.getDomain(), timerData.getTimestamp().getMillis(), identityKey);
    identityStore.put(identityKey, indexKey);
    indexStore.put(indexKey, encodeTimer(timerData));
  }

  @Override
  public void deleteTimer(
      StateNamespace namespace, String timerId, String timerFamilyId, TimeDomain timeDomain) {
    deleteByIdentity(identityKey(encodedKey, timerId, timerFamilyId, timeDomain, namespace));
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId, String timerFamilyId) {
    throw new UnsupportedOperationException(
        "Deleting a timer without a time domain is not supported; the domain is part of a timer's"
            + " store identity.");
  }

  @Override
  public void deleteTimer(TimerData timerKey) {
    deleteByIdentity(identityKey(encodedKey, timerKey));
  }

  private void deleteByIdentity(byte[] identityKey) {
    byte[] indexKey = identityStore.get(identityKey);
    if (indexKey != null) {
      indexStore.delete(indexKey);
    }
    identityStore.delete(identityKey);
  }

  @Override
  public Instant currentProcessingTime() {
    return processingTime;
  }

  /**
   * Returns {@code null}: a synchronized processing time is the slowest processing time across the
   * job's workers, which needs the cross-instance coordination that the runner's watermark reports
   * only carry for event time. {@link TimerInternals} allows null here, and nothing on the paths
   * this runner supports today reads it — it is consulted for processing-time triggers, which land
   * with the processing-time timer support in a follow-up (the same work that would supply it).
   */
  @Override
  public @Nullable Instant currentSynchronizedProcessingTime() {
    return null;
  }

  @Override
  public Instant currentInputWatermarkTime() {
    return inputWatermarkTime;
  }

  /**
   * The watermark this GroupByKey has last forwarded downstream, which trails {@link
   * #currentInputWatermarkTime} by the pending watermark holds.
   */
  @Override
  public Instant currentOutputWatermarkTime() {
    return outputWatermarkTime;
  }

  private byte[] encodeTimer(TimerData timerData) {
    try {
      return CoderUtils.encodeToByteArray(timerCoder, timerData);
    } catch (CoderException e) {
      throw new RuntimeException("Failed to encode timer " + timerData, e);
    }
  }

  /** Decodes an index store value back into its timer. */
  static TimerData decodeTimer(Coder<? extends BoundedWindow> windowCoder, byte[] bytes) {
    try {
      return CoderUtils.decodeFromByteArray(TimerInternals.TimerDataCoderV2.of(windowCoder), bytes);
    } catch (CoderException e) {
      throw new RuntimeException("Failed to decode timer", e);
    }
  }

  /**
   * The identity store key for a timer: {@code key | domain | timerFamily | timerId | namespace}.
   */
  static byte[] identityKey(byte[] encodedKey, TimerData timerData) {
    return identityKey(
        encodedKey,
        timerData.getTimerId(),
        timerData.getTimerFamilyId(),
        timerData.getDomain(),
        timerData.getNamespace());
  }

  static byte[] identityKey(
      byte[] encodedKey,
      String timerId,
      String timerFamilyId,
      TimeDomain domain,
      StateNamespace namespace) {
    byte[] domainBytes = {(byte) domain.ordinal()};
    byte[] familyBytes = timerFamilyId.getBytes(StandardCharsets.UTF_8);
    byte[] idBytes = timerId.getBytes(StandardCharsets.UTF_8);
    byte[] namespaceBytes = namespace.stringKey().getBytes(StandardCharsets.UTF_8);
    byte[] key =
        new byte
            [StoreKeys.segmentLength(encodedKey)
                + StoreKeys.segmentLength(domainBytes)
                + StoreKeys.segmentLength(familyBytes)
                + StoreKeys.segmentLength(idBytes)
                + StoreKeys.segmentLength(namespaceBytes)];
    int offset = StoreKeys.writeSegment(key, 0, encodedKey);
    offset = StoreKeys.writeSegment(key, offset, domainBytes);
    offset = StoreKeys.writeSegment(key, offset, familyBytes);
    offset = StoreKeys.writeSegment(key, offset, idBytes);
    StoreKeys.writeSegment(key, offset, namespaceBytes);
    return key;
  }

  /** The index store key for a timer: {@code domain | fireTimestamp | identity}. */
  static byte[] indexKey(TimeDomain domain, long fireMillis, byte[] identityKey) {
    byte[] key = new byte[1 + StoreKeys.TIMESTAMP_BYTES + identityKey.length];
    key[0] = (byte) domain.ordinal();
    int offset = StoreKeys.writeTimestamp(key, 1, fireMillis);
    System.arraycopy(identityKey, 0, key, offset, identityKey.length);
    return key;
  }

  /** Inclusive lower bound of the range scan for due event-time timers. */
  static byte[] dueEventTimeRangeStart() {
    byte[] bound = new byte[1 + StoreKeys.TIMESTAMP_BYTES];
    bound[0] = (byte) TimeDomain.EVENT_TIME.ordinal();
    StoreKeys.writeTimestamp(bound, 1, Long.MIN_VALUE);
    return bound;
  }

  /**
   * Inclusive upper bound of the range scan for event-time timers due at {@code watermarkMillis}.
   *
   * <p>Every index key carries a non-empty identity after its timestamp, so no key is equal to the
   * bare {@code domain | watermark + 1} prefix returned here: an inclusive scan up to it yields
   * exactly the timers whose fire time is at or before the watermark. Beam's maximum timestamp is
   * far below {@link Long#MAX_VALUE}, so the increment cannot overflow.
   */
  static byte[] dueEventTimeRangeEnd(long watermarkMillis) {
    byte[] bound = new byte[1 + StoreKeys.TIMESTAMP_BYTES];
    bound[0] = (byte) TimeDomain.EVENT_TIME.ordinal();
    StoreKeys.writeTimestamp(bound, 1, watermarkMillis + 1);
    return bound;
  }

  /** Reads the identity key back out of an index key. */
  static byte[] identityKeyOf(byte[] indexKey) {
    int offset = 1 + StoreKeys.TIMESTAMP_BYTES;
    byte[] identityKey = new byte[indexKey.length - offset];
    System.arraycopy(indexKey, offset, identityKey, 0, identityKey.length);
    return identityKey;
  }

  /** Reads the encoded Beam key (the first segment) back out of an identity key. */
  static byte[] encodedKeyOf(byte[] identityKey) {
    return StoreKeys.readSegment(identityKey, 0);
  }
}
