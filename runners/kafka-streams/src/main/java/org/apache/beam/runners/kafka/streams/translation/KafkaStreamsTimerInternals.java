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

import java.io.ByteArrayOutputStream;
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
 * A {@link TimerInternals} for one key, backed by a shared Kafka Streams timer {@link
 * KeyValueStore} for a GroupByKey.
 *
 * <p>Kafka Streams has no per-key timer service, so timers are persisted like any other state. A
 * timer is stored under a key built from its identity — {@code key | domain | timerFamily | timerId
 * | namespace} — so setting a timer with the same identity overwrites the previous one and deleting
 * it removes exactly that entry, matching {@link TimerInternals}' contract. The value is the {@link
 * TimerData} encoded with {@link TimerInternals.TimerDataCoderV2}, which carries the fire time.
 *
 * <p>Firing is driven by {@link WindowedGroupByKeyProcessor}: on a watermark advance it scans the
 * store for event-time timers whose fire time has passed and replays them through {@link
 * org.apache.beam.runners.core.ReduceFnRunner#onTimers}. Scanning the whole store per advance is
 * O(timers); acceptable for this first windowing support and replaceable with a fire-time-ordered
 * index later, the same way the initial GroupByKey buffered eagerly.
 *
 * <p>This instance reports the current input-watermark and processing times it was constructed with
 * (the caller advances them as reports arrive); it never fires timers itself.
 */
class KafkaStreamsTimerInternals implements TimerInternals {

  private final byte[] encodedKey;
  private final KeyValueStore<byte[], byte[]> timerStore;
  private final TimerInternals.TimerDataCoderV2 timerCoder;
  private final Instant inputWatermarkTime;
  private final Instant processingTime;

  KafkaStreamsTimerInternals(
      byte[] encodedKey,
      KeyValueStore<byte[], byte[]> timerStore,
      Coder<? extends BoundedWindow> windowCoder,
      Instant inputWatermarkTime,
      Instant processingTime) {
    this.encodedKey = encodedKey;
    this.timerStore = timerStore;
    this.timerCoder = TimerInternals.TimerDataCoderV2.of(windowCoder);
    this.inputWatermarkTime = inputWatermarkTime;
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
    timerStore.put(timerStoreKey(encodedKey, timerData), encodeTimer(timerData));
  }

  @Override
  public void deleteTimer(
      StateNamespace namespace, String timerId, String timerFamilyId, TimeDomain timeDomain) {
    timerStore.delete(timerStoreKey(encodedKey, timerId, timerFamilyId, timeDomain, namespace));
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId, String timerFamilyId) {
    throw new UnsupportedOperationException(
        "Deleting a timer without a time domain is not supported; the domain is part of a timer's"
            + " store identity.");
  }

  @Override
  public void deleteTimer(TimerData timerKey) {
    timerStore.delete(timerStoreKey(encodedKey, timerKey));
  }

  @Override
  public Instant currentProcessingTime() {
    return processingTime;
  }

  @Override
  public @Nullable Instant currentSynchronizedProcessingTime() {
    return null;
  }

  @Override
  public Instant currentInputWatermarkTime() {
    return inputWatermarkTime;
  }

  @Override
  public @Nullable Instant currentOutputWatermarkTime() {
    return null;
  }

  private byte[] encodeTimer(TimerData timerData) {
    try {
      return CoderUtils.encodeToByteArray(timerCoder, timerData);
    } catch (CoderException e) {
      throw new RuntimeException("Failed to encode timer " + timerData, e);
    }
  }

  /** The store key for a timer's identity. Package-visible so the processor can build/scan it. */
  static byte[] timerStoreKey(byte[] encodedKey, TimerData timerData) {
    return timerStoreKey(
        encodedKey,
        timerData.getTimerId(),
        timerData.getTimerFamilyId(),
        timerData.getDomain(),
        timerData.getNamespace());
  }

  static byte[] timerStoreKey(
      byte[] encodedKey,
      String timerId,
      String timerFamilyId,
      TimeDomain domain,
      StateNamespace namespace) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writeSegment(out, encodedKey);
    writeSegment(out, new byte[] {(byte) domain.ordinal()});
    writeSegment(out, timerFamilyId.getBytes(StandardCharsets.UTF_8));
    writeSegment(out, timerId.getBytes(StandardCharsets.UTF_8));
    writeSegment(out, namespace.stringKey().getBytes(StandardCharsets.UTF_8));
    return out.toByteArray();
  }

  /** Reads the encoded Beam key (the first segment) back out of a timer store key. */
  static byte[] encodedKeyOf(byte[] storeKey) {
    int length =
        ((storeKey[0] & 0xff) << 24)
            | ((storeKey[1] & 0xff) << 16)
            | ((storeKey[2] & 0xff) << 8)
            | (storeKey[3] & 0xff);
    byte[] key = new byte[length];
    System.arraycopy(storeKey, 4, key, 0, length);
    return key;
  }

  private static void writeSegment(ByteArrayOutputStream out, byte[] segment) {
    int length = segment.length;
    out.write((length >>> 24) & 0xff);
    out.write((length >>> 16) & 0xff);
    out.write((length >>> 8) & 0xff);
    out.write(length & 0xff);
    out.write(segment, 0, segment.length);
  }
}
