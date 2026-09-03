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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming.state;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.spark.sql.streaming.StatefulProcessorHandle;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * Beam {@link TimerInternals} on top of Spark 4 {@code transformWithState}.
 *
 * <p>Two things are stored per timer and they serve different purposes.
 *
 * <ul>
 *   <li>The full {@link TimerData}, encoded with {@link TimerInternals.TimerDataCoderV2}, lives in
 *       a {@link BytesKV} store keyed by {@link TimerData#stringKey()}. This is the source of truth
 *       for what fires, in which namespace, with which output timestamp.
 *   <li>A bare wake-up at the timer's expiry millisecond is registered with Spark through {@link
 *       StatefulProcessorHandle#registerTimer(long)}. Spark only knows a set of {@code long} expiry
 *       times per key, it carries no payload, so it can do nothing but wake us up.
 * </ul>
 *
 * <p><b>Wake-up de-duplication.</b> Many Beam timers can share one expiry millisecond, for example
 * the end-of-window timer and the garbage collection timer of the same window when allowed lateness
 * is zero. The Phase 0 spike found that registering the same expiry repeatedly is a real problem,
 * so wake-ups are reconciled rather than registered blindly: on {@link #flush()} the set of
 * expiries Spark currently holds for this key is read back with {@code listTimers()} and only the
 * genuine difference is registered or deleted. Registering an expiry twice is therefore impossible
 * by construction.
 *
 * <p><b>Same-millisecond re-arm inside a timer callback.</b> Spark deletes the expiry it is
 * currently firing <i>after</i> {@code handleExpiredTimer} returns and after the returned iterator
 * is drained. A wake-up re-registered at exactly that expiry from inside the callback would
 * therefore be silently removed again. When {@link #flush()} runs inside a timer callback, any
 * wake-up that would land at or before the firing expiry is nudged to {@code firedExpiry + 1}
 * instead. That is safe because the wake-up is only a wake-up: on the next callback all {@link
 * TimerData} due at or before the new expiry are fired, so the timer still fires with its own
 * timestamp and namespace. The only observable effect is that such a timer needs the watermark to
 * reach one extra millisecond.
 *
 * <p><b>Processing time timers are out of scope for this POC.</b> The operator runs in {@code
 * TimeMode.EventTime()}, in which Spark's timer registry is driven by the event time watermark
 * only, and a single {@code transformWithState} call cannot mix time modes. Setting a timer in the
 * {@link TimeDomain#PROCESSING_TIME} or {@link TimeDomain#SYNCHRONIZED_PROCESSING_TIME} domain
 * throws {@link UnsupportedOperationException}.
 *
 * <p>An instance is scoped to a single key and to a single {@code handleInputRows} or {@code
 * handleExpiredTimer} invocation. Mutations are buffered in memory and only reach the store and
 * Spark's timer registry when {@link #flush()} is called at the end of that invocation.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class TwsTimerInternals implements TimerInternals {

  /**
   * The bare {@code long} wake-up registry Spark exposes on {@link StatefulProcessorHandle}.
   *
   * <p>Extracted as an interface so the timer bridge can be unit tested without a running Spark
   * query.
   */
  public interface WakeupRegistry {

    /** Registers a wake-up at {@code expiryMs} for the current key. */
    void register(long expiryMs);

    /** Deletes the wake-up at {@code expiryMs} for the current key. */
    void delete(long expiryMs);

    /** Returns the wake-ups Spark currently holds for the current key. */
    Set<Long> registered();

    /** Returns a registry delegating to a Spark {@link StatefulProcessorHandle}. */
    static WakeupRegistry of(StatefulProcessorHandle handle) {
      return new WakeupRegistry() {
        @Override
        public void register(long expiryMs) {
          handle.registerTimer(expiryMs);
        }

        @Override
        public void delete(long expiryMs) {
          handle.deleteTimer(expiryMs);
        }

        @Override
        public Set<Long> registered() {
          Set<Long> timers = new HashSet<>();
          scala.collection.Iterator<Object> it = handle.listTimers();
          while (it.hasNext()) {
            timers.add(((Number) it.next()).longValue());
          }
          return timers;
        }
      };
    }
  }

  private final BytesKV store;
  private final WakeupRegistry registry;
  private final TimerDataCoderV2 timerCoder;
  private final Instant inputWatermark;
  private final Instant processingTime;
  private final @Nullable Long firedExpiryMs;

  /** Snapshot of the timers as they were loaded, used to compute the write-back diff. */
  private final Map<String, TimerData> loaded;

  /** Current timers, mutated by {@link #setTimer} and {@link #deleteTimer}. */
  private final Map<String, TimerData> current;

  private boolean flushed;

  private TwsTimerInternals(
      BytesKV store,
      WakeupRegistry registry,
      Coder<? extends BoundedWindow> windowCoder,
      Instant inputWatermark,
      Instant processingTime,
      @Nullable Long firedExpiryMs) {
    this.store = store;
    this.registry = registry;
    this.timerCoder = TimerDataCoderV2.of(windowCoder);
    this.inputWatermark = inputWatermark;
    this.processingTime = processingTime;
    this.firedExpiryMs = firedExpiryMs;
    this.loaded = new LinkedHashMap<>();
    for (Map.Entry<String, byte[]> entry : store.entries()) {
      loaded.put(entry.getKey(), decode(entry.getValue()));
    }
    this.current = new LinkedHashMap<>(loaded);
  }

  /**
   * Creates timer internals for one invocation.
   *
   * @param store where the encoded {@link TimerData} live
   * @param registry Spark's bare wake-up registry for the current key
   * @param windowCoder the window coder of the transform, needed to decode timer namespaces
   * @param inputWatermark the event time watermark visible for this invocation
   * @param processingTime the batch processing time
   * @param firedExpiryMs the expiry Spark is currently firing, or {@code null} outside a timer
   *     callback
   */
  public static TwsTimerInternals create(
      BytesKV store,
      WakeupRegistry registry,
      Coder<? extends BoundedWindow> windowCoder,
      Instant inputWatermark,
      Instant processingTime,
      @Nullable Long firedExpiryMs) {
    return new TwsTimerInternals(
        store, registry, windowCoder, inputWatermark, processingTime, firedExpiryMs);
  }

  private TimerData decode(byte[] bytes) {
    try {
      return CoderUtils.decodeFromByteArray(timerCoder, bytes);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to decode a stored Beam timer", e);
    }
  }

  private byte[] encode(TimerData timer) {
    try {
      return CoderUtils.encodeToByteArray(timerCoder, timer);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to encode Beam timer " + timer, e);
    }
  }

  private static void rejectUnsupportedDomain(TimeDomain domain) {
    if (domain != TimeDomain.EVENT_TIME) {
      throw new UnsupportedOperationException(
          "The Spark 4 structured streaming runner only supports event time timers, but a timer "
              + "in the "
              + domain
              + " domain was requested. Spark's transformWithState runs in a single TimeMode and "
              + "this operator uses TimeMode.EventTime(); processing time timers are out of scope "
              + "for the streaming POC.");
    }
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
  public void setTimer(TimerData timer) {
    rejectUnsupportedDomain(timer.getDomain());
    current.put(timer.stringKey(), timer);
  }

  @Override
  public void deleteTimer(
      StateNamespace namespace, String timerId, String timerFamilyId, TimeDomain timeDomain) {
    current
        .values()
        .removeIf(
            timer ->
                namespace.equals(timer.getNamespace())
                    && timerId.equals(timer.getTimerId())
                    && timerFamilyId.equals(timer.getTimerFamilyId())
                    && timeDomain.equals(timer.getDomain()));
  }

  @Override
  public void deleteTimer(StateNamespace namespace, String timerId, String timerFamilyId) {
    throw new UnsupportedOperationException(
        "Deleting a timer without a TimeDomain is not supported, use "
            + "deleteTimer(namespace, timerId, timerFamilyId, timeDomain).");
  }

  @Override
  public void deleteTimer(TimerData timer) {
    current.remove(timer.stringKey());
  }

  @Override
  public Instant currentProcessingTime() {
    return processingTime;
  }

  @Override
  public @Nullable Instant currentSynchronizedProcessingTime() {
    return null;
  }

  /**
   * Returns the event time watermark for this invocation.
   *
   * <p>This is the <b>batch start</b> watermark, that is the watermark Spark computed at the end of
   * the previous micro-batch. Elements of the current micro-batch never advance it, so an element
   * can never be late with respect to its own batch.
   */
  @Override
  public Instant currentInputWatermarkTime() {
    return inputWatermark;
  }

  @Override
  public @Nullable Instant currentOutputWatermarkTime() {
    return null;
  }

  /** Returns the timers currently held for this key, in no particular order. */
  public Iterable<TimerData> getTimers() {
    return Collections.unmodifiableCollection(new ArrayList<>(current.values()));
  }

  /**
   * Removes and returns the timers Beam considers due for the Spark wake-up at {@code
   * firedExpiryMs}, in Beam's natural timer order.
   *
   * <p>The two systems disagree on the boundary and the difference is not cosmetic.
   *
   * <ul>
   *   <li>Spark expires a {@code transformWithState} wake-up as soon as {@code expiry <=
   *       batchWatermark}.
   *   <li>Beam fires an event time timer only once the input watermark is <b>strictly past</b> the
   *       timer's timestamp, see {@code InMemoryTimerInternals.removeNextTimer} and {@code
   *       SparkTimerInternals}, both of which use {@code currentTime.isAfter(timestamp)}.
   * </ul>
   *
   * <p>Handing Beam a timer one millisecond early is silently destructive rather than merely early:
   * {@code ReduceFnRunner} asks {@code AfterWatermark.pastEndOfWindow} whether to fire, that
   * predicate is also strict, so the trigger declines, and because the runner is entitled to assume
   * the timer will not be delivered again the pane is simply lost. The end-of-window timer of a
   * fixed window sits at exactly {@code window.maxTimestamp()}, so this hits every window whose end
   * coincides with a batch watermark, which in practice means most of them.
   *
   * <p>Timers are therefore only released once {@code timestamp < currentInputWatermarkTime()}. A
   * timer withheld this way stays in the store, and {@link #flush()} re-registers a wake-up for it
   * at {@code firedExpiryMs + 1} through {@link #wakeupFor}, so it fires on the next batch whose
   * watermark has genuinely moved past it.
   *
   * <p>Firing a timer removes it, which is what makes a Spark wake-up that covers several Beam
   * timers safe: the second wake-up simply finds nothing left to fire.
   */
  public List<TimerData> removeTimersReadyToFire(long firedExpiryMs) {
    // Spark guarantees firedExpiryMs <= inputWatermark, so this only ever lowers the bound, and it
    // lowers it by at most one millisecond.
    long bound = Math.min(firedExpiryMs, inputWatermark.getMillis() - 1);
    return removeTimersAtOrBefore(new Instant(bound));
  }

  /**
   * Removes and returns, in Beam's natural timer order, every event time timer whose timestamp is
   * at or before {@code maxTimestamp}, without applying the watermark rule of {@link
   * #removeTimersReadyToFire}.
   */
  public List<TimerData> removeTimersAtOrBefore(Instant maxTimestamp) {
    List<TimerData> due = new ArrayList<>();
    for (TimerData timer : current.values()) {
      if (!timer.getTimestamp().isAfter(maxTimestamp)) {
        due.add(timer);
      }
    }
    Collections.sort(due);
    for (TimerData timer : due) {
      current.remove(timer.stringKey());
    }
    return due;
  }

  /**
   * Persists the timer changes of this invocation and reconciles Spark's wake-ups with them.
   *
   * <p>Must be called exactly once, at the end of the {@code handleInputRows} or {@code
   * handleExpiredTimer} invocation this instance belongs to.
   */
  public void flush() {
    if (flushed) {
      throw new IllegalStateException("TwsTimerInternals.flush() called more than once");
    }
    flushed = true;

    for (Map.Entry<String, TimerData> entry : current.entrySet()) {
      TimerData before = loaded.get(entry.getKey());
      if (before == null || !before.equals(entry.getValue())) {
        store.put(entry.getKey(), encode(entry.getValue()));
      }
    }
    for (String key : loaded.keySet()) {
      if (!current.containsKey(key)) {
        store.remove(key);
      }
    }

    Set<Long> desired = new HashSet<>();
    for (TimerData timer : current.values()) {
      desired.add(wakeupFor(timer.getTimestamp().getMillis()));
    }
    Set<Long> alreadyRegistered = registry.registered();
    for (Long expiry : desired) {
      if (!alreadyRegistered.contains(expiry)) {
        registry.register(expiry);
      }
    }
    for (Long expiry : alreadyRegistered) {
      // Spark removes the expiry it is currently firing on its own once the callback completes.
      if (!desired.contains(expiry) && !expiry.equals(firedExpiryMs)) {
        registry.delete(expiry);
      }
    }
  }

  /** Maps a Beam timer timestamp onto the Spark wake-up millisecond to register for it. */
  private long wakeupFor(long timestampMs) {
    if (firedExpiryMs != null && timestampMs <= firedExpiryMs) {
      return firedExpiryMs + 1;
    }
    return timestampMs;
  }
}
