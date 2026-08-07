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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.state.TwsStateInternalsTest.InMemoryBytesKV;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for the Beam {@code TimerInternals} bridge, exercised against an in-memory {@link
 * BytesKV} and a recording {@link TwsTimerInternals.WakeupRegistry} rather than a live Spark query.
 *
 * <p>The interesting behaviour is not "a timer can be set", it is the reconciliation between Beam's
 * rich {@link TimerData} and Spark's bare set of {@code long} wake-ups: de-duplication, deletion of
 * wake-ups that no timer needs any more, and the same-millisecond re-arm hazard inside a timer
 * callback.
 */
@RunWith(JUnit4.class)
public class TwsTimerInternalsTest {

  private static final IntervalWindow WINDOW =
      new IntervalWindow(new Instant(0), new Instant(10_000));
  private static final IntervalWindow OTHER_WINDOW =
      new IntervalWindow(new Instant(10_000), new Instant(20_000));

  private static final StateNamespace NS =
      StateNamespaces.window(IntervalWindow.getCoder(), WINDOW);
  private static final StateNamespace OTHER_NS =
      StateNamespaces.window(IntervalWindow.getCoder(), OTHER_WINDOW);

  /** Records everything the bridge asks Spark to do with its wake-ups. */
  private static final class RecordingRegistry implements TwsTimerInternals.WakeupRegistry {
    private final Set<Long> live = new LinkedHashSet<>();
    private final List<Long> registered = new ArrayList<>();
    private final List<Long> deleted = new ArrayList<>();

    @Override
    public void register(long expiryMs) {
      registered.add(expiryMs);
      live.add(expiryMs);
    }

    @Override
    public void delete(long expiryMs) {
      deleted.add(expiryMs);
      live.remove(expiryMs);
    }

    @Override
    public Set<Long> registered() {
      return new HashSet<>(live);
    }
  }

  private TwsTimerInternals internals(
      InMemoryBytesKV store, RecordingRegistry registry, Long firedExpiryMs) {
    return internals(store, registry, firedExpiryMs, 0L);
  }

  private TwsTimerInternals internals(
      InMemoryBytesKV store, RecordingRegistry registry, Long firedExpiryMs, long watermarkMs) {
    return TwsTimerInternals.create(
        store,
        registry,
        IntervalWindow.getCoder(),
        new Instant(watermarkMs),
        new Instant(0),
        firedExpiryMs);
  }

  private static TimerData timer(String id, StateNamespace namespace, long timestampMs) {
    return TimerData.of(
        id,
        "",
        namespace,
        new Instant(timestampMs),
        new Instant(timestampMs),
        TimeDomain.EVENT_TIME);
  }

  @Test(timeout = 30_000)
  public void testSetTimerPersistsAndRegistersAWakeup() {
    InMemoryBytesKV store = new InMemoryBytesKV();
    RecordingRegistry registry = new RecordingRegistry();

    TwsTimerInternals timers = internals(store, registry, null);
    timers.setTimer(timer("t", NS, 1_000));
    assertEquals("nothing may reach Spark before flush", 0, registry.registered.size());
    assertEquals(0, store.size());

    timers.flush();
    assertEquals(Lists.newArrayList(1_000L), registry.registered);
    assertEquals(1, store.size());

    // A fresh instance over the same store sees the timer again, byte for byte.
    TwsTimerInternals reloaded = internals(store, registry, null);
    List<TimerData> loaded = Lists.newArrayList(reloaded.getTimers());
    assertEquals(1, loaded.size());
    assertEquals(timer("t", NS, 1_000), loaded.get(0));
  }

  @Test(timeout = 30_000)
  public void testTimerDataSurvivesTheStoreRoundTripInFull() throws Exception {
    TimerData original =
        TimerData.of(
            "timerId",
            "familyId",
            NS,
            new Instant(1_234),
            new Instant(1_200),
            TimeDomain.EVENT_TIME);
    TimerInternals.TimerDataCoderV2 coder =
        TimerInternals.TimerDataCoderV2.of(IntervalWindow.getCoder());

    TimerData decoded =
        CoderUtils.decodeFromByteArray(coder, CoderUtils.encodeToByteArray(coder, original));

    assertEquals(original, decoded);
    assertEquals("familyId", decoded.getTimerFamilyId());
    assertEquals(new Instant(1_200), decoded.getOutputTimestamp());
    assertEquals(NS, decoded.getNamespace());
  }

  @Test(timeout = 30_000)
  public void testWakeupsAreDeduplicated() {
    InMemoryBytesKV store = new InMemoryBytesKV();
    RecordingRegistry registry = new RecordingRegistry();

    // Two distinct Beam timers, in two namespaces, that expire in the same millisecond. This is the
    // common case, an end-of-window timer and its garbage collection timer with zero lateness.
    TwsTimerInternals timers = internals(store, registry, null);
    timers.setTimer(timer("endOfWindow", NS, 9_999));
    timers.setTimer(timer("gc", OTHER_NS, 9_999));
    timers.flush();

    assertEquals("one wake-up for two timers", Lists.newArrayList(9_999L), registry.registered);
    assertEquals("both timers are persisted", 2, store.size());
  }

  @Test(timeout = 30_000)
  public void testAnAlreadyRegisteredWakeupIsNotRegisteredAgain() {
    InMemoryBytesKV store = new InMemoryBytesKV();
    RecordingRegistry registry = new RecordingRegistry();

    TwsTimerInternals first = internals(store, registry, null);
    first.setTimer(timer("t", NS, 1_000));
    first.flush();
    assertEquals(Lists.newArrayList(1_000L), registry.registered);

    // Second invocation, same key, sets the very same timer again.
    TwsTimerInternals second = internals(store, registry, null);
    second.setTimer(timer("t", NS, 1_000));
    second.flush();

    assertEquals(
        "no duplicate registerTimer call", Lists.newArrayList(1_000L), registry.registered);
    assertEquals("and nothing was deleted either", 0, registry.deleted.size());
  }

  @Test(timeout = 30_000)
  public void testDeleteTimerRemovesTheTimerAndItsWakeup() {
    InMemoryBytesKV store = new InMemoryBytesKV();
    RecordingRegistry registry = new RecordingRegistry();

    TwsTimerInternals first = internals(store, registry, null);
    first.setTimer(timer("t", NS, 1_000));
    first.flush();

    TwsTimerInternals second = internals(store, registry, null);
    second.deleteTimer(NS, "t", "", TimeDomain.EVENT_TIME);
    second.flush();

    assertEquals(0, store.size());
    assertEquals(Lists.newArrayList(1_000L), registry.deleted);
    assertTrue(registry.live.isEmpty());
  }

  @Test(timeout = 30_000)
  public void testDeletingOneOfTwoTimersKeepsTheSharedWakeup() {
    InMemoryBytesKV store = new InMemoryBytesKV();
    RecordingRegistry registry = new RecordingRegistry();

    TwsTimerInternals first = internals(store, registry, null);
    first.setTimer(timer("a", NS, 5_000));
    first.setTimer(timer("b", OTHER_NS, 5_000));
    first.flush();

    TwsTimerInternals second = internals(store, registry, null);
    second.deleteTimer(NS, "a", "", TimeDomain.EVENT_TIME);
    second.flush();

    assertEquals("the wake-up is still needed by timer b", 0, registry.deleted.size());
    assertEquals(1, store.size());
  }

  @Test(timeout = 30_000)
  public void testMovingATimerMovesItsWakeup() {
    InMemoryBytesKV store = new InMemoryBytesKV();
    RecordingRegistry registry = new RecordingRegistry();

    TwsTimerInternals first = internals(store, registry, null);
    first.setTimer(timer("t", NS, 1_000));
    first.flush();

    // TimerData.stringKey() is namespace, domain, family and id, it does not contain the
    // timestamp, so re-setting the same timer later is an in place move of the one store entry.
    TwsTimerInternals second = internals(store, registry, null);
    second.deleteTimer(NS, "t", "", TimeDomain.EVENT_TIME);
    second.setTimer(timer("t", NS, 2_000));
    second.flush();

    assertEquals(Lists.newArrayList(1_000L, 2_000L), registry.registered);
    assertEquals(Lists.newArrayList(1_000L), registry.deleted);
    assertEquals(1, store.size());
  }

  @Test(timeout = 30_000)
  public void testRemoveTimersAtOrBeforeIsOrderedAndConsuming() {
    InMemoryBytesKV store = new InMemoryBytesKV();
    RecordingRegistry registry = new RecordingRegistry();

    TwsTimerInternals timers = internals(store, registry, null);
    timers.setTimer(timer("late", NS, 3_000));
    timers.setTimer(timer("early", NS, 1_000));
    timers.setTimer(timer("middle", NS, 2_000));

    List<TimerData> due = timers.removeTimersAtOrBefore(new Instant(2_000));
    assertEquals(2, due.size());
    assertEquals(new Instant(1_000), due.get(0).getTimestamp());
    assertEquals(new Instant(2_000), due.get(1).getTimestamp());

    assertEquals("fired timers are gone", 1, Lists.newArrayList(timers.getTimers()).size());
    assertEquals(
        "and cannot fire twice", 0, timers.removeTimersAtOrBefore(new Instant(2_000)).size());

    timers.flush();
    assertEquals("only the surviving timer is persisted", 1, store.size());
    assertEquals(Lists.newArrayList(3_000L), registry.registered);
  }

  /**
   * Spark expires a wake-up as soon as {@code expiry <= watermark}, Beam only fires an event time
   * timer once the watermark is strictly past it. A timer sitting exactly on the batch watermark
   * must therefore be withheld and re-armed rather than handed to Beam, which would swallow it.
   */
  @Test(timeout = 30_000)
  public void testTimerExactlyAtTheWatermarkIsWithheldAndReArmed() {
    InMemoryBytesKV store = new InMemoryBytesKV();
    RecordingRegistry registry = new RecordingRegistry();

    TwsTimerInternals first = internals(store, registry, null);
    first.setTimer(timer("endOfWindow", NS, 9_999));
    first.flush();
    registry.registered.clear();
    registry.deleted.clear();

    // Batch watermark is exactly 9999, so Spark expires the wake-up but Beam is not ready.
    TwsTimerInternals early = internals(store, registry, 9_999L, 9_999L);
    assertEquals(
        "a timer on the watermark is not due yet", 0, early.removeTimersReadyToFire(9_999L).size());
    early.flush();

    assertEquals("the timer survives", 1, store.size());
    assertEquals(
        "and is re-armed one millisecond later", Lists.newArrayList(10_000L), registry.registered);
    assertEquals("the firing expiry is left to Spark", 0, registry.deleted.size());

    // Next batch, the watermark has genuinely moved past the timer.
    TwsTimerInternals late = internals(store, registry, 10_000L, 20_000L);
    List<TimerData> due = late.removeTimersReadyToFire(10_000L);
    assertEquals(1, due.size());
    assertEquals(
        "the Beam timestamp is untouched by the re-arm",
        new Instant(9_999),
        due.get(0).getTimestamp());
    late.flush();
    assertEquals("and it is consumed", 0, store.size());
  }

  @Test(timeout = 30_000)
  public void testFiringExpiryIsNotDeletedByFlush() {
    InMemoryBytesKV store = new InMemoryBytesKV();
    RecordingRegistry registry = new RecordingRegistry();

    TwsTimerInternals first = internals(store, registry, null);
    first.setTimer(timer("t", NS, 1_000));
    first.flush();
    registry.deleted.clear();

    // Spark is firing 1000 and removes that wake-up itself once the callback returns. The bridge
    // must not race it with a delete of its own.
    TwsTimerInternals callback = internals(store, registry, 1_000L, 1_001L);
    assertEquals(1, callback.removeTimersReadyToFire(1_000L).size());
    callback.flush();

    assertEquals(0, registry.deleted.size());
    assertEquals(0, store.size());
  }

  @Test(timeout = 30_000)
  public void testReArmAtTheFiringMillisecondIsNudgedForward() {
    InMemoryBytesKV store = new InMemoryBytesKV();
    RecordingRegistry registry = new RecordingRegistry();

    TwsTimerInternals first = internals(store, registry, null);
    first.setTimer(timer("t", NS, 1_000));
    first.flush();
    registry.registered.clear();

    // Inside the callback for expiry 1000 the DoFn sets a new timer at 1000 again. Spark would
    // delete a wake-up registered at exactly 1000 when the callback finishes, so it has to land at
    // 1001 instead. The TimerData keeps its own timestamp of 1000.
    TwsTimerInternals callback = internals(store, registry, 1_000L, 1_001L);
    callback.removeTimersReadyToFire(1_000L);
    callback.setTimer(timer("again", NS, 1_000));
    callback.flush();

    assertEquals(Lists.newArrayList(1_001L), registry.registered);

    TwsTimerInternals next = internals(store, registry, 1_001L, 1_002L);
    List<TimerData> due = next.removeTimersReadyToFire(1_001L);
    assertEquals(1, due.size());
    assertEquals(
        "the Beam timestamp is untouched by the wake-up nudge",
        new Instant(1_000),
        due.get(0).getTimestamp());
  }

  @Test(timeout = 30_000)
  public void testProcessingTimeTimersAreRejected() {
    InMemoryBytesKV store = new InMemoryBytesKV();
    TwsTimerInternals timers = internals(store, new RecordingRegistry(), null);

    UnsupportedOperationException processing =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                timers.setTimer(
                    NS, "t", "", new Instant(1), new Instant(1), TimeDomain.PROCESSING_TIME));
    assertTrue(processing.getMessage().contains("event time timers"));

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            timers.setTimer(
                NS,
                "t",
                "",
                new Instant(1),
                new Instant(1),
                TimeDomain.SYNCHRONIZED_PROCESSING_TIME));
  }

  @Test(timeout = 30_000)
  public void testDeleteTimerWithoutTimeDomainIsRejected() {
    TwsTimerInternals timers = internals(new InMemoryBytesKV(), new RecordingRegistry(), null);
    assertThrows(UnsupportedOperationException.class, () -> timers.deleteTimer(NS, "t", ""));
  }

  @Test(timeout = 30_000)
  public void testClocksAndUnsupportedWatermarks() {
    TwsTimerInternals timers =
        TwsTimerInternals.create(
            new InMemoryBytesKV(),
            new RecordingRegistry(),
            IntervalWindow.getCoder(),
            new Instant(7_000),
            new Instant(9_000),
            null);

    assertEquals(new Instant(7_000), timers.currentInputWatermarkTime());
    assertEquals(new Instant(9_000), timers.currentProcessingTime());
    assertNull(timers.currentSynchronizedProcessingTime());
    assertNull(timers.currentOutputWatermarkTime());
  }

  @Test(timeout = 30_000)
  public void testFlushIsSingleUse() {
    TwsTimerInternals timers = internals(new InMemoryBytesKV(), new RecordingRegistry(), null);
    timers.flush();
    assertThrows(IllegalStateException.class, timers::flush);
  }
}
