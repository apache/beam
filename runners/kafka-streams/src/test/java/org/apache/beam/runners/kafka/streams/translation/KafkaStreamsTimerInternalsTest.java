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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.api.MockProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the timer store and its fire-time index: that a timer can be set, replaced and deleted by
 * identity, and that the timers due at a watermark are found by a range scan over the index rather
 * than by inspecting every timer.
 */
public class KafkaStreamsTimerInternalsTest {

  private static final StateNamespace NAMESPACE =
      StateNamespaces.window(GlobalWindow.Coder.INSTANCE, GlobalWindow.INSTANCE);

  private KeyValueStore<byte[], byte[]> identityStore;
  private KeyValueStore<byte[], byte[]> indexStore;

  @Before
  public void setUp() {
    MockProcessorContext<Void, Void> context = new MockProcessorContext<>();
    identityStore = newStore("timers", context);
    indexStore = newStore("timers-index", context);
  }

  private static KeyValueStore<byte[], byte[]> newStore(
      String name, MockProcessorContext<Void, Void> context) {
    KeyValueStore<byte[], byte[]> store =
        Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore(name), Serdes.ByteArray(), Serdes.ByteArray())
            .withLoggingDisabled()
            .build();
    store.init(context.getStateStoreContext(), store);
    return store;
  }

  private KafkaStreamsTimerInternals timersFor(String key) {
    return new KafkaStreamsTimerInternals(
        encode(key),
        identityStore,
        indexStore,
        GlobalWindow.Coder.INSTANCE,
        BoundedWindow.TIMESTAMP_MIN_VALUE,
        BoundedWindow.TIMESTAMP_MIN_VALUE,
        new Instant(0));
  }

  private static byte[] encode(String key) {
    try {
      return CoderUtils.encodeToByteArray(StringUtf8Coder.of(), key);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static TimerData eventTimer(String id, long millis) {
    return TimerData.of(
        id, "", NAMESPACE, new Instant(millis), new Instant(millis), TimeDomain.EVENT_TIME);
  }

  /** The timers the processor would fire at {@code watermarkMillis}, in fire-time order. */
  private List<TimerData> dueAt(long watermarkMillis) {
    List<TimerData> due = new ArrayList<>();
    try (KeyValueIterator<byte[], byte[]> it =
        indexStore.range(
            KafkaStreamsTimerInternals.dueEventTimeRangeStart(),
            KafkaStreamsTimerInternals.dueEventTimeRangeEnd(watermarkMillis))) {
      while (it.hasNext()) {
        due.add(
            KafkaStreamsTimerInternals.decodeTimer(GlobalWindow.Coder.INSTANCE, it.next().value));
      }
    }
    return due;
  }

  private static int storeSize(KeyValueStore<byte[], byte[]> store) {
    int size = 0;
    try (KeyValueIterator<byte[], byte[]> it = store.all()) {
      while (it.hasNext()) {
        it.next();
        size++;
      }
    }
    return size;
  }

  @Test
  public void dueScanReturnsOnlyTimersAtOrBeforeTheWatermark() {
    KafkaStreamsTimerInternals timers = timersFor("key");
    timers.setTimer(eventTimer("early", 100L));
    timers.setTimer(eventTimer("onWatermark", 200L));
    timers.setTimer(eventTimer("late", 300L));

    List<TimerData> due = dueAt(200L);

    // Ordered by fire time, and the timer set exactly at the watermark is included.
    assertThat(due.size(), is(2));
    assertThat(due.get(0).getTimerId(), is("early"));
    assertThat(due.get(1).getTimerId(), is("onWatermark"));
  }

  @Test
  public void negativeTimestampsSortBeforePositiveOnes() {
    KafkaStreamsTimerInternals timers = timersFor("key");
    timers.setTimer(eventTimer("negative", -5000L));
    timers.setTimer(eventTimer("zero", 0L));
    timers.setTimer(eventTimer("positive", 5000L));

    List<TimerData> due = dueAt(0L);

    assertThat(due.size(), is(2));
    assertThat(due.get(0).getTimerId(), is("negative"));
    assertThat(due.get(1).getTimerId(), is("zero"));
  }

  @Test
  public void resettingATimerReplacesItsIndexEntry() {
    KafkaStreamsTimerInternals timers = timersFor("key");
    timers.setTimer(eventTimer("timer", 100L));
    // Re-setting the same timer identity for a later time must not leave the old entry behind,
    // or the timer would still fire at the time it was first set for.
    timers.setTimer(eventTimer("timer", 900L));

    assertThat(dueAt(100L).isEmpty(), is(true));
    assertThat(dueAt(900L).size(), is(1));
    assertThat(storeSize(indexStore), is(1));
    assertThat(storeSize(identityStore), is(1));
  }

  @Test
  public void deletingATimerRemovesItFromBothStores() {
    KafkaStreamsTimerInternals timers = timersFor("key");
    timers.setTimer(eventTimer("timer", 100L));
    timers.deleteTimer(NAMESPACE, "timer", "", TimeDomain.EVENT_TIME);

    assertThat(dueAt(1000L).isEmpty(), is(true));
    assertThat(storeSize(indexStore), is(0));
    assertThat(storeSize(identityStore), is(0));
  }

  @Test
  public void timersOfDifferentKeysAreIndependentButShareTheIndex() {
    timersFor("a").setTimer(eventTimer("timer", 100L));
    timersFor("b").setTimer(eventTimer("timer", 150L));

    // Same timer id under two Beam keys are two distinct timers, and one scan finds both.
    assertThat(storeSize(identityStore), is(2));
    assertThat(dueAt(200L).size(), is(2));

    timersFor("a").deleteTimer(NAMESPACE, "timer", "", TimeDomain.EVENT_TIME);
    assertThat(dueAt(200L).size(), is(1));
  }

  @Test
  public void processingTimeTimersAreNotReturnedByTheEventTimeScan() {
    KafkaStreamsTimerInternals timers = timersFor("key");
    timers.setTimer(
        TimerData.of(
            "processing",
            "",
            NAMESPACE,
            new Instant(100L),
            new Instant(100L),
            TimeDomain.PROCESSING_TIME));
    timers.setTimer(eventTimer("event", 100L));

    List<TimerData> due = dueAt(1000L);

    assertThat(due.size(), is(1));
    assertThat(due.get(0).getTimerId(), is("event"));
  }
}
