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
package org.apache.beam.runners.samza.runtime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.runtime.SamzaStateInternals.ByteArray;
import org.apache.beam.runners.samza.runtime.SamzaStateInternals.ByteArraySerdeFactory;
import org.apache.beam.runners.samza.runtime.SamzaStateInternals.StateValue;
import org.apache.beam.runners.samza.runtime.SamzaStateInternals.StateValueSerdeFactory;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.samza.config.MapConfig;
import org.apache.samza.context.TaskContext;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.operators.Scheduler;
import org.apache.samza.serializers.Serde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.storage.kv.KeyValueStoreMetrics;
import org.apache.samza.storage.kv.RocksDbKeyValueStore;
import org.apache.samza.storage.kv.SerializedKeyValueStore;
import org.apache.samza.storage.kv.SerializedKeyValueStoreMetrics;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.WriteOptions;

/**
 * Tests for {@link SamzaTimerInternalsFactory}. Covers both event-time timers and processing-timer
 * timers.
 */
public class SamzaTimerInternalsFactoryTest {
  @Rule public transient TemporaryFolder temporaryFolder = new TemporaryFolder();

  private KeyValueStore<ByteArray, StateValue<?>> createStore() {
    final Options options = new Options();
    options.setCreateIfMissing(true);

    RocksDbKeyValueStore rocksStore =
        new RocksDbKeyValueStore(
            temporaryFolder.getRoot(),
            options,
            new MapConfig(),
            false,
            "beamStore",
            new WriteOptions(),
            new FlushOptions(),
            new KeyValueStoreMetrics("beamStore", new MetricsRegistryMap()));

    return new SerializedKeyValueStore<>(
        rocksStore,
        new ByteArraySerdeFactory.ByteArraySerde(),
        new StateValueSerdeFactory.StateValueSerde(),
        new SerializedKeyValueStoreMetrics("beamStore", new MetricsRegistryMap()));
  }

  private static SamzaStateInternals.Factory<?> createNonKeyedStateInternalsFactory(
      SamzaPipelineOptions pipelineOptions, KeyValueStore<ByteArray, StateValue<?>> store) {
    final TaskContext context = mock(TaskContext.class);
    when(context.getStore(anyString())).thenReturn((KeyValueStore) store);
    final TupleTag<?> mainOutputTag = new TupleTag<>("output");

    return SamzaStateInternals.createStateInternalFactory(
        "42", null, context, pipelineOptions, null);
  }

  private static SamzaTimerInternalsFactory<String> createTimerInternalsFactory(
      Scheduler<KeyedTimerData<String>> timerRegistry,
      String timerStateId,
      SamzaPipelineOptions pipelineOptions,
      KeyValueStore<ByteArray, StateValue<?>> store) {

    final SamzaStateInternals.Factory<?> nonKeyedStateInternalsFactory =
        createNonKeyedStateInternalsFactory(pipelineOptions, store);

    return SamzaTimerInternalsFactory.createTimerInternalFactory(
        StringUtf8Coder.of(),
        timerRegistry,
        timerStateId,
        nonKeyedStateInternalsFactory,
        (WindowingStrategy) WindowingStrategy.globalDefault(),
        PCollection.IsBounded.BOUNDED,
        pipelineOptions);
  }

  private static class TestTimerRegistry implements Scheduler<KeyedTimerData<String>> {
    private final List<KeyedTimerData<String>> timers = new ArrayList<>();

    @Override
    public void schedule(KeyedTimerData<String> key, long timestamp) {
      timers.add(key);
    }

    @Override
    public void delete(KeyedTimerData<String> key) {
      timers.remove(key);
    }
  }

  @Test
  public void testEventTimeTimers() {
    final SamzaPipelineOptions pipelineOptions =
        PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);

    final KeyValueStore<ByteArray, StateValue<?>> store = createStore();
    final SamzaTimerInternalsFactory<String> timerInternalsFactory =
        createTimerInternalsFactory(null, "timer", pipelineOptions, store);

    final StateNamespace nameSpace = StateNamespaces.global();
    final TimerInternals timerInternals = timerInternalsFactory.timerInternalsForKey("testKey");
    final TimerInternals.TimerData timer1 =
        TimerInternals.TimerData.of(
            "timer1", nameSpace, new Instant(10), new Instant(10), TimeDomain.EVENT_TIME);
    timerInternals.setTimer(timer1);

    final TimerInternals.TimerData timer2 =
        TimerInternals.TimerData.of(
            "timer2", nameSpace, new Instant(100), new Instant(100), TimeDomain.EVENT_TIME);
    timerInternals.setTimer(timer2);

    timerInternalsFactory.setInputWatermark(new Instant(5));
    Collection<KeyedTimerData<String>> readyTimers = timerInternalsFactory.removeReadyTimers();
    assertTrue(readyTimers.isEmpty());

    timerInternalsFactory.setInputWatermark(new Instant(20));
    readyTimers = timerInternalsFactory.removeReadyTimers();
    assertEquals(1, readyTimers.size());
    assertEquals(timer1, readyTimers.iterator().next().getTimerData());

    timerInternalsFactory.setInputWatermark(new Instant(150));
    readyTimers = timerInternalsFactory.removeReadyTimers();
    assertEquals(1, readyTimers.size());
    assertEquals(timer2, readyTimers.iterator().next().getTimerData());

    store.close();
  }

  @Test
  public void testRestore() throws Exception {
    final SamzaPipelineOptions pipelineOptions =
        PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);

    KeyValueStore<ByteArray, StateValue<?>> store = createStore();
    final SamzaTimerInternalsFactory<String> timerInternalsFactory =
        createTimerInternalsFactory(null, "timer", pipelineOptions, store);

    final String key = "testKey";
    final StateNamespace nameSpace = StateNamespaces.global();
    final TimerInternals timerInternals = timerInternalsFactory.timerInternalsForKey(key);
    final TimerInternals.TimerData timer1 =
        TimerInternals.TimerData.of(
            "timer1", nameSpace, new Instant(10), new Instant(10), TimeDomain.EVENT_TIME);
    timerInternals.setTimer(timer1);

    final TimerInternals.TimerData timer2 =
        TimerInternals.TimerData.of(
            "timer2", nameSpace, new Instant(100), new Instant(100), TimeDomain.EVENT_TIME);
    timerInternals.setTimer(timer2);

    store.close();

    // restore by creating a new instance
    store = createStore();
    final SamzaTimerInternalsFactory<String> restoredFactory =
        createTimerInternalsFactory(null, "timer", pipelineOptions, store);

    restoredFactory.setInputWatermark(new Instant(150));
    Collection<KeyedTimerData<String>> readyTimers = restoredFactory.removeReadyTimers();
    assertEquals(2, readyTimers.size());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    StringUtf8Coder.of().encode(key, baos);
    byte[] keyBytes = baos.toByteArray();
    assertEquals(
        readyTimers,
        Arrays.asList(
            new KeyedTimerData<>(keyBytes, key, timer1),
            new KeyedTimerData<>(keyBytes, key, timer2)));

    store.close();
  }

  @Test
  public void testProcessingTimeTimers() throws IOException {
    final SamzaPipelineOptions pipelineOptions =
        PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);

    KeyValueStore<ByteArray, StateValue<?>> store = createStore();
    TestTimerRegistry timerRegistry = new TestTimerRegistry();

    final SamzaTimerInternalsFactory<String> timerInternalsFactory =
        createTimerInternalsFactory(timerRegistry, "timer", pipelineOptions, store);

    final StateNamespace nameSpace = StateNamespaces.global();
    final TimerInternals timerInternals = timerInternalsFactory.timerInternalsForKey("testKey");
    final TimerInternals.TimerData timer1 =
        TimerInternals.TimerData.of(
            "timer1", nameSpace, new Instant(10), new Instant(10), TimeDomain.PROCESSING_TIME);
    timerInternals.setTimer(timer1);

    final TimerInternals.TimerData timer2 =
        TimerInternals.TimerData.of(
            "timer2", nameSpace, new Instant(100), new Instant(100), TimeDomain.PROCESSING_TIME);
    timerInternals.setTimer(timer2);

    assertEquals(2, timerRegistry.timers.size());

    store.close();

    // restore by creating a new instance
    store = createStore();
    TestTimerRegistry restoredRegistry = new TestTimerRegistry();
    final SamzaTimerInternalsFactory<String> restoredFactory =
        createTimerInternalsFactory(restoredRegistry, "timer", pipelineOptions, store);

    assertEquals(2, restoredRegistry.timers.size());

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    StringUtf8Coder.of().encode("testKey", baos);
    final byte[] keyBytes = baos.toByteArray();
    restoredFactory.removeProcessingTimer(new KeyedTimerData(keyBytes, "testKey", timer1));
    restoredFactory.removeProcessingTimer(new KeyedTimerData(keyBytes, "testKey", timer2));

    store.close();
  }

  @Test
  public void testOverride() {
    final SamzaPipelineOptions pipelineOptions =
        PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);

    KeyValueStore<ByteArray, StateValue<?>> store = createStore();
    final SamzaTimerInternalsFactory<String> timerInternalsFactory =
        createTimerInternalsFactory(null, "timer", pipelineOptions, store);

    final StateNamespace nameSpace = StateNamespaces.global();
    final TimerInternals timerInternals = timerInternalsFactory.timerInternalsForKey("testKey");
    final TimerInternals.TimerData timer1 =
        TimerInternals.TimerData.of(
            "timerId", nameSpace, new Instant(10), new Instant(10), TimeDomain.EVENT_TIME);
    timerInternals.setTimer(timer1);

    // this timer should override the first timer
    final TimerInternals.TimerData timer2 =
        TimerInternals.TimerData.of(
            "timerId", nameSpace, new Instant(100), new Instant(100), TimeDomain.EVENT_TIME);
    timerInternals.setTimer(timer2);

    final TimerInternals.TimerData timer3 =
        TimerInternals.TimerData.of(
            "timerId2", nameSpace, new Instant(200), new Instant(200), TimeDomain.EVENT_TIME);
    timerInternals.setTimer(timer3);

    // this timer shouldn't override since it has a different id
    timerInternalsFactory.setInputWatermark(new Instant(50));
    Collection<KeyedTimerData<String>> readyTimers = timerInternalsFactory.removeReadyTimers();
    assertEquals(0, readyTimers.size());

    timerInternalsFactory.setInputWatermark(new Instant(150));
    readyTimers = timerInternalsFactory.removeReadyTimers();
    assertEquals(1, readyTimers.size());

    timerInternalsFactory.setInputWatermark(new Instant(250));
    readyTimers = timerInternalsFactory.removeReadyTimers();
    assertEquals(1, readyTimers.size());

    store.close();
  }

  /**
   * Test the number of event time timers maintained in memory does not go beyond the limit defined
   * in pipeline option.
   */
  @Test
  public void testEventTimeTimersMemoryBoundary1() {
    final SamzaPipelineOptions pipelineOptions =
        PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    pipelineOptions.setEventTimerBufferSize(2);

    final KeyValueStore<ByteArray, StateValue<?>> store = createStore();
    final SamzaTimerInternalsFactory<String> timerInternalsFactory =
        createTimerInternalsFactory(null, "timer", pipelineOptions, store);

    final StateNamespace nameSpace = StateNamespaces.global();
    final TimerInternals timerInternals = timerInternalsFactory.timerInternalsForKey("testKey");

    // prepare 5 timers.
    // timers in memory are then timestamped from 0 - 1;
    // timers in store are then timestamped from 0 - 4.
    TimerInternals.TimerData timer;
    for (int i = 0; i < 5; i++) {
      timer =
          TimerInternals.TimerData.of(
              "timer" + i, nameSpace, new Instant(i), new Instant(i), TimeDomain.EVENT_TIME);
      timerInternals.setTimer(timer);
    }

    timerInternalsFactory.setInputWatermark(new Instant(2));
    Collection<KeyedTimerData<String>> readyTimers;

    readyTimers = timerInternalsFactory.removeReadyTimers();
    assertEquals(2, readyTimers.size());
    assertEquals(2, timerInternalsFactory.getEventTimeBuffer().size());

    store.close();
  }

  /**
   * Test the total number of event time timers reloaded into memory is aligned with the number of
   * event time timers written to the store.
   */
  @Test
  public void testEventTimeTimersMemoryBoundary2() {
    final SamzaPipelineOptions pipelineOptions =
        PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    pipelineOptions.setEventTimerBufferSize(2);

    final KeyValueStore<ByteArray, StateValue<?>> store = createStore();
    final SamzaTimerInternalsFactory<String> timerInternalsFactory =
        createTimerInternalsFactory(null, "timer", pipelineOptions, store);

    final StateNamespace nameSpace = StateNamespaces.global();
    final TimerInternals timerInternals = timerInternalsFactory.timerInternalsForKey("testKey");

    // prepare 3 timers.
    // timers in memory now are timestamped from 0 - 1;
    // timers in store now are timestamped from 0 - 2.
    TimerInternals.TimerData timer;
    for (int i = 0; i < 3; i++) {
      timer =
          TimerInternals.TimerData.of(
              "timer" + i, nameSpace, new Instant(i), new Instant(i), TimeDomain.EVENT_TIME);
      timerInternals.setTimer(timer);
    }

    // total number of event time timers to fire equals to the number of timers in store
    Collection<KeyedTimerData<String>> readyTimers;
    timerInternalsFactory.setInputWatermark(new Instant(3));
    readyTimers = timerInternalsFactory.removeReadyTimers();
    assertEquals(3, readyTimers.size());

    store.close();
  }

  /**
   * Test the total number of event time timers reloaded into memory is aligned with the number of
   * the event time timers written to the store. Moreover, event time timers reloaded into memory is
   * maintained in order.
   */
  @Test
  public void testEventTimeTimersMemoryBoundary3() {
    final SamzaPipelineOptions pipelineOptions =
        PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    pipelineOptions.setEventTimerBufferSize(5);

    final KeyValueStore<ByteArray, StateValue<?>> store = createStore();
    final SamzaTimerInternalsFactory<String> timerInternalsFactory =
        createTimerInternalsFactory(null, "timer", pipelineOptions, store);

    final StateNamespace nameSpace = StateNamespaces.global();
    final TimerInternals timerInternals = timerInternalsFactory.timerInternalsForKey("testKey");

    // prepare 8 timers.
    // timers in memory now are timestamped from 0 - 4;
    // timers in store now are timestamped from 0 - 7.
    TimerInternals.TimerData timer;
    for (int i = 0; i < 8; i++) {
      timer =
          TimerInternals.TimerData.of(
              "timer" + i, nameSpace, new Instant(i), new Instant(i), TimeDomain.EVENT_TIME);
      timerInternals.setTimer(timer);
    }

    // fire the first 2 timers.
    // timers in memory now are timestamped from 2 - 4;
    // timers in store now are timestamped from 2 - 7.
    Collection<KeyedTimerData<String>> readyTimers;
    timerInternalsFactory.setInputWatermark(new Instant(2));
    long lastTimestamp = 0;
    readyTimers = timerInternalsFactory.removeReadyTimers();
    for (KeyedTimerData<String> keyedTimerData : readyTimers) {
      final long currentTimeStamp = keyedTimerData.getTimerData().getTimestamp().getMillis();
      assertTrue(lastTimestamp <= currentTimeStamp);
      lastTimestamp = currentTimeStamp;
    }
    assertEquals(2, readyTimers.size());

    // add another 12 timers.
    // timers in memory (reloaded for three times) now are timestamped from 2 - 4; 5 - 9; 10 - 14;
    // 15 - 19.
    // timers in store now are timestamped from 2 - 19.
    // the total number of timers to fire is 18.
    for (int i = 8; i < 20; i++) {
      timer =
          TimerInternals.TimerData.of(
              "timer" + i, nameSpace, new Instant(i), new Instant(i), TimeDomain.EVENT_TIME);
      timerInternals.setTimer(timer);
    }
    timerInternalsFactory.setInputWatermark(new Instant(20));
    lastTimestamp = 0;
    readyTimers = timerInternalsFactory.removeReadyTimers();
    for (KeyedTimerData<String> keyedTimerData : readyTimers) {
      final long currentTimeStamp = keyedTimerData.getTimerData().getTimestamp().getMillis();
      assertTrue(lastTimestamp <= currentTimeStamp);
      lastTimestamp = currentTimeStamp;
    }
    assertEquals(18, readyTimers.size());

    store.close();
  }

  /**
   * Test the total number of event time timers reloaded into memory is aligned with the number of
   * the event time timers written to the store. Moreover, event time timers reloaded into memory is
   * maintained in order, even though memory boundary is hit and timer is early than the last timer
   * in memory.
   */
  @Test
  public void testEventTimeTimersMemoryBoundary4() {
    final SamzaPipelineOptions pipelineOptions =
        PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    pipelineOptions.setEventTimerBufferSize(5);

    final KeyValueStore<ByteArray, StateValue<?>> store = createStore();
    final SamzaTimerInternalsFactory<String> timerInternalsFactory =
        createTimerInternalsFactory(null, "timer", pipelineOptions, store);

    final StateNamespace nameSpace = StateNamespaces.global();
    final TimerInternals timerInternals = timerInternalsFactory.timerInternalsForKey("testKey");

    // prepare 8 timers.
    // timers in memory now are timestamped from 0 - 4;
    // timers in store now are timestamped from 0 - 9.
    TimerInternals.TimerData timer;
    for (int i = 0; i < 10; i++) {
      timer =
          TimerInternals.TimerData.of(
              "timer" + i, nameSpace, new Instant(i), new Instant(i), TimeDomain.EVENT_TIME);
      timerInternals.setTimer(timer);
    }

    // fire the first 2 timers.
    // timers in memory now are timestamped from 2 - 4;
    // timers in store now are timestamped from 2 - 9.
    Collection<KeyedTimerData<String>> readyTimers;
    timerInternalsFactory.setInputWatermark(new Instant(2));
    long lastTimestamp = 0;
    readyTimers = timerInternalsFactory.removeReadyTimers();
    for (KeyedTimerData<String> keyedTimerData : readyTimers) {
      final long currentTimeStamp = keyedTimerData.getTimerData().getTimestamp().getMillis();
      assertTrue(lastTimestamp <= currentTimeStamp);
      lastTimestamp = currentTimeStamp;
    }
    assertEquals(2, readyTimers.size());

    // add 3 timers.
    // timers in memory now are timestamped from 0 to 2 prefixed with lateTimer, and 2 to
    // 4 prefixed with timer, timestamp is in order;
    // timers in store now are timestamped from 0 to 2 prefixed with lateTimer, and 2 to 9
    // prefixed with timer, timestamp is in order;
    for (int i = 0; i < 3; i++) {
      timer =
          TimerInternals.TimerData.of(
              "lateTimer" + i, nameSpace, new Instant(i), new Instant(i), TimeDomain.EVENT_TIME);
      timerInternals.setTimer(timer);
    }

    // there are 11 timers in state now.
    // watermark 5 comes, so 6 timers will be evicted because their timestamp is less than 5.
    // memory will be reloaded once to have 5 to 8 left (reload to have 4 to 8, but 4 is evicted), 5
    // to 9 left in store.
    // all of them are in order for firing.
    timerInternalsFactory.setInputWatermark(new Instant(5));
    lastTimestamp = 0;
    readyTimers = timerInternalsFactory.removeReadyTimers();
    for (KeyedTimerData<String> keyedTimerData : readyTimers) {
      final long currentTimeStamp = keyedTimerData.getTimerData().getTimestamp().getMillis();
      assertTrue(lastTimestamp <= currentTimeStamp);
      lastTimestamp = currentTimeStamp;
    }
    assertEquals(6, readyTimers.size());
    assertEquals(4, timerInternalsFactory.getEventTimeBuffer().size());

    // watermark 10 comes, so all timers will be evicted in order.
    timerInternalsFactory.setInputWatermark(new Instant(10));
    readyTimers = timerInternalsFactory.removeReadyTimers();
    for (KeyedTimerData<String> keyedTimerData : readyTimers) {
      final long currentTimeStamp = keyedTimerData.getTimerData().getTimestamp().getMillis();
      assertTrue(lastTimestamp <= currentTimeStamp);
      lastTimestamp = currentTimeStamp;
    }
    assertEquals(5, readyTimers.size());
    assertEquals(0, timerInternalsFactory.getEventTimeBuffer().size());

    store.close();
  }

  @Test
  public void testByteArray() {
    ByteArray key1 = ByteArray.of("hello world".getBytes(StandardCharsets.UTF_8));
    Serde<ByteArray> serde = new ByteArraySerdeFactory().getSerde("", null);
    byte[] keyBytes = serde.toBytes(key1);
    ByteArray key2 = serde.fromBytes(keyBytes);
    assertEquals(key1, key2);

    Map<ByteArray, String> map = new HashMap<>();
    map.put(key1, "found it");
    assertEquals("found it", map.get(key2));

    map.remove(key1);
    assertTrue(!map.containsKey(key2));
    assertTrue(map.isEmpty());
  }
}
