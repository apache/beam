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
import java.io.File;
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
import org.apache.beam.runners.samza.runtime.SamzaStoreStateInternals.ByteArray;
import org.apache.beam.runners.samza.runtime.SamzaStoreStateInternals.ByteArraySerdeFactory;
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
import org.apache.samza.serializers.ByteSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.storage.kv.KeyValueStoreMetrics;
import org.apache.samza.storage.kv.RocksDbKeyValueStore;
import org.apache.samza.storage.kv.SerializedKeyValueStore;
import org.apache.samza.storage.kv.SerializedKeyValueStoreMetrics;
import org.joda.time.Instant;
import org.junit.Test;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.WriteOptions;

/**
 * Tests for {@link SamzaTimerInternalsFactory}. Covers both event-time timers and processing-timer
 * timers.
 */
public class SamzaTimerInternalsFactoryTest {
  private static KeyValueStore<ByteArray, byte[]> createStore(String name) {
    final Options options = new Options();
    options.setCreateIfMissing(true);

    RocksDbKeyValueStore rocksStore =
        new RocksDbKeyValueStore(
            new File(System.getProperty("java.io.tmpdir") + "/" + name),
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
        new ByteSerde(),
        new SerializedKeyValueStoreMetrics("beamStore", new MetricsRegistryMap()));
  }

  private static SamzaStoreStateInternals.Factory<?> createNonKeyedStateInternalsFactory(
      SamzaPipelineOptions pipelineOptions, KeyValueStore<ByteArray, byte[]> store) {
    final TaskContext context = mock(TaskContext.class);
    when(context.getStore(anyString())).thenReturn((KeyValueStore) store);
    final TupleTag<?> mainOutputTag = new TupleTag<>("output");

    return SamzaStoreStateInternals.createStateInternalFactory(
        "42", null, context, pipelineOptions, null);
  }

  private static SamzaTimerInternalsFactory<String> createTimerInternalsFactory(
      Scheduler<KeyedTimerData<String>> timerRegistry,
      String timerStateId,
      SamzaPipelineOptions pipelineOptions,
      KeyValueStore<ByteArray, byte[]> store) {

    final SamzaStoreStateInternals.Factory<?> nonKeyedStateInternalsFactory =
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

    final KeyValueStore<ByteArray, byte[]> store = createStore("store1");
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

    KeyValueStore<ByteArray, byte[]> store = createStore("store2");
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
    store = createStore("store2");
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

    KeyValueStore<ByteArray, byte[]> store = createStore("store3");
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
    store = createStore("store3");
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

    KeyValueStore<ByteArray, byte[]> store = createStore("store4");
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
