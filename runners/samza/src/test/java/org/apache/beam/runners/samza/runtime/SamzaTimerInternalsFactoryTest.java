package org.apache.beam.runners.samza.runtime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.operators.TimerRegistry;
import org.apache.samza.storage.kv.KeyValueStoreMetrics;
import org.apache.samza.storage.kv.RocksDbKeyValueStore;
import org.apache.samza.task.TaskContext;
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
  private static RocksDbKeyValueStore createStore(String name) {
    final Options options = new Options();
    options.setCreateIfMissing(true);

    return new RocksDbKeyValueStore(
        new File(System.getProperty("java.io.tmpdir") + "/" + name),
        options,
        new MapConfig(),
        false,
        "beamStore",
        new WriteOptions(),
        new FlushOptions(),
        new KeyValueStoreMetrics("beamStore", new MetricsRegistryMap()));
  }

  private static SamzaStoreStateInternals.Factory<?> createNonKeyedStateInternalsFactory(
      SamzaPipelineOptions pipelineOptions, RocksDbKeyValueStore store) {
    final TaskContext context = mock(TaskContext.class);
    when(context.getStore(anyString())).thenReturn(store);
    final TupleTag<?> mainOutputTag = new TupleTag<>("output");

    return SamzaStoreStateInternals.createStateInternalFactory(
        null, context, pipelineOptions, null, mainOutputTag);
  }

  private static SamzaTimerInternalsFactory<String> createTimerInternalsFactory(
      TimerRegistry<KeyedTimerData<String>> timerRegistry,
      String timerStateId,
      SamzaPipelineOptions pipelineOptions,
      RocksDbKeyValueStore store) {

    final SamzaStoreStateInternals.Factory<?> nonKeyedStateInternalsFactory =
        createNonKeyedStateInternalsFactory(pipelineOptions, store);

    return SamzaTimerInternalsFactory.createTimerInternalFactory(
        StringUtf8Coder.of(),
        timerRegistry,
        timerStateId,
        nonKeyedStateInternalsFactory,
        (WindowingStrategy) WindowingStrategy.globalDefault(),
        pipelineOptions);
  }

  private static class TestTimerRegistry implements TimerRegistry<KeyedTimerData<String>> {
    private final List<KeyedTimerData<String>> timers = new ArrayList<>();

    @Override
    public void register(KeyedTimerData<String> key, long timestamp) {
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
    pipelineOptions.setTimerBufferSize(1);

    final RocksDbKeyValueStore store = createStore("store1");
    final SamzaTimerInternalsFactory<String> timerInternalsFactory =
        createTimerInternalsFactory(null, "timer", pipelineOptions, store);

    final StateNamespace nameSpace = StateNamespaces.global();
    final TimerInternals timerInternals = timerInternalsFactory.timerInternalsForKey("testKey");
    final TimerInternals.TimerData timer1 =
        TimerInternals.TimerData.of("timer1", nameSpace, new Instant(10), TimeDomain.EVENT_TIME);
    timerInternals.setTimer(timer1);

    final TimerInternals.TimerData timer2 =
        TimerInternals.TimerData.of("timer2", nameSpace, new Instant(100), TimeDomain.EVENT_TIME);
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
  public void testRestore() {
    final SamzaPipelineOptions pipelineOptions =
        PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);
    pipelineOptions.setTimerBufferSize(1);

    RocksDbKeyValueStore store = createStore("store2");
    final SamzaTimerInternalsFactory<String> timerInternalsFactory =
        createTimerInternalsFactory(null, "timer", pipelineOptions, store);

    final StateNamespace nameSpace = StateNamespaces.global();
    final TimerInternals timerInternals = timerInternalsFactory.timerInternalsForKey("testKey");
    final TimerInternals.TimerData timer1 =
        TimerInternals.TimerData.of("timer1", nameSpace, new Instant(10), TimeDomain.EVENT_TIME);
    timerInternals.setTimer(timer1);

    final TimerInternals.TimerData timer2 =
        TimerInternals.TimerData.of("timer2", nameSpace, new Instant(100), TimeDomain.EVENT_TIME);
    timerInternals.setTimer(timer2);

    store.close();

    // restore by creating a new instance
    store = createStore("store2");
    final SamzaTimerInternalsFactory<String> restoredFactory =
        createTimerInternalsFactory(null, "timer", pipelineOptions, store);

    restoredFactory.setInputWatermark(new Instant(150));
    Collection<KeyedTimerData<String>> readyTimers = restoredFactory.removeReadyTimers();
    assertEquals(2, readyTimers.size());

    store.close();
  }

  @Test
  public void testProcessingTimeTimers() throws IOException {
    final SamzaPipelineOptions pipelineOptions =
        PipelineOptionsFactory.create().as(SamzaPipelineOptions.class);

    RocksDbKeyValueStore store = createStore("store3");
    TestTimerRegistry timerRegistry = new TestTimerRegistry();

    final SamzaTimerInternalsFactory<String> timerInternalsFactory =
        createTimerInternalsFactory(timerRegistry, "timer", pipelineOptions, store);

    final StateNamespace nameSpace = StateNamespaces.global();
    final TimerInternals timerInternals = timerInternalsFactory.timerInternalsForKey("testKey");
    final TimerInternals.TimerData timer1 =
        TimerInternals.TimerData.of(
            "timer1", nameSpace, new Instant(10), TimeDomain.PROCESSING_TIME);
    timerInternals.setTimer(timer1);

    final TimerInternals.TimerData timer2 =
        TimerInternals.TimerData.of(
            "timer2", nameSpace, new Instant(100), TimeDomain.PROCESSING_TIME);
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
}
