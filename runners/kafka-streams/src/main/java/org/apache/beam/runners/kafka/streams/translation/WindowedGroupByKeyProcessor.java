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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.ReduceFnRunner;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.triggers.ExecutableTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachines;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.construction.TriggerTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.BaseEncoding;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes a windowed {@code GroupByKey} by driving Beam's {@link ReduceFnRunner} — the same
 * windowing + triggering state machine the Flink and Spark portable runners use — with Kafka
 * Streams state and timers behind it.
 *
 * <p>Records arrive on the repartition topic keyed by the encoded Beam key, so every value of a key
 * is co-located here. For each data record this builds a {@link ReduceFnRunner} for that key over a
 * {@link KafkaStreamsStateInternals} and {@link KafkaStreamsTimerInternals} (both backed by
 * persistent stores) and feeds the element in; the runner assigns it to windows, updates the
 * trigger state, and sets any timers it needs. When the aggregated input watermark advances, every
 * event-time timer whose fire time has passed is replayed through {@code onTimers}, which is what
 * makes windows emit their panes. The runner is stateless between records — all durable state lives
 * in the stores — so a fresh one per record is correct, mirroring {@code
 * GroupAlsoByWindowViaWindowSetNewDoFn}.
 *
 * <p>This first version supports the default trigger and non-merging windows well; richer triggers,
 * processing-time timers and session (merging) windows build on the same machinery in a follow-up.
 */
class WindowedGroupByKeyProcessor<K, V, W extends BoundedWindow>
    implements Processor<byte[], KStreamsPayload<?>, byte[], KStreamsPayload<?>> {

  private static final Logger LOG = LoggerFactory.getLogger(WindowedGroupByKeyProcessor.class);

  private final String stateStoreName;
  private final String holdsIndexStoreName;
  private final String timerStoreName;
  private final String timerIndexStoreName;
  private final String transformId;
  private final Coder<K> keyCoder;
  private final WindowingStrategy<?, W> windowingStrategy;
  private final Coder<? extends BoundedWindow> windowCoder;
  private final RunnerApi.Trigger triggerProto;
  private final SystemReduceFn<K, V, Iterable<V>, Iterable<V>, W> reduceFn;
  private final PipelineOptions options;

  private final WatermarkAggregator watermarkAggregator;
  private Instant lastForwardedWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
  private Instant inputWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

  private @Nullable ProcessorContext<byte[], KStreamsPayload<?>> context;
  // Reports this GroupByKey instance as finished once it emits the terminal watermark, which it
  // only does after firing every pane it was holding.
  private final TerminationReporter terminationReporter;

  private @Nullable KeyValueStore<byte[], byte[]> stateStore;
  private @Nullable KeyValueStore<byte[], byte[]> holdsIndexStore;
  private @Nullable KeyValueStore<byte[], byte[]> timerStore;
  private @Nullable KeyValueStore<byte[], byte[]> timerIndexStore;

  WindowedGroupByKeyProcessor(
      String stateStoreName,
      String holdsIndexStoreName,
      String timerStoreName,
      String timerIndexStoreName,
      String transformId,
      Set<String> upstreamTransformIds,
      Coder<K> keyCoder,
      Coder<V> valueCoder,
      WindowingStrategy<?, W> windowingStrategy,
      PipelineOptions options,
      TerminationTracker terminationTracker) {
    this.terminationReporter = new TerminationReporter(terminationTracker, transformId);
    this.stateStoreName = stateStoreName;
    this.holdsIndexStoreName = holdsIndexStoreName;
    this.timerStoreName = timerStoreName;
    this.timerIndexStoreName = timerIndexStoreName;
    this.transformId = transformId;
    this.keyCoder = keyCoder;
    this.windowingStrategy = windowingStrategy;
    this.windowCoder = windowingStrategy.getWindowFn().windowCoder();
    this.triggerProto = TriggerTranslation.toProto(windowingStrategy.getTrigger());
    this.reduceFn = SystemReduceFn.buffering(valueCoder);
    this.options = options;
    this.watermarkAggregator = new WatermarkAggregator(upstreamTransformIds);
  }

  @Override
  public void init(ProcessorContext<byte[], KStreamsPayload<?>> context) {
    this.context = context;
    this.stateStore = context.getStateStore(stateStoreName);
    this.holdsIndexStore = context.getStateStore(holdsIndexStoreName);
    this.timerStore = context.getStateStore(timerStoreName);
    this.timerIndexStore = context.getStateStore(timerIndexStoreName);
    terminationReporter.init(context);
  }

  @Override
  public void close() {
    terminationReporter.close();
  }

  @Override
  public void process(Record<byte[], KStreamsPayload<?>> record) {
    KStreamsPayload<?> payload = record.value();
    if (payload == null) {
      LOG.warn(
          "GroupByKey {} dropping record with null payload (external write or tombstone)",
          transformId);
      return;
    }
    if (payload.isData()) {
      processData(record, payload);
      return;
    }
    watermarkAggregator.observe(payload.asWatermark());
    Instant advanced = watermarkAggregator.advance();
    if (advanced.isAfter(inputWatermark)) {
      inputWatermark = advanced;
      fireDueEventTimeTimers(record, advanced);
    }
    // Firing may have emitted panes and released their holds, so the output watermark is computed
    // after it.
    Instant output = outputWatermark();
    if (output.isAfter(lastForwardedWatermark)) {
      lastForwardedWatermark = output;
      forwardWatermark(record, output.getMillis());
    }
  }

  /**
   * The watermark to publish downstream: the input watermark, held back by the earliest watermark
   * hold any pending pane has taken.
   *
   * <p>{@link ReduceFnRunner} takes a hold for buffered elements that have not been emitted yet, at
   * the timestamp their pane will carry. Forwarding the raw input watermark would tell downstream
   * that nothing earlier is coming while those panes are still buffered, and the elements would
   * then arrive late against the watermark we had already published.
   */
  private Instant outputWatermark() {
    Instant minHold =
        KafkaStreamsStateInternals.minWatermarkHold(checkInitialized(holdsIndexStore));
    return minHold == null || inputWatermark.isBefore(minHold) ? inputWatermark : minHold;
  }

  private void processData(Record<byte[], KStreamsPayload<?>> record, KStreamsPayload<?> payload) {
    byte[] encodedKey = record.key();
    if (encodedKey == null) {
      throw new IllegalStateException("GroupByKey data record is missing its key");
    }
    @SuppressWarnings("unchecked")
    WindowedValue<KV<K, V>> element = (WindowedValue<KV<K, V>>) payload.getData();
    K key = decodeKey(encodedKey);
    WindowedValue<V> valueElement = element.withValue(element.getValue().getValue());
    runReduceFn(
        record, encodedKey, key, Collections.singletonList(valueElement), Collections.emptyList());
  }

  /**
   * Fires every event-time timer whose fire time is at or before the new input watermark.
   *
   * <p>The timers to fire are found by range-scanning the fire-time-ordered index over exactly the
   * window {@code (-inf, watermark]}, so the cost is proportional to the number of timers that are
   * actually due rather than to the number of keys that hold a timer.
   */
  private void fireDueEventTimeTimers(
      Record<byte[], KStreamsPayload<?>> record, Instant watermark) {
    KeyValueStore<byte[], byte[]> identityStore = checkInitialized(timerStore);
    KeyValueStore<byte[], byte[]> indexStore = checkInitialized(timerIndexStore);
    // Group the due timers by the Beam key they belong to; a key's timers fire together in one
    // ReduceFnRunner turn.
    Map<String, DueTimers> dueByKey = new LinkedHashMap<>();
    List<byte[]> firedIndexKeys = new ArrayList<>();
    try (KeyValueIterator<byte[], byte[]> it =
        indexStore.range(
            KafkaStreamsTimerInternals.dueEventTimeRangeStart(),
            KafkaStreamsTimerInternals.dueEventTimeRangeEnd(watermark.getMillis()))) {
      while (it.hasNext()) {
        org.apache.kafka.streams.KeyValue<byte[], byte[]> entry = it.next();
        TimerData timer = KafkaStreamsTimerInternals.decodeTimer(windowCoder, entry.value);
        byte[] encodedKey =
            KafkaStreamsTimerInternals.encodedKeyOf(
                KafkaStreamsTimerInternals.identityKeyOf(entry.key));
        dueByKey
            .computeIfAbsent(
                BaseEncoding.base16().encode(encodedKey), k -> new DueTimers(encodedKey))
            .timers
            .add(timer);
        firedIndexKeys.add(entry.key);
      }
    }
    // Clear the fired timers from both stores before replaying them, since onTimers may
    // legitimately
    // set new ones — including at the same identity.
    for (byte[] indexKey : firedIndexKeys) {
      indexStore.delete(indexKey);
      identityStore.delete(KafkaStreamsTimerInternals.identityKeyOf(indexKey));
    }
    for (DueTimers due : dueByKey.values()) {
      runReduceFn(
          record, due.encodedKey, decodeKey(due.encodedKey), Collections.emptyList(), due.timers);
    }
  }

  private void runReduceFn(
      Record<byte[], KStreamsPayload<?>> record,
      byte[] encodedKey,
      @NonNull K key,
      List<WindowedValue<V>> elements,
      List<TimerData> timers) {
    StateInternals stateInternals =
        new KafkaStreamsStateInternals<>(
            key, encodedKey, checkInitialized(stateStore), checkInitialized(holdsIndexStore));
    KafkaStreamsTimerInternals timerInternals =
        new KafkaStreamsTimerInternals(
            encodedKey,
            checkInitialized(timerStore),
            checkInitialized(timerIndexStore),
            windowCoder,
            inputWatermark,
            lastForwardedWatermark,
            Instant.now());
    ReduceFnRunner<K, V, Iterable<V>, W> runner =
        new ReduceFnRunner<>(
            key,
            windowingStrategy,
            ExecutableTriggerStateMachine.create(
                TriggerStateMachines.stateMachineForTrigger(triggerProto)),
            stateInternals,
            timerInternals,
            output -> forwardData(record, encodedKey, output),
            NullSideInputReader.empty(),
            reduceFn,
            options);
    try {
      runner.processElements(elements);
      runner.onTimers(timers);
      runner.persist();
    } catch (Exception e) {
      throw new RuntimeException("GroupByKey " + transformId + " failed to run windowing", e);
    }
  }

  private void forwardData(
      Record<byte[], KStreamsPayload<?>> trigger,
      byte[] encodedKey,
      WindowedValue<KV<K, Iterable<V>>> output) {
    ProcessorContext<byte[], KStreamsPayload<?>> ctx = checkInitialized(context);
    ctx.forward(
        new Record<byte[], KStreamsPayload<?>>(
            encodedKey, KStreamsPayload.data(output), trigger.timestamp()));
  }

  private void forwardWatermark(Record<byte[], KStreamsPayload<?>> trigger, long watermarkMillis) {
    ProcessorContext<byte[], KStreamsPayload<?>> ctx = checkInitialized(context);
    // Labelled as the only source a consumer will see; see ExecutableStageProcessor for why an
    // in-process edge reports a single source and a shuffle relabels.
    ctx.forward(
        new Record<byte[], KStreamsPayload<?>>(
            trigger.key(),
            KStreamsPayload.watermark(watermarkMillis, transformId, 0, 1),
            trigger.timestamp()));
    terminationReporter.watermarkEmitted(ctx, watermarkMillis);
  }

  private @NonNull K decodeKey(byte[] bytes) {
    try {
      K key = CoderUtils.decodeFromByteArray(keyCoder, bytes);
      if (key == null) {
        throw new IllegalStateException("GroupByKey key decoded to null");
      }
      return key;
    } catch (CoderException e) {
      throw new RuntimeException("Failed to decode GroupByKey key", e);
    }
  }

  private static <T> T checkInitialized(@Nullable T value) {
    if (value == null) {
      throw new IllegalStateException("WindowedGroupByKeyProcessor used before init()");
    }
    return value;
  }

  /** The event-time timers due for one Beam key, plus that key's encoded bytes. */
  private static final class DueTimers {
    final byte[] encodedKey;
    final List<TimerData> timers = new ArrayList<>();

    DueTimers(byte[] encodedKey) {
      this.encodedKey = encodedKey;
    }
  }
}
