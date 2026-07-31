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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.GroupAlsoByWindowViaWindowSetNewDoFn;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StatefulDoFnRunner;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternalsFactory;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.TwsTransformFactory;
import org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.util.WindowedValueMultiReceiver;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.streaming.ExpiredTimerInfo;
import org.apache.spark.sql.streaming.MapState;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StatefulProcessor;
import org.apache.spark.sql.streaming.TTLConfig;
import org.apache.spark.sql.streaming.TimeMode;
import org.apache.spark.sql.streaming.TimerValues;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A single Spark 4 {@code transformWithState} operator that can host any keyed Beam transform: a
 * stateful {@code ParDo} or the group-also-by-window that implements a windowed {@code GroupByKey}.
 * Which one is decided by {@link BeamStatefulProcessorConfig#mode()}.
 *
 * <p>Keys, inputs and outputs are all raw Beam coder bytes and every Spark encoder involved is
 * {@code Encoders.BINARY()} or {@code Encoders.STRING()}, so no Catalyst schema is ever derived for
 * a Beam type. The exact row layouts are documented on {@link TwsTransformFactory}.
 *
 * <h2>State layout</h2>
 *
 * <p>Two Spark {@code MapState}s are declared, both {@code String -> byte[]}:
 *
 * <ul>
 *   <li>{@code beamState} holds all Beam user and system state, addressed by {@code namespace +
 *       tag}, see {@link TwsStateInternals}.
 *   <li>{@code beamTimers} holds the encoded {@link TimerInternals.TimerData}, see {@link
 *       TwsTimerInternals}.
 * </ul>
 *
 * <p>Both are declared with {@code TTLConfig.NONE()}: state lifetime is governed by Beam's own
 * garbage collection timers, not by Spark's.
 *
 * <h2>Bundles</h2>
 *
 * <p>Spark invokes {@code handleInputRows} once per key per micro-batch, and the Beam {@code
 * DoFnRunner} has to be bound to that key's state, so a Beam bundle here is one key inside one
 * micro-batch rather than the whole micro-batch. {@code startBundle} and {@code finishBundle} are
 * therefore called around each invocation that has work to do, and skipped entirely for an
 * invocation with neither elements nor due timers. {@code setup} and {@code teardown} are called
 * once per Spark task, from {@link #init} and {@link #close}.
 *
 * <h2>Watermarks</h2>
 *
 * <p>{@code TimerValues.getCurrentWatermarkInMs()} is the <b>batch start</b> watermark, that is the
 * watermark Spark computed at the end of the previous micro-batch. An element therefore can never
 * be considered late with respect to its own micro-batch, and an end-of-window timer fires in the
 * micro-batch after the one whose data crossed the end of the window. That is the same one batch
 * delay every micro-batch runner has.
 *
 * <h2>Timers</h2>
 *
 * <p>Beam timers fire only in {@code handleExpiredTimer}, never in {@code handleInputRows}. A timer
 * set while processing elements is picked up by Spark's own timer scan for the same micro-batch if
 * it is already due, so nothing is delayed by that choice, and it removes any risk of firing a
 * timer twice. Only event time timers are supported, see {@link TwsTimerInternals}.
 *
 * <p>Spark and Beam disagree on the firing boundary by one millisecond, Spark expiring a wake-up at
 * {@code expiry <= watermark} and Beam requiring the watermark to be strictly past the timer. The
 * gap is bridged in {@link TwsTimerInternals#removeTimersReadyToFire(long)}, which is where the
 * reasoning lives; getting it wrong silently loses on-time panes rather than merely reordering
 * them.
 */
@SuppressWarnings({
  "rawtypes",
  "unchecked",
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BeamStatefulProcessor extends StatefulProcessor<byte[], byte[], byte[]> {

  /** Name of the Spark state variable holding all Beam state. */
  public static final String BEAM_STATE_NAME = "beamState";

  /** Name of the Spark state variable holding the encoded Beam timers. */
  public static final String BEAM_TIMER_STATE_NAME = "beamTimers";

  private final BeamStatefulProcessorConfig config;

  private transient MapState<String, byte[]> beamState;
  private transient MapState<String, byte[]> beamTimers;
  private transient PipelineOptions options;
  private transient @Nullable DoFn<?, ?> doFn;
  private transient @Nullable DoFnInvoker<?, ?> doFnInvoker;
  private transient Map<TupleTag<?>, Integer> outputIndexes;

  public BeamStatefulProcessor(BeamStatefulProcessorConfig config) {
    this.config = config;
  }

  /** Returns the configuration this processor was built with. */
  public BeamStatefulProcessorConfig getConfig() {
    return config;
  }

  @Override
  public void init(OutputMode outputMode, TimeMode timeMode) {
    if (!TimeMode.EventTime().equals(timeMode)) {
      throw new UnsupportedOperationException(
          "BeamStatefulProcessor requires TimeMode.EventTime() but was initialised with "
              + timeMode
              + ". Beam event time timers cannot be expressed in any other Spark time mode.");
    }
    beamState =
        getHandle()
            .getMapState(BEAM_STATE_NAME, Encoders.STRING(), Encoders.BINARY(), TTLConfig.NONE());
    beamTimers =
        getHandle()
            .getMapState(
                BEAM_TIMER_STATE_NAME, Encoders.STRING(), Encoders.BINARY(), TTLConfig.NONE());

    options = config.optionsSupplier().get();

    outputIndexes = new HashMap<>();
    List<TupleTag<?>> tags = config.outputTags();
    for (int i = 0; i < tags.size(); i++) {
      outputIndexes.put(tags.get(i), i);
    }

    if (config.mode() == BeamStatefulProcessorConfig.Mode.STATEFUL_PARDO) {
      DoFn<?, ?> cloned = SerializableUtils.clone(config.doFn());
      doFn = cloned;
      doFnInvoker = DoFnInvokers.invokerFor(cloned);
      DoFnInvokers.tryInvokeSetupFor(cloned, options);
    }
  }

  @Override
  public scala.collection.Iterator<byte[]> handleInputRows(
      byte[] key, scala.collection.Iterator<byte[]> rows, TimerValues timerValues) {
    WindowedValues.FullWindowedValueCoder<Object> valueCoder = config.inputValueCoder();
    List<WindowedValue<Object>> elements = new ArrayList<>();
    while (rows.hasNext()) {
      byte[] payload = TwsTransformFactory.inputPayload(rows.next());
      elements.add(decode(valueCoder, payload, "input element"));
    }
    return process(key, elements, timerValues, null);
  }

  @Override
  public scala.collection.Iterator<byte[]> handleExpiredTimer(
      byte[] key, TimerValues timerValues, ExpiredTimerInfo expiredTimerInfo) {
    return process(key, Collections.emptyList(), timerValues, expiredTimerInfo.getExpiryTimeInMs());
  }

  @Override
  public void close() {
    if (doFnInvoker != null) {
      doFnInvoker.invokeTeardown();
      doFnInvoker = null;
      doFn = null;
    }
  }

  /**
   * Runs one Beam bundle for a single key.
   *
   * @param encodedKey the Spark grouping key, the Beam key encoded with the key coder
   * @param elements the elements of this micro-batch for that key, empty in a timer callback
   * @param timerValues Spark's clock for this invocation
   * @param firedExpiryMs the expiry Spark is firing, or {@code null} when processing elements
   */
  private scala.collection.Iterator<byte[]> process(
      byte[] encodedKey,
      List<WindowedValue<Object>> elements,
      TimerValues timerValues,
      @Nullable Long firedExpiryMs) {

    Object key = decode(config.keyCoder(), encodedKey, "key");
    Coder<? extends BoundedWindow> windowCoder = config.windowCoder();
    WindowingStrategy<?, ?> windowingStrategy = config.windowingStrategy();

    TwsStateInternals<Object> stateInternals = TwsStateInternals.forKey(key, BytesKV.of(beamState));
    TwsTimerInternals timerInternals =
        TwsTimerInternals.create(
            BytesKV.of(beamTimers),
            TwsTimerInternals.WakeupRegistry.of(getHandle()),
            windowCoder,
            new Instant(timerValues.getCurrentWatermarkInMs()),
            new Instant(timerValues.getCurrentProcessingTimeInMs()),
            firedExpiryMs);

    // Timers only ever fire from handleExpiredTimer, see the class javadoc.
    List<TimerInternals.TimerData> dueTimers =
        firedExpiryMs == null
            ? Collections.emptyList()
            : timerInternals.removeTimersReadyToFire(firedExpiryMs);

    if (elements.isEmpty() && dueTimers.isEmpty()) {
      // Nothing to do, but timer bookkeeping still has to be reconciled with Spark.
      timerInternals.flush();
      return ScalaInterop.scalaIterator(Collections.<byte[]>emptyList());
    }

    StepContext stepContext =
        new StepContext() {
          @Override
          public StateInternals stateInternals() {
            return stateInternals;
          }

          @Override
          public TimerInternals timerInternals() {
            return timerInternals;
          }
        };

    List<byte[]> outputs = new ArrayList<>();
    WindowedValueMultiReceiver receiver = new EncodingReceiver(outputs);

    if (config.mode() == BeamStatefulProcessorConfig.Mode.GROUP_ALSO_BY_WINDOW) {
      runGroupAlsoByWindow(
          key, elements, dueTimers, stepContext, receiver, stateInternals, timerInternals);
    } else {
      runStatefulParDo(key, elements, dueTimers, stepContext, receiver);
    }

    timerInternals.flush();
    return ScalaInterop.scalaIterator(outputs);
  }

  private void runStatefulParDo(
      Object key,
      List<WindowedValue<Object>> elements,
      List<TimerInternals.TimerData> dueTimers,
      StepContext stepContext,
      WindowedValueMultiReceiver receiver) {

    Coder<KV<Object, Object>> inputCoder = config.kvInputCoder();

    DoFnRunner<KV<Object, Object>, Object> simpleRunner =
        DoFnRunners.simpleRunner(
            options,
            (DoFn<KV<Object, Object>, Object>) doFn,
            config.sideInputReader(),
            receiver,
            (TupleTag<Object>) config.mainOutputTag(),
            config.additionalOutputTags(),
            stepContext,
            inputCoder,
            config.outputCoders(),
            config.windowingStrategy(),
            config.doFnSchemaInformation(),
            config.sideInputMapping());

    DoFnRunner<KV<Object, Object>, Object> runner =
        DoFnRunners.defaultStatefulDoFnRunner(
            (DoFn<KV<Object, Object>, Object>) doFn,
            inputCoder,
            simpleRunner,
            stepContext,
            config.windowingStrategy(),
            new StatefulDoFnRunner.TimeInternalsCleanupTimer<>(
                stepContext.timerInternals(), config.windowingStrategy()),
            new StatefulDoFnRunner.StateInternalsStateCleaner<>(
                doFn, stepContext.stateInternals(), (Coder) config.windowCoder()));

    runner.startBundle();
    for (WindowedValue<Object> element : elements) {
      runner.processElement(element.withValue(KV.of(key, element.getValue())));
    }
    for (TimerInternals.TimerData timer : dueTimers) {
      runner.onTimer(
          timer.getTimerId(),
          timer.getTimerFamilyId(),
          key,
          windowOf(timer.getNamespace()),
          timer.getTimestamp(),
          timer.getOutputTimestamp(),
          timer.getDomain(),
          timer.causedByDrain());
    }
    runner.finishBundle();
  }

  private void runGroupAlsoByWindow(
      Object key,
      List<WindowedValue<Object>> elements,
      List<TimerInternals.TimerData> dueTimers,
      StepContext stepContext,
      WindowedValueMultiReceiver receiver,
      StateInternals stateInternals,
      TimerInternals timerInternals) {

    StateInternalsFactory<Object> stateFactory = ignored -> stateInternals;
    TimerInternalsFactory<Object> timerFactory = ignored -> timerInternals;

    DoFn<KeyedWorkItem<Object, Object>, KV<Object, Iterable<Object>>> gabwDoFn =
        (DoFn)
            GroupAlsoByWindowViaWindowSetNewDoFn.create(
                (WindowingStrategy) config.windowingStrategy(),
                stateFactory,
                timerFactory,
                config.sideInputReader(),
                (SystemReduceFn) SystemReduceFn.buffering(config.valueCoder()),
                receiver,
                (TupleTag) config.mainOutputTag());

    DoFnRunner<KeyedWorkItem<Object, Object>, KV<Object, Iterable<Object>>> runner =
        DoFnRunners.simpleRunner(
            options,
            gabwDoFn,
            config.sideInputReader(),
            receiver,
            (TupleTag) config.mainOutputTag(),
            config.additionalOutputTags(),
            stepContext,
            null, // KeyedWorkItem has no coder here, SimpleDoFnRunner allows a null input coder
            config.outputCoders(),
            config.windowingStrategy(),
            config.doFnSchemaInformation(),
            config.sideInputMapping());

    runner =
        DoFnRunners.lateDataDroppingRunner(
            runner, stepContext, (WindowingStrategy) config.windowingStrategy());

    KeyedWorkItem<Object, Object> workItem = KeyedWorkItems.workItem(key, dueTimers, elements);

    runner.startBundle();
    runner.processElement(WindowedValues.valueInGlobalWindow(workItem));
    runner.finishBundle();
  }

  private static BoundedWindow windowOf(StateNamespace namespace) {
    if (namespace instanceof StateNamespaces.WindowNamespace) {
      return ((StateNamespaces.WindowNamespace<?>) namespace).getWindow();
    }
    if (namespace instanceof StateNamespaces.WindowAndTriggerNamespace) {
      return ((StateNamespaces.WindowAndTriggerNamespace<?>) namespace).getWindow();
    }
    throw new IllegalStateException(
        "Cannot fire a Beam timer set in namespace "
            + namespace.stringKey()
            + ", it is not bound to a window.");
  }

  private static <T> T decode(Coder<T> coder, byte[] bytes, String what) {
    try {
      return CoderUtils.decodeFromByteArray(coder, bytes);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to decode a Beam " + what, e);
    }
  }

  /** Encodes every emitted element into the tagged output row layout and appends it. */
  private final class EncodingReceiver implements WindowedValueMultiReceiver {
    private final List<byte[]> outputs;

    private EncodingReceiver(List<byte[]> outputs) {
      this.outputs = outputs;
    }

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> value) {
      Integer index = outputIndexes.get(tag);
      if (index == null) {
        throw new IllegalStateException(
            "Step "
                + config.stepName()
                + " emitted to unknown output tag "
                + tag
                + ", known tags are "
                + config.outputTags());
      }
      WindowedValues.FullWindowedValueCoder<T> coder = config.outputCoderFor(tag);
      try {
        outputs.add(
            TwsTransformFactory.encodeOutputRow(index, CoderUtils.encodeToByteArray(coder, value)));
      } catch (Exception e) {
        throw new IllegalStateException("Failed to encode an output of tag " + tag, e);
      }
    }
  }
}
