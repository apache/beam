/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.ByteArrayCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.DefaultTrigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.MergeResult;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnElementEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnMergeEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnTimerEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerContext;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerId;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.ActiveWindowSet.MergeCallback;
import com.google.cloud.dataflow.sdk.util.TimerManager.TimeDomain;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.CodedTupleTagMap;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Manages the execution of a trigger.
 *
 * @param <K>
 * @param <InputT>
 * @param <OutputT>
 * @param <W> The type of windows this operates on.
 */
public class TriggerExecutor<K, InputT, OutputT, W extends BoundedWindow> {

  private static final int FINAL_CLEANUP_PSEUDO_ID = -1;

  private final WindowFn<Object, W> windowFn;
  private final ExecutableTrigger<W> trigger;
  private final AccumulationMode mode;
  private final Duration allowedLateness;

  private final WindowingInternals<?, KV<K, OutputT>> windowingInternals;
  private final TimerManager timerManager;
  private final WindowingInternals.KeyedState keyedState;
  private final Coder<TriggerId<W>> triggerIdCoder;
  private final ActiveWindowSet<W> activeWindows;
  private final OutputBuffer<K, InputT, OutputT, W> outputBuffer;

  private final WatermarkHold<W> watermarkHolder;

  private K key;


  TriggerExecutor(K key,
      WindowFn<Object, W> windowFn,
      TimerManager timerManager,
      ExecutableTrigger<W> trigger,
      WindowingInternals.KeyedState keyedState,
      WindowingInternals<?, KV<K, OutputT>> windowingInternals,
      AccumulationMode mode,
      Duration allowedLateness,
      ActiveWindowSet<W> activeWindows,
      OutputBuffer<K, InputT, OutputT, W> outputBuffer) {
    this.key = key;
    this.windowFn = windowFn;
    this.trigger = trigger;
    this.keyedState = keyedState;
    this.windowingInternals = windowingInternals;
    this.allowedLateness = allowedLateness;
    this.activeWindows = activeWindows;
    this.outputBuffer = outputBuffer;
    this.watermarkHolder = new WatermarkHold<W>(allowedLateness);
    this.timerManager = timerManager;
    this.mode = mode;
    this.triggerIdCoder = new TriggerIdCoder<>(windowFn.windowCoder());
  }

  private boolean isRootFinished(BitSet bitSet) {
    return bitSet.get(0);
  }

  private OutputBuffer.Context<K, W> bufferContext(final W window) {
    return new OutputBuffer.Context<K, W>() {
      @Override
      public K key() {
        return key;
      }

      @Override
      public W window() {
        return window;
      }

      @Override
      public Iterable<W> sourceWindows() {
        return activeWindows.sourceWindows(window);
      }

      private <T> CodedTupleTag<T> windowedTag(W window, CodedTupleTag<T> tag)
          throws CoderException {
        return CodedTupleTag.of(
            CoderUtils.encodeToBase64(windowFn.windowCoder(), window) + "/" + tag.getId(),
            tag.getCoder());
      }

      @Override
      public <T> void addToBuffer(W window, CodedTupleTag<T> buffer, T value) throws IOException {
        windowingInternals.writeToTagList(windowedTag(window, buffer), value);
      }

      @Override
      public <T> void addToBuffer(W window, CodedTupleTag<T> buffer, T value, Instant timestamp)
          throws IOException {
        windowingInternals.writeToTagList(windowedTag(window, buffer), value, timestamp);
      }

      @Override
      public void clearBuffers(CodedTupleTag<?> buffer, Iterable<W> windows) throws IOException {
        for (W window : windows) {
          windowingInternals.deleteTagList(windowedTag(window, buffer));
        }
      }

      @Override
      public <T> Iterable<T> readBuffers(CodedTupleTag<T> buffer, Iterable<W> windows)
          throws IOException {
        List<CodedTupleTag<T>> tags = new ArrayList<>();
        for (W window : windows) {
          tags.add(windowedTag(window, buffer));
        }
        return Iterables.concat(windowingInternals.readTagList(tags).values());
      }
    };
  }

  public CodedTupleTag<BitSet> finishedSetTag(W window) throws CoderException {
    return CodedTupleTag.of(
        CoderUtils.encodeToBase64(windowFn.windowCoder(), window) + "/finished-set",
        BitSetCoder.of());
  }

  public static <K, InputT, OutputT, W extends BoundedWindow>
  TriggerExecutor<K, InputT, OutputT, W> create(
      K key,
      WindowingStrategy<Object, W> windowingStrategy,
      TimerManager timerManager,
      OutputBuffer<K, InputT, OutputT, W> outputBuffer,
      WindowingInternals<?, KV<K, OutputT>> windowingInternals)
          throws Exception {
    ActiveWindowSet<W> activeWindows = windowingStrategy.getWindowFn().isNonMerging()
        ? new NonMergingActiveWindowSet<W>()
        : new MergingActiveWindowSet<W>(
            windowingStrategy.getWindowFn(), windowingInternals.keyedState());
    return new TriggerExecutor<K, InputT, OutputT, W>(key,
        windowingStrategy.getWindowFn(), timerManager, windowingStrategy.getTrigger(),
        windowingInternals.keyedState(), windowingInternals, windowingStrategy.getMode(),
        windowingStrategy.getAllowedLateness(), activeWindows, outputBuffer);
  }

  private TriggerContext<W> context(BitSet finishedSet) {
    return new TriggerContextImpl(finishedSet, trigger);
  }

  @VisibleForTesting BitSet lookupFinishedSet(W window) throws IOException {
    // TODO: If we know that no trigger in the tree will ever finish, we don't need to do the
    // lookup. Right now, we special case this for the DefaultTrigger.
    if (trigger.getSpec() instanceof DefaultTrigger) {
      return new BitSet(1);
    }

    BitSet finishedSet = keyedState.lookup(finishedSetTag(window));
    return finishedSet == null ? new BitSet(trigger.getFirstIndexAfterSubtree()) : finishedSet;
  }

  /**
   * Issue a load for all the keyed state tags that we know we need for the given windows.
   */
  private void warmUpCache(Iterable<W> windows) throws IOException {
    if ((trigger.getSpec() instanceof DefaultTrigger)) {
      return;
    }

    // Prepare the cache by loading keyed state for all the given windows.
    Set<CodedTupleTag<?>> tags = new HashSet<>();
    for (W window : windows) {
      tags.add(finishedSetTag(window));
    }
    keyedState.lookup(tags);
  }

  private TriggerId<W> cleanupTimer(W window) {
    return new TriggerId<W>(window, FINAL_CLEANUP_PSEUDO_ID);
  }

  public void onElement(WindowedValue<InputT> value) throws Exception {
    Instant minimumAllowedTimestamp = timerManager.currentWatermarkTime().minus(allowedLateness);
    if (minimumAllowedTimestamp.isAfter(value.getTimestamp())) {
      // TODO: Count the number of elements discarded because they are too late.
      return;
    }

    @SuppressWarnings("unchecked")
    Collection<W> windows = (Collection<W>) value.getWindows();

    warmUpCache(windows);

    for (W window : windows) {
      BitSet finishedSet = lookupFinishedSet(window);
      if (isRootFinished(finishedSet)) {
        // If the trigger was already finished in that window, don't bother passing the element down
        // TODO: Count the number of elements discarded because the window is closed.
        continue;
      }

      if (activeWindows.add(window)) {
        scheduleCleanup(window);
      }
      outputBuffer.addValue(bufferContext(window), value.getValue());
      watermarkHolder.addHold(bufferContext(window), value.getTimestamp(),
          timerManager.currentWatermarkTime().isAfter(value.getTimestamp()));

      BitSet originalFinishedSet = (BitSet) finishedSet.clone();
      OnElementEvent<W> e = new OnElementEvent<W>(value.getValue(), value.getTimestamp(), window);

      // Update the trigger state as appropriate for the arrival of the element.
      // Must come before merge so the state is updated (for merging).
      TriggerResult result = trigger.invokeElement(context(finishedSet), e);

      // Make sure we merge before firing, in case a larger window is produced
      boolean stillExists = true;
      if (result.isFire()) {
        stillExists = mergeIfAppropriate(window);
      }

      // Only invoke handleResult if the window is still active after merging. If not, the
      // merge should have taken care of any firing behaviors that needed to happen.
      if (stillExists) {
        handleResult(trigger, window, originalFinishedSet, finishedSet, result);
      }
    }
  }

  private void scheduleCleanup(W window) throws CoderException {
    // Set the timer for final cleanup. We add an extra millisecond since
    // maxTimestamp will be the maximum timestamp in the window, and we
    // want the maximum timestamp of an element outside the window.
    Instant cleanupTime = window.maxTimestamp()
        .plus(allowedLateness)
        .plus(Duration.millis(1));
    setTimer(cleanupTimer(window), cleanupTime, TimeDomain.EVENT_TIME);
  }

  public void onTimer(String timerTag) throws Exception {
    TriggerId<W> triggerId = CoderUtils.decodeFromBase64(triggerIdCoder, timerTag);
    W window = triggerId.window();
    BitSet finishedSet = lookupFinishedSet(window);

    if (triggerId.getTriggerIdx() == FINAL_CLEANUP_PSEUDO_ID) {
      if (mergeIfAppropriate(window)) {
        emitWindow(window);
        outputBuffer.clear(bufferContext(window));
      }

      // Perform final cleanup.
      activeWindows.remove(window);
      trigger.invokeClear(context(finishedSet), window);
      keyedState.remove(finishedSetTag(window));
      return;
    }

    // If we receive a timer for an already finished trigger tree, we can ignore it. Once the
    // trigger is finished, it has reached a terminal state, and the trigger shouldn't be allowed
    // to do anything.
    if (isRootFinished(finishedSet)) {
      // TODO: Add logging for this case since it means we failed to clean up the timer.
      return;
    }

    BitSet originalFinishedSet = (BitSet) finishedSet.clone();

    // Attempt to merge windows before continuing; that may remove the current window from
    // consideration.
    if (mergeIfAppropriate(window)) {
      TriggerResult result = trigger.invokeTimer(
          context(finishedSet), new OnTimerEvent<W>(triggerId));
      handleResult(trigger, window, originalFinishedSet, finishedSet, result);
    }
  }

  private OnMergeEvent<W> createMergeEvent(Collection<W> toBeMerged, W resultWindow)
      throws IOException {
    warmUpCache(
        toBeMerged.contains(resultWindow)
        ? toBeMerged
        : ImmutableSet.<W>builder().addAll(toBeMerged).add(resultWindow).build());
    ImmutableMap.Builder<W, BitSet> finishedSets = ImmutableMap.builder();
    for (W window : toBeMerged) {
      finishedSets.put(window, lookupFinishedSet(window));
    }

    return new OnMergeEvent<W>(toBeMerged, resultWindow, finishedSets.build());
  }

  public void persist() throws Exception {
    activeWindows.persist(keyedState);
    outputBuffer.flush(bufferContext(null));
    watermarkHolder.flush(bufferContext(null));
  }

  private void onMerge(Collection<W> toBeMerged, W resultWindow) throws Exception {
    OnMergeEvent<W> e = createMergeEvent(toBeMerged, resultWindow);
    BitSet originalFinishedSet = lookupFinishedSet(resultWindow);
    BitSet finishedSet = (BitSet) originalFinishedSet.clone();

    TriggerContext<W> context = context(finishedSet);
    MergeResult result = trigger.invokeMerge(context, e);
    if (MergeResult.ALREADY_FINISHED.equals(result)) {
      throw new IllegalStateException("Root trigger returned MergeResult.ALREADY_FINISHED.");
    }

    // Commit the updated states
    handleResult(
        trigger, resultWindow, originalFinishedSet, finishedSet, result.getTriggerResult());

    // Before we finish, we can clean up the state associated with the trigger in the old windows
    for (W windowBeingMerged : toBeMerged) {
      if (!resultWindow.equals(windowBeingMerged)) {
        trigger.invokeClear(context(lookupFinishedSet(windowBeingMerged)), windowBeingMerged);
        keyedState.remove(finishedSetTag(windowBeingMerged));
        deleteTimer(cleanupTimer(windowBeingMerged), TimeDomain.EVENT_TIME);
      }
    }
  }

  /**
   * Invoke merge if the windowFn supports it, and return a boolean indicating whether the window
   * still exists.
   */
  private boolean mergeIfAppropriate(W window) throws Exception {
    return activeWindows.mergeIfAppropriate(window, new MergeCallback<W>() {
      @Override
      public void onMerge(
          Collection<W> mergedWindows, W resultWindow, boolean isResultNew) throws Exception {
        TriggerExecutor.this.onMerge(mergedWindows, resultWindow);

        if (isResultNew) {
          scheduleCleanup(resultWindow);
        }
      }
    });
  }

  public void merge() throws Exception {
    mergeIfAppropriate(null);
  }

  private void handleResult(
      ExecutableTrigger<W> trigger, W window,
      BitSet originalFinishedSet, BitSet finishedSet, TriggerResult result) throws Exception {
    if (result.isFire()) {
      emitWindow(window);
    }

    if (result.isFinish()
        || (mode == AccumulationMode.DISCARDING_FIRED_PANES && result.isFire())) {
      outputBuffer.clear(bufferContext(window));

      // Remove the window from management (assume it is "done")
      activeWindows.remove(window);
    }

    // If the trigger is finished, we can clear out its state as long as we keep the
    // IS_ROOT_FINISHED bit.
    if (result.isFinish()) {
      trigger.invokeClear(context(finishedSet), window);
    }

    if (!finishedSet.equals(originalFinishedSet)) {
      keyedState.store(finishedSetTag(window), finishedSet);
    }
  }

  private void emitWindow(W window) throws Exception {
    Instant timestamp = watermarkHolder.extractAndRelease(bufferContext(window));
    OutputT finalValue = outputBuffer.extract(bufferContext(window));

    // If there were any contents to output in the window, do so.
    if (finalValue != null) {
      // Emit the (current) final values for the window
      KV<K, OutputT> value = KV.of(key, finalValue);

      // Output the windowed value.
      windowingInternals.outputWindowedValue(value, timestamp, Arrays.asList(window));
    }
  }

  @VisibleForTesting void setTimer(TriggerId<W> triggerId, Instant timestamp, TimeDomain domain)
      throws CoderException {
    timerManager.setTimer(CoderUtils.encodeToBase64(triggerIdCoder, triggerId), timestamp, domain);
  }

  @VisibleForTesting void deleteTimer(
      TriggerId<W> triggerId, TimeDomain domain) throws CoderException {
    timerManager.deleteTimer(CoderUtils.encodeToBase64(triggerIdCoder, triggerId), domain);
  }

  private <T> Map<W, T> lookupKeyedState(
      Iterable<W> windows, Function<W, CodedTupleTag<T>> tagFn) throws IOException {
    List<CodedTupleTag<T>> tags = new ArrayList<>();
    for (W window : windows) {
      tags.add(tagFn.apply(window));
    }

    CodedTupleTagMap tagMap = keyedState.lookup(tags);

    Map<W, T> result = new LinkedHashMap<>();
    int i = 0;
    for (W window : windows) {
      result.put(window, tagMap.get(tags.get(i++)));
    }

    return result;
  }

  private class TriggerContextImpl implements TriggerContext<W> {

    private final BitSet finishedSet;
    private final ExecutableTrigger<W> trigger;

    private TriggerContextImpl(BitSet finishedSet, ExecutableTrigger<W> trigger) {
      this.finishedSet = finishedSet;
      this.trigger = trigger;
    }

    private TriggerId<W> triggerId(W window) {
      return new TriggerId<>(window, trigger.getTriggerIndex());
    }

    private String triggerIdTag(W window) throws CoderException {
      return CoderUtils.encodeToBase64(triggerIdCoder, triggerId(window));
    }

    private <T> CodedTupleTag<T> codedTriggerIdTag(CodedTupleTag<T> tag, W window)
        throws CoderException {
      return CodedTupleTag.of(tag.getId() + "-" + triggerIdTag(window), tag.getCoder());
    }

    @Override
    public void setTimer(W window, Instant timestamp, TimeDomain domain) throws IOException {
      TriggerExecutor.this.setTimer(triggerId(window), timestamp, domain);
    }

    @Override
    public void deleteTimer(W window, TimeDomain domain) throws IOException {
      TriggerExecutor.this.deleteTimer(triggerId(window), domain);
    }

    @Override
    public <T> void store(CodedTupleTag<T> tag, W window, T value) throws IOException {
      CodedTupleTag<T> codedTriggerIdTag = codedTriggerIdTag(tag, window);
      keyedState.store(codedTriggerIdTag, value);
    }

    @Override
    public <T> void remove(CodedTupleTag<T> tag, W window) throws IOException {
      CodedTupleTag<T> codedTriggerIdTag = codedTriggerIdTag(tag, window);
      keyedState.remove(codedTriggerIdTag);
    }

    @Override
    public <T> T lookup(CodedTupleTag<T> tag, W window) throws IOException {
      return keyedState.lookup(codedTriggerIdTag(tag, window));
    }

    @Override
    public <T> Map<W, T> lookup(
        final CodedTupleTag<T> tag, final Iterable<W> windows) throws IOException {
      return lookupKeyedState(windows, new Function<W, CodedTupleTag<T>>() {
        @Override
        public CodedTupleTag<T> apply(W window) {
          try {
            return codedTriggerIdTag(tag, window);
          } catch (CoderException e) {
            throw Throwables.propagate(e);
          }
        }
      });
    }

    @Override
    public Instant currentProcessingTime() {
      return timerManager.currentProcessingTime();
    }

    @Override
    public TriggerContext<W> forTrigger(ExecutableTrigger<W> trigger) {
      return new TriggerContextImpl(finishedSet, trigger);
    }

    @Override
    public ExecutableTrigger<W> current() {
      return trigger;
    }

    @Override
    public boolean isCurrentTrigger(int triggerIndex) {
      return trigger.getTriggerIndex() == triggerIndex;
    }

    @Override
    public ExecutableTrigger<W> nextStepTowards(int someTriggerIndex) {
      return trigger.getSubTriggerContaining(someTriggerIndex);
    }

    @Override
    public Iterable<ExecutableTrigger<W>> subTriggers() {
      return trigger.subTriggers();
    }

    @Override
    public ExecutableTrigger<W> subTrigger(int subtriggerIndex) {
      return trigger.subTriggers().get(subtriggerIndex);
    }

    @Override
    public boolean isFinished() {
      return finishedSet.get(trigger.getTriggerIndex());
    }

    @Override
    public boolean areAllSubtriggersFinished() {
      return Iterables.isEmpty(unfinishedSubTriggers());
    }

    @Override
    public Iterable<ExecutableTrigger<W>> unfinishedSubTriggers() {
      return FluentIterable
          .from(trigger.subTriggers())
          .filter(new Predicate<ExecutableTrigger<W>>() {
            @Override
            public boolean apply(ExecutableTrigger<W> input) {
              return !finishedSet.get(input.getTriggerIndex());
            }
          });
    }

    @Override
    public ExecutableTrigger<W> firstUnfinishedSubTrigger() {
      for (ExecutableTrigger<W> subTrigger : trigger.subTriggers()) {
        if (!finishedSet.get(subTrigger.getTriggerIndex())) {
          return subTrigger;
        }
      }
      return null;
    }

    @Override
    public void resetTree(W window) throws Exception {
      finishedSet.clear(trigger.getTriggerIndex(), trigger.getFirstIndexAfterSubtree());
      trigger.invokeClear(this, window);
    }

    @Override
    public void setFinished(boolean finished) {
      finishedSet.set(trigger.getTriggerIndex(), finished);
    }
  }

  /**
   * Coder for Trigger IDs.
   */
  public static class TriggerIdCoder<W extends BoundedWindow> extends StandardCoder<TriggerId<W>> {

    private static final long serialVersionUID = 1L;

    private final Coder<W> windowCoder;
    private transient Coder<Integer> triggerIdxCoder = VarIntCoder.of();

    public TriggerIdCoder(Coder<W> windowCoder) {
      this.windowCoder = windowCoder;
    }

    @Override
    public void encode(TriggerId<W> triggerId, OutputStream outStream, Context context)
        throws CoderException, IOException {
      windowCoder.encode(triggerId.window(), outStream, context);
      triggerIdxCoder.encode(triggerId.getTriggerIdx(), outStream, context);
    }

    @Override
    public TriggerId<W> decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      W window = windowCoder.decode(inStream, context);
      Integer triggerIdx = triggerIdxCoder.decode(inStream, context);
      return new TriggerId<>(window, triggerIdx);
    }

    @Override
    public void verifyDeterministic() throws Coder.NonDeterministicException {
      verifyDeterministic("TriggerIdCoder requires a deterministic windowCoder", windowCoder);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(windowCoder);
    }
  }

  /**
   * Coder for the BitSet used to track child-trigger finished states.
   */
  protected static class BitSetCoder extends AtomicCoder<BitSet> {

    private static final BitSetCoder INSTANCE = new BitSetCoder();
    private static final long serialVersionUID = 1L;

    private transient ByteArrayCoder byteArrayCoder = ByteArrayCoder.of();

    private BitSetCoder() {}

    public static BitSetCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(BitSet value, OutputStream outStream, Context context)
        throws CoderException, IOException {
      byteArrayCoder.encodeAndOwn(value.toByteArray(), outStream, context);
    }

    @Override
    public BitSet decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      return BitSet.valueOf(byteArrayCoder.decode(inStream, context));
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(
          "SubTriggerExecutor.BitSetCoder requires its byteArrayCoder to be deterministic.",
          byteArrayCoder);
    }
  }
}
