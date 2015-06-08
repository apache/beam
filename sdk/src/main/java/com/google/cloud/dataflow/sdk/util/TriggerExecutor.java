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
import com.google.cloud.dataflow.sdk.coders.InstantCoder;
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
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.WindowStatus;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
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
  private AccumulationMode mode;
  private Duration allowedLateness;

  private final WindowingInternals<?, KV<K, OutputT>> windowingInternals;
  private final AbstractWindowSet<K, InputT, OutputT, W> windowSet;
  private final TimerManager timerManager;
  private final WindowingInternals.KeyedState keyedState;
  private final MergeContext mergeContext;
  private final Coder<TriggerId<W>> triggerIdCoder;
  private final WatermarkHold watermarkHold;

  TriggerExecutor(
      WindowFn<Object, W> windowFn,
      TimerManager timerManager,
      ExecutableTrigger<W> trigger,
      WindowingInternals.KeyedState keyedState,
      WindowingInternals<?, KV<K, OutputT>> windowingInternals,
      AbstractWindowSet<K, InputT, OutputT, W> windowSet,
      AccumulationMode mode,
      Duration allowedLateness) {
    this.windowFn = windowFn;
    this.trigger = trigger;
    this.keyedState = keyedState;
    this.windowingInternals = windowingInternals;
    this.windowSet = windowSet;
    this.timerManager = timerManager;
    this.mode = mode;
    this.mergeContext = new MergeContext();
    this.triggerIdCoder = new TriggerIdCoder<>(windowFn.windowCoder());
    this.watermarkHold = new WatermarkHold();
    this.allowedLateness = allowedLateness;
  }

  private boolean isRootFinished(BitSet bitSet) {
    return bitSet.get(0);
  }

  public CodedTupleTag<BitSet> finishedSetTag(W window) throws CoderException {
    return CodedTupleTag.of(
        CoderUtils.encodeToBase64(windowFn.windowCoder(), window) + "finished-set",
        BitSetCoder.of());
  }

  public CodedTupleTag<Instant> earliestElementTag(W window) throws CoderException {
    return CodedTupleTag.of(
        CoderUtils.encodeToBase64(windowFn.windowCoder(), window) + "earliest-element",
        InstantCoder.of());
  }

  public static <K, InputT, OutputT, W extends BoundedWindow>
  TriggerExecutor<K, InputT, OutputT, W> create(
      K key,
      WindowingStrategy<Object, W> windowingStrategy,
      TimerManager timerManager,
      AbstractWindowSet.Factory<K, InputT, OutputT, W> windowSetFactory,
      WindowingInternals<?, KV<K, OutputT>> windowingInternals)
          throws Exception {
    AbstractWindowSet<K, InputT, OutputT, W> windowSet = windowSetFactory.create(
        key, windowingStrategy.getWindowFn().windowCoder(),
        windowingInternals.keyedState(), windowingInternals);
    return new TriggerExecutor<K, InputT, OutputT, W>(
        windowingStrategy.getWindowFn(), timerManager, windowingStrategy.getTrigger(),
        windowingInternals.keyedState(), windowingInternals, windowSet,
        windowingStrategy.getMode(), windowingStrategy.getAllowedLateness());
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
    // Prepare the cache by loading keyed state for all the given windows.
    Set<CodedTupleTag<?>> tags = new HashSet<>();
    for (W window : windows) {
      tags.add(finishedSetTag(window));
      tags.add(earliestElementTag(window));
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

      WindowStatus status = windowSet.put(window, value.getValue());
      if (status != WindowStatus.EXISTING) {
        // Set the timer for final cleanup. We add an extra millisecond since
        // maxTimestamp will be the maximum timestamp in the window, and we
        // want the maximum timestamp of an element outside the window.
        Instant cleanupTime = window.maxTimestamp()
            .plus(allowedLateness)
            .plus(Duration.millis(1));
        setTimer(cleanupTimer(window), cleanupTime, TimeDomain.EVENT_TIME);
      }

      watermarkHold.updateHoldForElement(window, value.getTimestamp(),
          value.getTimestamp().isBefore(timerManager.currentWatermarkTime()));

      BitSet originalFinishedSet = (BitSet) finishedSet.clone();
      OnElementEvent<W> e =
          new OnElementEvent<W>(value.getValue(), value.getTimestamp(), window, status);

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

  public void onTimer(String timerTag) throws Exception {
    TriggerId<W> triggerId = CoderUtils.decodeFromBase64(triggerIdCoder, timerTag);
    W window = triggerId.window();
    BitSet finishedSet = lookupFinishedSet(window);

    if (triggerId.getTriggerIdx() == FINAL_CLEANUP_PSEUDO_ID) {
      // If there are pending elements in the pane, emit it:
      if (watermarkHold.holdingForElements(window)) {
        if (mergeIfAppropriate(window)) {
          emitWindow(window);
        }
      }

      // Perform final cleanup.
      windowSet.remove(window);
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

  public void persistWindowSet() throws Exception {
    windowSet.persist();
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

    watermarkHold.updateHoldForMerge(toBeMerged, resultWindow);

    // Commit the updated states
    handleResult(
        trigger, resultWindow, originalFinishedSet, finishedSet, result.getTriggerResult());

    // Before we finish, we can clean up the state associated with the trigger in the old windows
    for (W windowBeingMerged : toBeMerged) {
      if (!resultWindow.equals(windowBeingMerged)) {
        trigger.invokeClear(context(lookupFinishedSet(windowBeingMerged)), windowBeingMerged);
        keyedState.remove(finishedSetTag(windowBeingMerged));
        watermarkHold.clearHold(windowBeingMerged);
      }
    }
  }

  /**
   * Invoke merge if the windowFn supports it, and return a boolean indicating whether the window
   * still exists.
   */
  private boolean mergeIfAppropriate(W window) throws Exception {
    if (windowFn.isNonMerging()) {
      // These never merge so the window won't disappear.
      return true;
    } else {
      windowFn.mergeWindows(mergeContext);
      return window != null && windowSet.contains(window);
    }
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
      // Remove the window from management (assume it is "done")
      windowSet.remove(window);
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
    if (!watermarkHold.holdingForElements(window)) {
      // No elements to emit.
      return;
    }

    OutputT finalValue = windowSet.finalValue(window);

    // If there were any contents to output in the window, do so.
    if (finalValue != null) {
      // Emit the (current) final values for the window
      KV<K, OutputT> value = KV.of(windowSet.getKey(), finalValue);

      // Output the windowed value.
      windowingInternals.outputWindowedValue(
          value, watermarkHold.timestampToEmit(window), Arrays.asList(window));
    }

    watermarkHold.clearHold(window);
  }

  @VisibleForTesting void setTimer(TriggerId<W> triggerId, Instant timestamp, TimeDomain domain)
      throws CoderException {
    timerManager.setTimer(CoderUtils.encodeToBase64(triggerIdCoder, triggerId), timestamp, domain);
  }

  @VisibleForTesting void deleteTimer(
      TriggerId<W> triggerId, TimeDomain domain) throws CoderException {
    timerManager.deleteTimer(CoderUtils.encodeToBase64(triggerIdCoder, triggerId), domain);
  }

  /**
   * Manages the time to which the watermark is held, specifically, we hold it to earliest non-late
   * timestamp as elements arrive.
   *
   * <p>When windows merge, {@link #updateHoldForMerge} determines the earliest non-late element
   * across all those windows.
   */
  private class WatermarkHold {

    /**
     * Return true if there is an active hold for elements in the given window.
     */
    public boolean holdingForElements(W window) throws IOException {
      return keyedState.lookup(earliestElementTag(window)) != null;
    }

    /**
     * Determine the timestamp to emit the current values at.
     */
    public Instant timestampToEmit(W window) throws IOException {
      // Normally, output at the earliest non-late element in the pane.
      // If the pane is empty or all the elements were late, output at window.maxTimestamp().
      Instant earliest = keyedState.lookup(earliestElementTag(window));
      return earliest == null || earliest.isAfter(window.maxTimestamp())
          ? window.maxTimestamp() : earliest;
    }

    public void updateHoldForElement(
        W window, Instant timestamp, boolean wasLate) throws IOException {
      CodedTupleTag<Instant> earliestElementTag = earliestElementTag(window);
      Instant earliestElement = keyedState.lookup(earliestElementTag);

      if (earliestElement == null && wasLate) {
        // If the element was late, then we want to put a hold in at the maxTimestamp for the end
        // of the window plus the allowed lateness to ensure that we don't output something
        // that is dropably late.
        earliestElement = window.maxTimestamp().plus(allowedLateness);
      } else if (earliestElement == null
          || (!wasLate && timestamp.isBefore(earliestElement))) {
        earliestElement = timestamp;
      }
      windowingInternals.store(earliestElementTag, earliestElement, earliestElement);
    }

    public void updateHoldForMerge(Iterable<W> mergingWindows, W newWindow) throws IOException {
      Iterable<Instant> mergingEarliestElements = lookupKeyedState(
          mergingWindows, new Function<W, CodedTupleTag<Instant>>() {
        @Override
        public CodedTupleTag<Instant> apply(W window) {
          try {
            return earliestElementTag(window);
          } catch (CoderException e) {
            throw Throwables.propagate(e);
          }
        }
      }).values();

      // If any of the merging windows had a hold, we should too.
      // That hold should be at the earliest hold that any of the merging windows had in place.
      Instant result = newWindow.maxTimestamp().plus(allowedLateness);
      for (Instant earliestElement : mergingEarliestElements) {
        if (earliestElement != null && result.isAfter(earliestElement)) {
          result = earliestElement;
        }
      }

      windowingInternals.store(earliestElementTag(newWindow), result, result);
    }

    public void clearHold(W window) throws IOException {
      keyedState.remove(earliestElementTag(window));
    }
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

  private class MergeContext extends WindowFn<Object, W>.MergeContext {

    @SuppressWarnings("cast")
    public MergeContext() {
      ((WindowFn<Object, W>) windowFn).super();
    }

    @Override
    public Collection<W> windows() {
      return windowSet.windows();
    }

    @Override
    public void merge(Collection<W> toBeMerged, W mergeResult) throws Exception {
      windowSet.merge(toBeMerged, mergeResult);
      onMerge(toBeMerged, mergeResult);
    }
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
