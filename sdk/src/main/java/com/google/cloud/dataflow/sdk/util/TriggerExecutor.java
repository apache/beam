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
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.DefaultTrigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.MergeResult;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerId;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.ActiveWindowSet.MergeCallback;
import com.google.cloud.dataflow.sdk.util.TimerManager.TimeDomain;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode;
import com.google.cloud.dataflow.sdk.util.state.MergeableState;
import com.google.cloud.dataflow.sdk.util.state.State;
import com.google.cloud.dataflow.sdk.util.state.StateContents;
import com.google.cloud.dataflow.sdk.util.state.StateInternals;
import com.google.cloud.dataflow.sdk.util.state.StateNamespace;
import com.google.cloud.dataflow.sdk.util.state.StateNamespaces;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.StateTags;
import com.google.cloud.dataflow.sdk.util.state.ValueState;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Manages the execution of a trigger.
 *
 * @param <K>
 * @param <InputT>
 * @param <OutputT>
 * @param <W> The type of windows this operates on.
 */
public class TriggerExecutor<K, InputT, OutputT, W extends BoundedWindow> {

  private static final String BUFFER_NAME = "__buffer";

  @VisibleForTesting static final StateTag<ValueState<BitSet>> FINISHED_BITS_TAG =
      StateTags.value("finished_set", BitSetCoder.of());

  public static final String DROPPED_DUE_TO_CLOSED_WINDOW = "DroppedDueToClosedWindow";
  public static final String DROPPED_DUE_TO_LATENESS_COUNTER = "DroppedDueToLateness";

  private static final int FINAL_CLEANUP_PSEUDO_ID = -1;

  private final WindowFn<Object, W> windowFn;
  private final ExecutableTrigger<W> rootTrigger;
  private final AccumulationMode mode;
  private final Duration allowedLateness;

  private final WindowingInternals<?, KV<K, OutputT>> windowingInternals;
  private final TimerManager timerManager;
  private final Coder<TriggerId<W>> triggerIdCoder;
  private final ActiveWindowSet<W> activeWindows;
  private final StateInternals stateInternals;

  @VisibleForTesting final StateTag<? extends MergeableState<InputT, OutputT>> buffer;
  private final WatermarkHold<W> watermarkHolder;

  private final K key;

  private final Aggregator<Long, Long> droppedDueToClosedWindow;
  private final Aggregator<Long, Long> droppedDueToLateness;

  TriggerExecutor(K key,
      WindowFn<Object, W> windowFn,
      TimerManager timerManager,
      ExecutableTrigger<W> rootTrigger,
      WindowingInternals<?, KV<K, OutputT>> windowingInternals,
      AccumulationMode mode,
      Duration allowedLateness,
      ActiveWindowSet<W> activeWindows,
      StateTag<? extends MergeableState<InputT, OutputT>> outputBuffer,
      Aggregator<Long, Long> droppedDueToClosedWindow,
      Aggregator<Long, Long> droppedDueToLateness) {
    this.key = key;
    this.windowFn = windowFn;
    this.rootTrigger = rootTrigger;
    this.windowingInternals = windowingInternals;
    this.allowedLateness = allowedLateness;
    this.activeWindows = activeWindows;
    this.buffer = outputBuffer;
    this.droppedDueToClosedWindow = droppedDueToClosedWindow;
    this.droppedDueToLateness = droppedDueToLateness;
    this.watermarkHolder = new WatermarkHold<W>(allowedLateness);
    this.timerManager = timerManager;
    this.mode = mode;
    this.triggerIdCoder = new TriggerIdCoder<>(windowFn.windowCoder());
    this.stateInternals = windowingInternals.stateInternals();
  }

  private boolean isRootFinished(BitSet bitSet) {
    return bitSet.get(0);
  }

  public static <T> StateTag<? extends MergeableState<T, Iterable<T>>> listBuffer(Coder<T> coder) {
    return StateTags.<T>bag(BUFFER_NAME, coder);
  }

  public static <K, InputT, OutputT> StateTag<? extends MergeableState<InputT, OutputT>>
  combiningBuffer(K key, Coder<K> keyCoder, Coder<InputT> inputCoder,
      KeyedCombineFn<K, InputT, ?, OutputT> combineFn) {
    return StateTags.<InputT, OutputT>combiningValue(
        BUFFER_NAME, inputCoder, combineFn.forKey(key, keyCoder));
  }

  public static <K, InputT, OutputT, W extends BoundedWindow>
  TriggerExecutor<K, InputT, OutputT, W> create(
      K key,
      WindowingStrategy<Object, W> windowingStrategy,
      TimerManager timerManager,
      StateTag<? extends MergeableState<InputT, OutputT>> outputBuffer,
      WindowingInternals<?, KV<K, OutputT>> windowingInternals,
      Aggregator<Long, Long> droppedDueToClosedWindow,
      Aggregator<Long, Long> droppedDueToLateness) throws Exception {
    ActiveWindowSet<W> activeWindows = windowingStrategy.getWindowFn().isNonMerging()
        ? new NonMergingActiveWindowSet<W>()
        : new MergingActiveWindowSet<W>(
            windowingStrategy.getWindowFn(), windowingInternals.stateInternals());
    return new TriggerExecutor<K, InputT, OutputT, W>(key,
        windowingStrategy.getWindowFn(), timerManager, windowingStrategy.getTrigger(),
        windowingInternals, windowingStrategy.getMode(),
        windowingStrategy.getAllowedLateness(), activeWindows, outputBuffer,
        droppedDueToClosedWindow, droppedDueToLateness);
  }

  private Context context(W window) {
    return new Context(window, StateNamespaces.window(windowFn.windowCoder(), window));
  }

  @VisibleForTesting BitSet lookupFinishedSet(W window) {
    // TODO: If we know that no trigger in the tree will ever finish, we don't need to do the
    // lookup. Right now, we special case this for the DefaultTrigger.
    if (rootTrigger.getSpec() instanceof DefaultTrigger) {
      return new BitSet(1);
    }

    BitSet finishedSet =
        stateInternals.state(windowNamespace(window), FINISHED_BITS_TAG).get().read();
    return finishedSet == null ? new BitSet(rootTrigger.getFirstIndexAfterSubtree()) : finishedSet;
  }

  /**
   * Issue a load for all the keyed state tags that we know we need for the given windows.
   */
  private void warmUpCache(Iterable<W> windows) {
    if ((rootTrigger.getSpec() instanceof DefaultTrigger)) {
      return;
    }

    // Prepare the cache by loading keyed state for all the given windows.
    for (W window : windows) {
      stateInternals.state(windowNamespace(window), FINISHED_BITS_TAG).get();
    }
  }

  private TriggerId<W> cleanupTimer(W window) {
    return new TriggerId<W>(window, FINAL_CLEANUP_PSEUDO_ID);
  }

  private Trigger<W>.OnElementContext onElementContext(
      BitSet finishedSet, W window, WindowedValue<InputT> value) {
    TriggerContextImpl delegate = new TriggerContextImpl(finishedSet, rootTrigger, window);
    return new OnElementContextImpl(delegate, value.getValue(), value.getTimestamp());
  }

  public void onElement(WindowedValue<InputT> value) throws Exception {
    Instant minimumAllowedTimestamp = timerManager.currentWatermarkTime().minus(allowedLateness);
    if (minimumAllowedTimestamp.isAfter(value.getTimestamp())) {
      // We drop the element in all assigned windows if it is too late.
      droppedDueToLateness.addValue((long) value.getWindows().size());
      return;
    }

    @SuppressWarnings("unchecked")
    Collection<W> windows = (Collection<W>) value.getWindows();

    warmUpCache(windows);

    for (W window : windows) {
      BitSet finishedSet = lookupFinishedSet(window);
      if (isRootFinished(finishedSet)) {
        // If the window was finished (and closed) drop the element.
        droppedDueToClosedWindow.addValue(1L);
        continue;
      }

      if (activeWindows.add(window)) {
        scheduleCleanup(window);
      }

      BitSet originalFinishedSet = (BitSet) finishedSet.clone();
      Context context = context(window);

      context.access(buffer).add(value.getValue());
      watermarkHolder.addHold(context, value.getTimestamp(),
          timerManager.currentWatermarkTime().isAfter(value.getTimestamp()));

      // Update the trigger state as appropriate for the arrival of the element.
      // Must come before merge so the state is updated (for merging).
      TriggerResult result =
          rootTrigger.invokeElement(onElementContext(finishedSet, window, value));

      // Make sure we merge before firing, in case a larger window is produced
      boolean stillExists = true;
      if (result.isFire()) {
        stillExists = mergeIfAppropriate(window);
      }

      // Only invoke handleResult if the window is still active after merging. If not, the
      // merge should have taken care of any firing behaviors that needed to happen.
      if (stillExists) {
        handleResult(rootTrigger, window, originalFinishedSet, finishedSet, result);
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
    Context context = context(window);

    if (triggerId.getTriggerIdx() == FINAL_CLEANUP_PSEUDO_ID) {
      // TODO: Create appropriate Pane here.
      PaneInfo pane = PaneInfo.createPaneInternal();
      if (mergeIfAppropriate(window)) {
        emitWindow(context, pane);
        context.accessAcrossSources(buffer).clear();
      }

      // Perform final cleanup.
      activeWindows.remove(window);
      rootTrigger.invokeClear(new TriggerContextImpl(finishedSet, rootTrigger, window));
      stateInternals.state(windowNamespace(window), FINISHED_BITS_TAG).clear();
      return;
    }

    // If we receive a timer for an already finished trigger tree, we can ignore it. Once the
    // trigger is finished, it has reached a terminal state, and the trigger shouldn't be allowed
    // to do anything.
    if (isRootFinished(finishedSet)) {
      // TODO: Add logging for this case since it means we failed to clean up the timer.
      return;
    }

    // Attempt to merge windows before continuing; that may remove the current window from
    // consideration.
    if (mergeIfAppropriate(window)) {
      BitSet originalFinishedSet = (BitSet) finishedSet.clone();
      TriggerResult result =
          rootTrigger.invokeTimer(onTimerContext(finishedSet, window, triggerId));
      handleResult(rootTrigger, window, originalFinishedSet, finishedSet, result);
    }
  }

  private Trigger<W>.OnTimerContext onTimerContext(
      BitSet finishedSet, W window, TriggerId<W> triggerId) {
    TriggerContextImpl delegate = new TriggerContextImpl(finishedSet, rootTrigger, window);
    return new OnTimerContextImpl(delegate, triggerId);
  }

  private OnMergeContextImpl createMergeEvent(
      TriggerContextImpl context, Collection<W> toBeMerged, W resultWindow) {
    warmUpCache(
        toBeMerged.contains(resultWindow)
        ? toBeMerged
        : ImmutableSet.<W>builder().addAll(toBeMerged).add(resultWindow).build());
    ImmutableMap.Builder<W, BitSet> finishedSets = ImmutableMap.builder();
    for (W window : toBeMerged) {
      finishedSets.put(window, lookupFinishedSet(window));
    }

    return new OnMergeContextImpl(context, toBeMerged, finishedSets.build());
  }

  public void persist() {
    activeWindows.persist();
  }

  private void onMerge(Collection<W> toBeMerged, W resultWindow) throws Exception {
    BitSet originalFinishedSet = lookupFinishedSet(resultWindow);
    BitSet finishedSet = (BitSet) originalFinishedSet.clone();

    OnMergeContextImpl mergeContext = createMergeEvent(
        new TriggerContextImpl(finishedSet, rootTrigger, resultWindow), toBeMerged, resultWindow);
    MergeResult result = rootTrigger.invokeMerge(mergeContext);
    if (MergeResult.ALREADY_FINISHED.equals(result)) {
      throw new IllegalStateException("Root trigger returned MergeResult.ALREADY_FINISHED.");
    }

    // Commit the updated states
    handleResult(
        rootTrigger, resultWindow, originalFinishedSet, finishedSet, result.getTriggerResult());

    // Before we finish, we can clean up the state associated with the trigger in the old windows
    for (W windowBeingMerged : toBeMerged) {
      if (!resultWindow.equals(windowBeingMerged)) {
        rootTrigger.invokeClear(new TriggerContextImpl(
            lookupFinishedSet(windowBeingMerged), rootTrigger, windowBeingMerged));
        stateInternals.state(windowNamespace(windowBeingMerged), FINISHED_BITS_TAG).clear();
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
    Context context = context(window);
    if (result.isFire()) {
      // TODO: Obtain pain from ExecutableTrigger or TriggerResult.
      PaneInfo pane = PaneInfo.createPaneInternal();
      emitWindow(context, pane);
    }

    if (result.isFinish()
        || (mode == AccumulationMode.DISCARDING_FIRED_PANES && result.isFire())) {
      context.accessAcrossSources(buffer).clear();

      // Remove the window from management (assume it is "done")
      activeWindows.remove(window);
    }

    // If the trigger is finished, we can clear out its state as long as we keep the
    // IS_ROOT_FINISHED bit.
    if (result.isFinish()) {
      trigger.invokeClear(new TriggerContextImpl(finishedSet, rootTrigger, window));
    }

    if (!finishedSet.equals(originalFinishedSet)) {
      context.access(FINISHED_BITS_TAG).set(finishedSet);
    }
  }

  private void emitWindow(Context context, PaneInfo pane) throws Exception {
    StateContents<OutputT> finalValue = context.accessAcrossSources(buffer).get();
    Instant timestamp = watermarkHolder.extractAndRelease(context);

    // If there were any contents to output in the window, do so.
    if (timestamp != null) {
      // Emit the (current) final values for the window
      KV<K, OutputT> value = KV.of(key, finalValue.read());

      // Output the windowed value.
      windowingInternals.outputWindowedValue(
          value, timestamp, Arrays.asList(context.window()), pane);
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

  private StateNamespace windowNamespace(W window) {
    return StateNamespaces.window(windowFn.windowCoder(), window);
  }

  /**
   * Context for general trigger execution, without a specific trigger selected.
   */
  class Context {
    private final W window;
    private final StateNamespace namespace;

    private Context(W window, StateNamespace namespace) {
      this.window = window;
      this.namespace = namespace;
    }

    public W window() {
      return window;
    }

    public <StorageT extends State> StorageT access(StateTag<StorageT> address) {
      return stateInternals.state(namespace, address);
    }

    public <StorageT extends MergeableState<?, ?>> StorageT accessAcrossSources(
        StateTag<StorageT> address) {
      List<StateNamespace> sourceNamespaces = new ArrayList<>();
      for (W sourceWindow : activeWindows.sourceWindows(window)) {
        sourceNamespaces.add(windowNamespace(sourceWindow));
      }

      return stateInternals.mergedState(sourceNamespaces, namespace, address);
    }
  }

  private class TriggerContextImpl extends Trigger<W>.TriggerContext {

    private final BitSet finishedSet;
    private final ExecutableTrigger<W> trigger;
    private final W window;
    private final StateNamespace namespace;

    private TriggerContextImpl(
        BitSet finishedSet, ExecutableTrigger<W> trigger, W window) {
      trigger.getSpec().super();
      this.finishedSet = finishedSet;
      this.trigger = trigger;
      this.window = window;
      this.namespace = StateNamespaces.windowAndTrigger(
          windowFn.windowCoder(), window, trigger.getTriggerIndex());
    }

    private TriggerId<W> triggerId(W window) {
      return new TriggerId<>(window, trigger.getTriggerIndex());
    }

    @Override
    public void setTimer(Instant timestamp, TimeDomain domain) throws IOException {
      TriggerExecutor.this.setTimer(triggerId(window), timestamp, domain);
    }

    @Override
    public void deleteTimer(TimeDomain domain) throws IOException {
      TriggerExecutor.this.deleteTimer(triggerId(window), domain);
    }

    @Override
    public Instant currentProcessingTime() {
      return timerManager.currentProcessingTime();
    }

    @Override
    public TriggerContextImpl forTrigger(ExecutableTrigger<W> trigger) {
      return new TriggerContextImpl(finishedSet, trigger, window);
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
    public void resetTree() throws Exception {
      finishedSet.clear(trigger.getTriggerIndex(), trigger.getFirstIndexAfterSubtree());
      trigger.invokeClear(this);
    }

    @Override
    public void setFinished(boolean finished) {
      finishedSet.set(trigger.getTriggerIndex(), finished);
    }

    @Override
    public W window() {
      return window;
    }

    @Override
    public <StorageT extends State> StorageT access(StateTag<StorageT> address) {
      return stateInternals.state(namespace, address);
    }

    @Override
    public <StorageT extends MergeableState<?, ?>> StorageT accessAcrossMergedWindows(
        StateTag<StorageT> address) {
      List<StateNamespace> sourceNamespaces = new ArrayList<>();
      for (W sourceWindow : activeWindows.sourceWindows(window)) {
        sourceNamespaces.add(StateNamespaces.windowAndTrigger(
            windowFn.windowCoder(), sourceWindow, trigger.getTriggerIndex()));
      }

      return stateInternals.mergedState(sourceNamespaces, namespace, address);
    }
  }

  private class OnElementContextImpl extends Trigger<W>.OnElementContext {
    private final TriggerContextImpl delegate;

    public OnElementContextImpl(
        TriggerContextImpl delegate, Object value, Instant timestamp) {
      delegate.trigger.getSpec().super(value, timestamp);
      this.delegate = delegate;
    }

    @Override
    public Trigger<W>.OnElementContext forTrigger(ExecutableTrigger<W> trigger) {
      return new OnElementContextImpl(delegate.forTrigger(trigger), element(), eventTimestamp());
    }

    @Override
    public void setTimer(Instant timestamp, TimeDomain timeDomain) throws IOException {
      delegate.setTimer(timestamp, timeDomain);
    }

    @Override
    public void deleteTimer(TimeDomain timeDomain) throws IOException {
      delegate.deleteTimer(timeDomain);
    }

    @Override
    public Instant currentProcessingTime() {
      return delegate.currentProcessingTime();
    }

    @Override
    public Iterable<ExecutableTrigger<W>> subTriggers() {
      return delegate.subTriggers();
    }

    @Override
    public ExecutableTrigger<W> subTrigger(int subtriggerIndex) {
      return delegate.subTrigger(subtriggerIndex);
    }

    @Override
    public boolean isFinished() {
      return delegate.isFinished();
    }

    @Override
    public boolean areAllSubtriggersFinished() {
      return delegate.areAllSubtriggersFinished();
    }

    @Override
    public Iterable<ExecutableTrigger<W>> unfinishedSubTriggers() {
      return delegate.unfinishedSubTriggers();
    }

    @Override
    public ExecutableTrigger<W> firstUnfinishedSubTrigger() {
      return delegate.firstUnfinishedSubTrigger();
    }

    @Override
    public void resetTree() throws Exception {
      delegate.resetTree();
    }

    @Override
    public void setFinished(boolean finished) {
      delegate.setFinished(finished);
    }

    @Override
    public W window() {
      return delegate.window();
    }

    @Override
    public <StorageT extends State> StorageT access(StateTag<StorageT> address) {
      return delegate.access(address);
    }

    @Override
    public <StorageT extends MergeableState<?, ?>> StorageT accessAcrossMergedWindows(
        StateTag<StorageT> address) {
      return delegate.accessAcrossMergedWindows(address);
    }
  }

  private class OnMergeContextImpl extends Trigger<W>.OnMergeContext {

    private final TriggerContextImpl delegate;

    public OnMergeContextImpl(
        TriggerContextImpl delegate,
        Iterable<W> oldWindows, Map<W, BitSet> finishedSets) {
      delegate.trigger.getSpec().super(oldWindows, finishedSets);
      this.delegate = delegate;
    }

    @Override
    public Trigger<W>.OnMergeContext forTrigger(ExecutableTrigger<W> trigger) {
      return new OnMergeContextImpl(delegate.forTrigger(trigger), oldWindows(), finishedSets);
    }

    @Override
    public void setTimer(Instant timestamp, TimeDomain timeDomain) throws IOException {
      delegate.setTimer(timestamp, timeDomain);
    }

    @Override
    public void deleteTimer(TimeDomain timeDomain) throws IOException {
      delegate.deleteTimer(timeDomain);
    }

    @Override
    public Instant currentProcessingTime() {
      return delegate.currentProcessingTime();
    }

    @Override
    public Iterable<ExecutableTrigger<W>> subTriggers() {
      return delegate.subTriggers();
    }

    @Override
    public ExecutableTrigger<W> subTrigger(int subtriggerIndex) {
      return delegate.subTrigger(subtriggerIndex);
    }

    @Override
    public boolean isFinished() {
      return delegate.isFinished();
    }

    @Override
    public boolean areAllSubtriggersFinished() {
      return delegate.areAllSubtriggersFinished();
    }

    @Override
    public Iterable<ExecutableTrigger<W>> unfinishedSubTriggers() {
      return delegate.unfinishedSubTriggers();
    }

    @Override
    public ExecutableTrigger<W> firstUnfinishedSubTrigger() {
      return delegate.firstUnfinishedSubTrigger();
    }

    @Override
    public void resetTree() throws Exception {
      delegate.resetTree();
    }

    @Override
    public void setFinished(boolean finished) {
      delegate.setFinished(finished);
    }

    @Override
    public W window() {
      return delegate.window();
    }

    @Override
    public boolean finishedInAnyMergingWindow() {
      for (BitSet bitSet : finishedSets.values()) {
        if (bitSet.get(delegate.trigger.getTriggerIndex())) {
          return true;
        }
      }
      return false;
    }

    @Override
    public Iterable<W> getFinishedMergingWindows() {
      return Maps.filterValues(finishedSets, new Predicate<BitSet>() {
        @Override
        public boolean apply(BitSet input) {
          return input.get(delegate.trigger.getTriggerIndex());
        }
      }).keySet();
    }

    @Override
    public <StorageT extends State> StorageT access(StateTag<StorageT> address) {
      return delegate.access(address);
    }

    @Override
    public <StorageT extends MergeableState<?, ?>> StorageT accessAcrossMergedWindows(
        StateTag<StorageT> address) {
      return delegate.accessAcrossMergedWindows(address);
    }

    @Override
    public <StorageT extends MergeableState<?, ?>> StorageT accessAcrossMergingWindows(
        StateTag<StorageT> address) {
      List<StateNamespace> mergingNamespaces = new ArrayList<>();
      for (W oldWindow : oldWindows()) {
        mergingNamespaces.add(StateNamespaces.windowAndTrigger(
            windowFn.windowCoder(), oldWindow, delegate.trigger.getTriggerIndex()));
      }

      return stateInternals.mergedState(mergingNamespaces, delegate.namespace, address);
    }
  }

  private class OnTimerContextImpl extends Trigger<W>.OnTimerContext {

    private final TriggerContextImpl delegate;
    public OnTimerContextImpl(TriggerContextImpl delegate, TriggerId<W> triggerId) {
      delegate.trigger.getSpec().super(triggerId);
      this.delegate = delegate;
    }

    @Override
    public Trigger<W>.OnTimerContext forTrigger(ExecutableTrigger<W> trigger) {
      return new OnTimerContextImpl(delegate.forTrigger(trigger), destinationId);
    }

    @Override
    public void setTimer(Instant timestamp, TimeDomain timeDomain) throws IOException {
      delegate.setTimer(timestamp, timeDomain);
    }

    @Override
    public void deleteTimer(TimeDomain timeDomain) throws IOException {
      delegate.deleteTimer(timeDomain);
    }

    @Override
    public Instant currentProcessingTime() {
      return delegate.currentProcessingTime();
    }

    @Override
    public Iterable<ExecutableTrigger<W>> subTriggers() {
      return delegate.subTriggers();
    }

    @Override
    public ExecutableTrigger<W> subTrigger(int subtriggerIndex) {
      return delegate.subTrigger(subtriggerIndex);
    }

    @Override
    public boolean isDestination() {
      return delegate.trigger.getTriggerIndex() == destinationId.getTriggerIdx();
    }

    @Override
    public ExecutableTrigger<W> nextStepTowardsDestination() {
      return delegate.trigger.getSubTriggerContaining(destinationId.getTriggerIdx());
    }

    @Override
    public boolean isFinished() {
      return delegate.isFinished();
    }

    @Override
    public boolean areAllSubtriggersFinished() {
      return delegate.areAllSubtriggersFinished();
    }

    @Override
    public Iterable<ExecutableTrigger<W>> unfinishedSubTriggers() {
      return delegate.unfinishedSubTriggers();
    }

    @Override
    public ExecutableTrigger<W> firstUnfinishedSubTrigger() {
      return delegate.firstUnfinishedSubTrigger();
    }

    @Override
    public void resetTree() throws Exception {
      delegate.resetTree();
    }

    @Override
    public void setFinished(boolean finished) {
      delegate.setFinished(finished);
    }

    @Override
    public W window() {
      return delegate.window();
    }

    @Override
    public <StorageT extends State> StorageT access(StateTag<StorageT> address) {
      return delegate.access(address);
    }

    @Override
    public <StorageT extends MergeableState<?, ?>> StorageT accessAcrossMergedWindows(
        StateTag<StorageT> address) {
      return delegate.accessAcrossMergedWindows(address);
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
