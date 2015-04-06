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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.transforms.DoFn.KeyedState;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PartitioningWindowFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnElementEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnMergeEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnTimerEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TimeDomain;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerContext;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerId;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.WindowStatus;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.CodedTupleTagMap;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages the execution of a trigger.
 *
 * @param <K>
 * @param <VI>
 * @param <VO>
 * @param <W> The type of windows this operates on.
 */
public class TriggerExecutor<K, VI, VO, W extends BoundedWindow> {

  private static final CodedTupleTag<Integer> IS_ROOT_FINISHED =
      CodedTupleTag.of("finished-root", VarIntCoder.of());

  private static final Integer FINISHED = Integer.valueOf(1);

  private final Trigger<W> trigger;
  private final WindowingInternals<?, KV<K, VO>> windowingInternals;
  private final AbstractWindowSet<K, VI, VO, W> windowSet;
  private final WindowFn<Object, W> windowFn;
  private final TimerManager timerManager;
  private final KeyedState keyedState;
  private final MergeContext mergeContext;
  private final TriggerContextImpl triggerContext;
  private final Coder<TriggerId<W>> triggerIdCoder;
  private final boolean willNeverFinish;

  /**
   * Methods that the system must provide in order for us to implement triggers.
   */
  public interface TimerManager {

    /**
     * Writes out a timer to be fired when the watermark reaches the given
     * timestamp.  Timers are identified by their name, and can be moved
     * by calling {@code setTimer} again, or deleted with {@link #deleteTimer}.
     */
    void setTimer(String timer, Instant timestamp, Trigger.TimeDomain domain);

    /**
     * Deletes the given timer.
     */
    void deleteTimer(String timer, Trigger.TimeDomain domain);

    /**
     * @return the current timestamp in the {@link Trigger.TimeDomain#PROCESSING_TIME}.
     */
    Instant currentProcessingTime();
  }

  public TriggerExecutor(
      WindowFn<Object, W> windowFn,
      TimerManager timerManager,
      Trigger<W> trigger,
      KeyedState keyedState,
      WindowingInternals<?, KV<K, VO>> windowingInternals,
      AbstractWindowSet<K, VI, VO, W> windowSet) {
    this.windowFn = windowFn;
    this.trigger = trigger;
    this.keyedState = keyedState;
    this.windowingInternals = windowingInternals;
    this.windowSet = windowSet;
    this.timerManager = timerManager;
    this.mergeContext = new MergeContext();
    this.triggerContext = new TriggerContextImpl();
    this.triggerIdCoder = new TriggerIdCoder<>(windowFn.windowCoder());

    this.willNeverFinish = trigger.willNeverFinish();
  }

  /**
   * Determine if the root trigger is finished in the given window.
   */
  @VisibleForTesting boolean isRootFinished(W window) throws IOException {
    return !willNeverFinish
        && FINISHED.equals(triggerContext.lookup(IS_ROOT_FINISHED, window));
  }

  /**
   * The root is finished in a merged window if it was finished in any of the windows being merged.
   */
  private boolean isRootFinished(Iterable<W> windows) throws IOException {
    if (willNeverFinish) {
      return false;
    }

    for (Integer isFinished : triggerContext.lookup(IS_ROOT_FINISHED, windows).values()) {
      if (FINISHED.equals(isFinished)) {
        return true;
      }
    }
    return false;
  }

  /**
   * The root is finished in a merged window if it was finished in any of the windows being merged.
   */
  private Map<W, Boolean> isRootFinishedInEachWindow(Iterable<W> windows) throws IOException {
    if (willNeverFinish) {
      return FluentIterable.from(windows).toMap(Functions.constant(false));
    }

    return Maps.transformValues(
        triggerContext.lookup(IS_ROOT_FINISHED, windows),
        Functions.forPredicate(Predicates.equalTo(FINISHED)));
  }

  public void onElement(WindowedValue<VI> value) throws Exception {
    @SuppressWarnings("unchecked")
    Collection<W> windows = (Collection<W>) value.getWindows();

    for (Map.Entry<W, Boolean> entry : isRootFinishedInEachWindow(windows).entrySet()) {
      if (entry.getValue()) {
        // If the trigger was already finished in that window, don't bother passing the element down
        continue;
      }

      W window = entry.getKey();
      WindowStatus status = windowSet.put(window, value.getValue(), value.getTimestamp());

      handleResult(trigger, window,
          trigger.onElement(triggerContext,
              new OnElementEvent<W>(value.getValue(), value.getTimestamp(), window, status)));

      if (WindowStatus.NEW.equals(status)) {
        // Attempt to merge windows before continuing
        windowFn.mergeWindows(mergeContext);
      }
    }
  }

  public void onTimer(String timerTag) throws Exception {
    TriggerId<W> triggerId = CoderUtils.decodeFromBase64(triggerIdCoder, timerTag);
    W window = triggerId.window();

    // If we receive a timer for an already finished trigger tree, we can ignore it. Once the
    // trigger is finished, it has reached a terminal state, and the trigger shouldn't be allowed
    // to do anything.
    if (isRootFinished(window)) {
      return;
    }

    // Attempt to merge windows before continuing; that may remove the current window from
    // consideration.
    windowFn.mergeWindows(mergeContext);

    // The WindowSet used with PartitioningWindowFn doesn't support contains, but it will never
    // merge windows in a way that causes the timer to no longer be applicable. Otherwise, we
    // confirm that the window is still in the windowSet.
    if ((windowFn instanceof PartitioningWindowFn) || windowSet.contains(window)) {
      handleResult(trigger, window,
          trigger.onTimer(triggerContext, new OnTimerEvent<W>(triggerId)));
    }
  }

  private void onMerge(Collection<W> toBeMerged, W mergeResult) throws Exception {
    // If the root is finished in any of the windows to be merged, then it is finished in the result
    // Merging cannot "wake up" a trigger. This case should never happen, because once finished the
    // window should have been removed from the window set, so it shouldn't even be around as a
    // source for merges. If it is, we don't bother merging and mark things finished.
    boolean isFinished = isRootFinished(toBeMerged);

    if (!isFinished) {
      // If the root wasn't finished in any of the windows, then call the underlying merge and
      // handle the result appropriately.
      handleResult(trigger, mergeResult,
          trigger.onMerge(triggerContext, new OnMergeEvent<W>(toBeMerged, mergeResult)));
    } else {
      // Otherwise, act like we were just told to finish in the resulting window.
      handleResult(trigger, mergeResult, TriggerResult.FINISH);
    }

    // Before we finish, we can clean up the state associated with the trigger in the old windows
    for (W window : toBeMerged) {
      trigger.clear(triggerContext, window);
      if (!willNeverFinish) {
        triggerContext.remove(IS_ROOT_FINISHED, window);
      }
    }
  }

  private void handleResult(Trigger<W> trigger, W window, TriggerResult result) throws Exception {
    if (result.isFire()) {
      emitWindow(window);
    }

    if (result.isFire() || result.isFinish()) {
      // Remove the window from management (assume it is "done")
      windowSet.remove(window);
    }

    // If the trigger is finished, we can clear out its state as long as we keep the
    // IS_ROOT_FINISHED bit.
    if (result.isFinish()) {
      if (willNeverFinish) {
        throw new RuntimeException("Trigger that shouldn't finish finished: " + trigger);
      }
      triggerContext.store(IS_ROOT_FINISHED, window, FINISHED);
      trigger.clear(triggerContext, window);
    }
  }

  private void emitWindow(W window) throws Exception {
    TimestampedValue<VO> finalValue = windowSet.finalValue(window);

    // Emit the (current) final values for the window
    KV<K, VO> value = KV.of(windowSet.getKey(), finalValue.getValue());

    // Output the windowed value.
    windowingInternals.outputWindowedValue(value, finalValue.getTimestamp(), Arrays.asList(window));
  }

  @VisibleForTesting void setTimer(TriggerId<W> triggerId, Instant timestamp, TimeDomain domain)
      throws CoderException {
    timerManager.setTimer(CoderUtils.encodeToBase64(triggerIdCoder, triggerId), timestamp, domain);
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

    private List<Integer> subTriggers;

    private TriggerContextImpl() {
      this.subTriggers = ImmutableList.of();
    }

    private TriggerContextImpl(List<Integer> subTriggers) {
      this.subTriggers = subTriggers;
    }

    private TriggerId<W> triggerId(W window) {
      return new TriggerId<>(window, subTriggers);
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
      timerManager.deleteTimer(triggerIdTag(window), domain);
    }

    @Override
    public <T> void store(CodedTupleTag<T> tag, W window, T value) throws IOException {
      CodedTupleTag<T> codedTriggerIdTag = codedTriggerIdTag(tag, window);
      keyedState.lookup(codedTriggerIdTag);
      keyedState.store(codedTriggerIdTag, value);
    }

    @Override
    public <T> void remove(CodedTupleTag<T> tag, W window) throws IOException {
      CodedTupleTag<T> codedTriggerIdTag = codedTriggerIdTag(tag, window);
      keyedState.lookup(codedTriggerIdTag);
      keyedState.remove(codedTriggerIdTag);
    }

    @Override
    public <T> T lookup(CodedTupleTag<T> tag, W window) throws IOException {
      return keyedState.lookup(codedTriggerIdTag(tag, window));
    }

    @Override
    public <T> Map<W, T> lookup(CodedTupleTag<T> tag, Iterable<W> windows) throws IOException {
      List<CodedTupleTag<T>> tags = new ArrayList<>();
      for (W window : windows) {
        tags.add(codedTriggerIdTag(tag, window));
      }

      CodedTupleTagMap tagMap = keyedState.lookup(tags);

      Map<W, T> result = new LinkedHashMap<>();
      int i = 0;
      for (W window : windows) {
        result.put(window, tagMap.get(tags.get(i++)));
      }

      return result;
    }

    @Override
    public Instant currentProcessingTime() {
      return timerManager.currentProcessingTime();
    }

    @Override
    public TriggerContext<W> forChild(int childIndex) {
      return new TriggerContextImpl(
          ImmutableList.<Integer>builder().addAll(subTriggers).add(childIndex).build());
    }
  }

  /**
   * Coder for Trigger IDs.
   */
  public static class TriggerIdCoder<W extends BoundedWindow> extends StandardCoder<TriggerId<W>> {

    private static final long serialVersionUID = 1L;

    private transient Coder<Iterable<Integer>> pathCoder = IterableCoder.of(VarIntCoder.of());
    private final Coder<W> windowCoder;

    public TriggerIdCoder(Coder<W> windowCoder) {
      this.windowCoder = windowCoder;
    }

    @Override
    public void encode(TriggerId<W> triggerId, OutputStream outStream, Context context)
        throws CoderException, IOException {
      windowCoder.encode(triggerId.window(), outStream, context);
      pathCoder.encode(triggerId.getPath(), outStream, context);
    }

    @Override
    public TriggerId<W> decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      W window = windowCoder.decode(inStream, context);
      List<Integer> path = ImmutableList.copyOf(pathCoder.decode(inStream, context));
      return new TriggerId<>(window, path);
    }

    @Deprecated
    @Override
    public boolean isDeterministic() {
      return windowCoder.isDeterministic();
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
}
