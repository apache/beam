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

package com.google.cloud.dataflow.sdk.transforms.windowing;

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.ByteArrayCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * {@code CompositeTrigger} performs much of the book-keeping necessary for implementing a trigger
 * that has multiple sub-triggers. Specifically, it includes support for passing events to the
 * sub-triggers, and tracking the finished-states of each sub-trigger.
 *
 * TODO: Document the methods on this and SubTriggerExecutor to support writing new composite
 * triggers.
 *
 * <p> This functionality is experimental and likely to change.
 *
 * @param <W> {@link BoundedWindow} subclass used to represent the windows used by this
 *            {@code CompositeTrigger}
 */
public abstract class CompositeTrigger<W extends BoundedWindow> implements Trigger<W> {

  private static final long serialVersionUID = 0L;

  private static final CodedTupleTag<BitSet> SUBTRIGGERS_FINISHED_SET_TAG =
      CodedTupleTag.of("finished", new BitSetCoder());

  protected List<Trigger<W>> subTriggers;

  protected CompositeTrigger(List<Trigger<W>> subTriggers) {
    this.subTriggers = subTriggers;
  }

  /**
   * Helper that allows allows a {@code CompositeTrigger} to execute callbacks.
   */
  protected class SubTriggerExecutor {

    private final BitSet isFinished;
    private final W window;
    private final TriggerContext<W> context;

    private SubTriggerExecutor(
        TriggerContext<W> context, W window, BitSet isFinished) {
      this.context = context;
      this.window = window;
      this.isFinished = isFinished;
    }

    private void flush() throws Exception {
      context.store(SUBTRIGGERS_FINISHED_SET_TAG, window, isFinished);
    }

    public boolean allFinished() {
      return isFinished.cardinality() == subTriggers.size();
    }

    public List<Integer> getUnfinishedTriggers() {
      ImmutableList.Builder<Integer> result = ImmutableList.builder();
      for (int i = isFinished.nextClearBit(0); i >= 0 && i < subTriggers.size();
          i = isFinished.nextClearBit(i + 1)) {
        result.add(i);
      }
      return result.build();
    }

    public int firstUnfinished() {
      return isFinished.nextClearBit(0);
    }

    public BitSet getFinishedSet() {
      return (BitSet) isFinished.clone();
    }

    private TriggerResult handleChildResult(
        TriggerContext<W> childContext, int index, TriggerResult result) throws Exception {
      if (result.isFinish()) {
        markFinishedInChild(childContext, index);
      }

      return result;
    }

    public TriggerResult onElement(
        TriggerContext<W> compositeContext, int index, OnElementEvent<W> e) throws Exception {
      if (isFinished.get(index)) {
        return TriggerResult.FINISH;
      }

      TriggerContext<W> childContext = compositeContext.forChild(index);
      Trigger<W> subTrigger = subTriggers.get(index);
      return handleChildResult(
          childContext, index,
          subTrigger.onElement(childContext, e));
    }

    public TriggerResult onTimer(
        TriggerContext<W> compositeContext, int index, OnTimerEvent<W> e) throws Exception {
      TriggerContext<W> childContext = compositeContext.forChild(index);
      return handleChildResult(
          childContext, index, subTriggers.get(index).onTimer(childContext, e));
    }

    public TriggerResult onMerge(
        TriggerContext<W> compositeContext, int index, OnMergeEvent<W> e)
        throws Exception {
      TriggerContext<W> childContext = compositeContext.forChild(index);
      return handleChildResult(
          childContext, index, subTriggers.get(index).onMerge(childContext, e));
    }

    public void clear(TriggerContext<W> compositeContext, int index, W window)
        throws Exception {
      subTriggers.get(index).clear(compositeContext.forChild(index), window);
    }

    /**
     * Mark the sub-trigger at {@code index} as never-started. If the sub-trigger wasn't finished,
     * clears any associated state.
     *
     * @param compositeContext the context that the parent trigger was executing in.
     * @param index the index of the sub-trigger to affect.
     * @param window the window that the trigger is operating in.
     */
    public void reset(TriggerContext<W> compositeContext, int index, W window) throws Exception {
      // If it wasn't finished, the trigger may have state associated with it. Clear that up.
      if (!isFinished.get(index)) {
        subTriggers.get(index).clear(compositeContext.forChild(index), window);
      }

      isFinished.clear(index);
      flush();
    }

    public boolean isFinished(int index) {
      return isFinished.get(index);
    }

    private void markFinishedInChild(TriggerContext<W> childContext, int index) throws Exception {
      isFinished.set(index);
      flush();
      subTriggers.get(index).clear(childContext, window);
    }

    public void markFinished(TriggerContext<W> compositeContext, int index) throws Exception {
      markFinishedInChild(compositeContext.forChild(index), index);
    }
  }

  /**
   * Return a {@link SubTriggerExecutor} for executing sub-triggers in the given context and window.
   *
   * <p>TODO: Consider having the composite trigger always create the sub-executor and pass it down
   * to the composite.
   *
   * @param c The context of the composite trigger
   * @param window the window
   */
  protected SubTriggerExecutor subExecutor(TriggerContext<W> c, W window) throws IOException {
    BitSet result = c.lookup(SUBTRIGGERS_FINISHED_SET_TAG, window);
    if (result == null) {
      result = new BitSet(subTriggers.size());
    }
    return new SubTriggerExecutor(c, window, result);
  }
  /**
   * Return a {@link SubTriggerExecutor} for executing sub-triggers in the given context and window.
   *
   * <p>The finished states of all of the sub-triggers will be OR-ed across all of the windows. This
   * applies the behavior that a trigger which has finished in any of the merged windows is finished
   * in the merged window.
   *
   * @param c The context of the composite trigger
   * @param e The on merge event that is being processed.o
   */
  protected SubTriggerExecutor subExecutor(TriggerContext<W> c, OnMergeEvent<W> e)
      throws Exception {
    BitSet result = new BitSet(subTriggers.size());
    Map<W, BitSet> lookup = c.lookup(SUBTRIGGERS_FINISHED_SET_TAG, e.oldWindows());
    for (BitSet stateInWindow : lookup.values()) {
      if (stateInWindow != null) {
        result.or(stateInWindow);
      }
    }

    SubTriggerExecutor subTriggerStates = new SubTriggerExecutor(c, e.newWindow(), result);

    // Preemptively flush this since we just constructed it from the sub-windows.
    subTriggerStates.flush();
    return subTriggerStates;
  }

  @Override
  public void clear(TriggerContext<W> c, W window) throws Exception {
    // Clear all triggers (even if they were already cleared).
    for (Trigger<W> subTrigger : subTriggers) {
      subTrigger.clear(c, window);
    }
    c.remove(SUBTRIGGERS_FINISHED_SET_TAG, window);
  }

  @Override
  public final TriggerResult onTimer(TriggerContext<W> c, OnTimerEvent<W> e) throws Exception {
    if (e.isForCurrentLayer()) {
      // TODO: Modify the composite trigger interface to enforce this.
      throw new UnsupportedOperationException("Composite triggers should not set timers.");
    }

    int childIndex = e.getChildIndex();
    SubTriggerExecutor subTriggerStates = subExecutor(c, e.window());
    if (subTriggerStates.isFinished(childIndex)) {
      // The child was already finished, so this timer doesn't do anything. There has been no change
      // which might cause the composite to fire or change its state, so we just continue.
      return TriggerResult.CONTINUE;
    }

    TriggerResult result = subTriggerStates.onTimer(c, childIndex, e.withoutOuterTrigger());
    return afterChildTimer(c, e.window(), childIndex, result);
  }

  /**
   * Called after a timer has been executed on a sub-trigger.
   *
   * @param c The context for the composite trigger.
   * @param window The window that the timer fired in.
   * @param childIdx The index of the child that received the timer.
   * @param result The result of the timer firing in the child.
   */
  public abstract TriggerResult afterChildTimer(
      TriggerContext<W> c, W window, int childIdx, TriggerResult result)
      throws Exception;

  @Override
  public boolean isCompatible(Trigger<?> other) {
    if (!getClass().equals(other.getClass())) {
      return false;
    }

    CompositeTrigger<?> that = (CompositeTrigger<?>) other;
    if (subTriggers.size() != that.subTriggers.size()) {
      return false;
    }

    for (int i = 0; i < subTriggers.size(); i++) {
      if (!subTriggers.get(i).isCompatible(that.subTriggers.get(i))) {
        return false;
      }
    }

    return true;
  }

  /**
   * Coder for the BitSet used to track child-trigger finished states.
   */
  protected static class BitSetCoder extends AtomicCoder<BitSet> {

    private static final long serialVersionUID = 1L;

    private transient Coder<byte[]> byteArrayCoder = ByteArrayCoder.of();

    @Override
    public void encode(BitSet value, OutputStream outStream, Context context)
        throws CoderException, IOException {
      byteArrayCoder.encode(value.toByteArray(), outStream, context);
    }

    @Override
    public BitSet decode(InputStream inStream, Context context)
        throws CoderException, IOException {
      return BitSet.valueOf(byteArrayCoder.decode(inStream, context));
    }

    @Deprecated
    @Override
    public boolean isDeterministic() {
      return byteArrayCoder.isDeterministic();
    }
  }
}
