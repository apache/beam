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
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * Base class for implementing composite triggers.
 *
 * @param <W> The type of windows the trigger operates in.
 */
public abstract class CompositeTrigger<W extends BoundedWindow> extends Trigger<W> {

  private static final CodedTupleTag<BitSet> SUBTRIGGERS_FINISHED_SET_TAG =
      CodedTupleTag.of("finished", new BitSetCoder());

  private List<Trigger<W>> subTriggers;

  public CompositeTrigger(List<Trigger<W>> subTriggers) {
    this.subTriggers = subTriggers;
  }

  /**
   * Encapsulates the sub-trigger states that have been looked up for this composite trigger and
   * allows invoking the various trigger methods on the sub-triggers.
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

    private TriggerResult handleResult(
        TriggerContext<W> childContext, int index, TriggerResult result) throws Exception {
      if (result.isFinish()) {
        markFinishedInChild(childContext, index);
      }

      return result;
    }

    public TriggerResult onElement(
        TriggerContext<W> compositeContext, int index, Object value, W window, WindowStatus status)
        throws Exception {
      if (isFinished.get(index)) {
        return TriggerResult.FINISH;
      }

      TriggerContext<W> childContext = compositeContext.forChild(index);
      Trigger<W> subTrigger = subTriggers.get(index);
      return handleResult(
          childContext, index, subTrigger.onElement(childContext, value, window, status));
    }

    public TriggerResult onTimer(
        TriggerContext<W> compositeContext, int index, TriggerId<W> triggerId) throws Exception {
      TriggerContext<W> childContext = compositeContext.forChild(index);
      return handleResult(
          childContext, index, subTriggers.get(index).onTimer(childContext, triggerId));
    }

    public TriggerResult onMerge(
        TriggerContext<W> compositeContext, int index, Iterable<W> oldWindows, W newWindow)
        throws Exception {
      TriggerContext<W> childContext = compositeContext.forChild(index);
      return handleResult(
          childContext, index, subTriggers.get(index).onMerge(childContext, oldWindows, newWindow));
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

    public void markFinished(TriggerContext<W> childContext, int index) throws Exception {
      markFinishedInChild(childContext.forChild(index), index);
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
   * @param windows the windows that are being merged
   * @param outputWindow the window that the results should be written to
   */
  protected SubTriggerExecutor subExecutor(
      TriggerContext<W> c, Iterable<W> windows, W outputWindow)
      throws Exception {
    BitSet result = new BitSet(subTriggers.size());
    Map<W, BitSet> lookup = c.lookup(SUBTRIGGERS_FINISHED_SET_TAG, windows);
    for (BitSet stateInWindow : lookup.values()) {
      if (stateInWindow != null) {
        result.or(stateInWindow);
      }
    }

    SubTriggerExecutor subTriggerStates = new SubTriggerExecutor(c, outputWindow, result);

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
  }

  @Override
  public final TriggerResult onTimer(TriggerContext<W> c, TriggerId<W> triggerId) throws Exception {
    if (!triggerId.isForChild()) {
      // TODO: Modify the composite trigger interface to enforce this.
      throw new UnsupportedOperationException("Composite triggers should not set timers.");
    }

    int childIndex = triggerId.getChildIndex();
    SubTriggerExecutor subTriggerStates = subExecutor(c, triggerId.getWindow());
    if (subTriggerStates.isFinished(childIndex)) {
      // The child was already finished, so this timer doesn't do anything. There has been no change
      // which might cause the composite to fire or change its state, so we just continue.
      return TriggerResult.CONTINUE;
    }

    TriggerResult result = subTriggerStates.onTimer(c, childIndex, triggerId.forChildTrigger());
    return afterChildTimer(c, triggerId.getWindow(), childIndex, result);
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

  /**
   * Coder for the BitSet used to track child-trigger finished states.
   */
  private static class BitSetCoder extends AtomicCoder<BitSet> {

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
