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
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnElementEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnMergeEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.OnTimerEvent;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerContext;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TriggerResult;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * Utility that executes some subtriggers and tracks (in keyed state) which of the sub-triggers
 * have already finished. Useful for implementing composite {@link Trigger Triggers} that need
 * to track state of their sub-triggers between elements.
 */
class SubTriggerExecutor<W extends BoundedWindow> {

  private List<Trigger<W>> subTriggers;
  private final BitSet isFinished;
  private final W window;
  private final TriggerContext<W> context;

  private static final CodedTupleTag<BitSet> SUBTRIGGERS_FINISHED_SET_TAG =
      CodedTupleTag.of("finished", new BitSetCoder());

  /**
   * Return a {code SubTriggerExecutor} for executing sub-triggers in the given context and
   * window.
   *
   * @param c The context of the composite trigger
   * @param window the window
   */
  public static <W extends BoundedWindow> SubTriggerExecutor<W> forWindow(
      List<Trigger<W>> subTriggers, TriggerContext<W> c, W window) throws IOException {
    BitSet bitset = c.lookup(SUBTRIGGERS_FINISHED_SET_TAG, window);
    if (bitset == null) {
      bitset = new BitSet(subTriggers.size());
    }
    return new SubTriggerExecutor<W>(subTriggers, c, window, bitset);
  }

  /**
   * Return a {code SubTriggerExecutor} for executing sub-triggers in the given context and
   * windows.
   *
   * <p>The finished states of all of the sub-triggers will be OR-ed across all of the windows.
   * This applies the behavior that a trigger that has finished in any of the merged windows is
   * finished in the merged window.
   *
   * @param c The context of the composite trigger
   * @param e The on merge event that is being processed.o
   */
  public static <W extends BoundedWindow> SubTriggerExecutor<W> forMerge(
      List<Trigger<W>> subTriggers, TriggerContext<W> c, OnMergeEvent<W> e)
      throws Exception {
    BitSet bitset = new BitSet(subTriggers.size());
    Map<W, BitSet> lookup = c.lookup(SUBTRIGGERS_FINISHED_SET_TAG, e.oldWindows());
    for (BitSet stateInWindow : lookup.values()) {
      if (stateInWindow != null) {
        bitset.or(stateInWindow);
      }
    }

    SubTriggerExecutor<W> subTrigger =
        new SubTriggerExecutor<W>(subTriggers, c, e.newWindow(), bitset);

    // Preemptively flush this since we just constructed it from the sub-windows.
    subTrigger.flush();
    return subTrigger;
  }

  private SubTriggerExecutor(
      List<Trigger<W>> subTriggers, TriggerContext<W> context, W window, BitSet isFinished) {
    this.subTriggers = subTriggers;
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

  public TriggerResult onElement(int index, OnElementEvent<W> e) throws Exception {
    if (isFinished.get(index)) {
      throw new IllegalStateException("Calling onElement on already finished sub-element " + index);
    }

    TriggerContext<W> childContext = context.forChild(index);
    Trigger<W> subTrigger = subTriggers.get(index);
    return handleChildResult(
        childContext, index,
        subTrigger.onElement(childContext, e));
  }

  public TriggerResult onTimer(int index, OnTimerEvent<W> e) throws Exception {
    if (isFinished.get(index)) {
      throw new IllegalStateException("Calling onTimer on already finished sub-element " + index);
    }

    TriggerContext<W> childContext = context.forChild(index);
    TriggerResult onTimer = subTriggers.get(index).onTimer(childContext, e.withoutOuterTrigger());
    return handleChildResult(childContext, index, onTimer);
  }

  public TriggerResult onMerge(int index, OnMergeEvent<W> e)
      throws Exception {
    if (isFinished.get(index)) {
      throw new IllegalStateException("Calling onMerge on already finished sub-element " + index);
    }

    TriggerContext<W> childContext = context.forChild(index);
    return handleChildResult(
        childContext, index, subTriggers.get(index).onMerge(childContext, e));
  }

  /**
   * Clears the state associated with the given subtrigger.
   */
  public void clearSubTrigger(int index) throws Exception {
    subTriggers.get(index).clear(context.forChild(index), window);
  }

  /**
   * Clears the sub-triggers and the finished bits for the sub-trigger executor in the window.
   */
  public void clear() throws Exception {
    for (int i = 0; i < subTriggers.size(); i++) {
      clearSubTrigger(i);
    }
    context.remove(SUBTRIGGERS_FINISHED_SET_TAG, window);
  }

  /**
   * Mark the sub-trigger at {@code index} as never-started. If the sub-trigger wasn't finished,
   * clears any associated state.
   *
   * @param index the index of the sub-trigger to affect.
   */
  public void reset(int index) throws Exception {
    // If it wasn't finished, the trigger may have state associated with it. Clear that up.
    if (!isFinished.get(index)) {
      clearSubTrigger(index);
    }

    // And mark it finished.
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

  public void markFinished(TriggerContext<W> context, int index) throws Exception {
    markFinishedInChild(context.forChild(index), index);
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

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(
          "SubTriggerExecutor.BitSetCoder requires its byteArrayCoder to be deterministic.",
          byteArrayCoder);
    }
  }
}
