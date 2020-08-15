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
package org.apache.beam.runners.core;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.GroupingState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

/**
 * {@link ReduceFn} implementing the default reduction behaviors of {@link GroupByKey}.
 *
 * @param <K> The type of key being processed.
 * @param <InputT> The type of values associated with the key.
 * @param <OutputT> The output type that will be produced for each key.
 * @param <W> The type of windows this operates on.
 */
public abstract class SystemReduceFn<K, InputT, AccumT, OutputT, W extends BoundedWindow>
    extends ReduceFn<K, InputT, OutputT, W> {
  private static final String BUFFER_NAME = "buf";

  /**
   * Create a factory that produces {@link SystemReduceFn} instances that that buffer all of the
   * input values in persistent state and produces an {@code Iterable<T>}.
   */
  public static <K, T, W extends BoundedWindow>
      SystemReduceFn<K, T, Iterable<T>, Iterable<T>, W> buffering(final Coder<T> inputCoder) {
    final StateTag<BagState<T>> bufferTag =
        StateTags.makeSystemTagInternal(StateTags.bag(BUFFER_NAME, inputCoder));
    return new SystemReduceFn<K, T, Iterable<T>, Iterable<T>, W>(bufferTag) {
      @Override
      public void prefetchOnMerge(MergingStateAccessor<K, W> state) throws Exception {
        StateMerging.prefetchBags(state, bufferTag);
      }

      @Override
      public void onMerge(OnMergeContext c) throws Exception {
        StateMerging.mergeBags(c.state(), bufferTag);
      }
    };
  }

  /**
   * Create a factory that produces {@link SystemReduceFn} instances that combine all of the input
   * values using a {@link CombineFn}.
   */
  public static <K, InputT, AccumT, OutputT, W extends BoundedWindow>
      SystemReduceFn<K, InputT, AccumT, OutputT, W> combining(
          final Coder<K> keyCoder, final AppliedCombineFn<K, InputT, AccumT, OutputT> combineFn) {
    final StateTag<CombiningState<InputT, AccumT, OutputT>> bufferTag;
    if (combineFn.getFn() instanceof CombineFnWithContext) {
      bufferTag =
          StateTags.makeSystemTagInternal(
              StateTags.combiningValueWithContext(
                  BUFFER_NAME,
                  combineFn.getAccumulatorCoder(),
                  (CombineFnWithContext<InputT, AccumT, OutputT>) combineFn.getFn()));

    } else {
      bufferTag =
          StateTags.makeSystemTagInternal(
              StateTags.combiningValue(
                  BUFFER_NAME,
                  combineFn.getAccumulatorCoder(),
                  (CombineFn<InputT, AccumT, OutputT>) combineFn.getFn()));
    }
    return new SystemReduceFn<K, InputT, AccumT, OutputT, W>(bufferTag) {
      @Override
      public void prefetchOnMerge(MergingStateAccessor<K, W> state) throws Exception {
        StateMerging.prefetchCombiningValues(state, bufferTag);
      }

      @Override
      public void onMerge(OnMergeContext c) throws Exception {
        StateMerging.mergeCombiningValues(c.state(), bufferTag);
      }
    };
  }

  private StateTag<? extends GroupingState<InputT, OutputT>> bufferTag;

  public SystemReduceFn(StateTag<? extends GroupingState<InputT, OutputT>> bufferTag) {
    this.bufferTag = bufferTag;
  }

  @VisibleForTesting
  StateTag<? extends GroupingState<InputT, OutputT>> getBufferTag() {
    return bufferTag;
  }

  @Override
  public void processValue(ProcessValueContext c) throws Exception {
    c.state().access(bufferTag).add(c.value());
  }

  @Override
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT") // just prefetch calls to readLater
  public void prefetchOnTrigger(StateAccessor<K> state) {
    state.access(bufferTag).readLater();
  }

  @Override
  public void onTrigger(OnTriggerContext c) throws Exception {
    c.output(c.state().access(bufferTag).read());
  }

  @Override
  public void clearState(Context c) throws Exception {
    c.state().access(bufferTag).clear();
  }

  @Override
  public ReadableState<Boolean> isEmpty(StateAccessor<K> state) {
    return state.access(bufferTag).isEmpty();
  }
}
