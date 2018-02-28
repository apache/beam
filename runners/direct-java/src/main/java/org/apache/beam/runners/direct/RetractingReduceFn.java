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
package org.apache.beam.runners.direct;

import static org.apache.beam.runners.core.StateTags.bag;
import static org.apache.beam.runners.core.StateTags.makeSystemTagInternal;
import static org.apache.beam.sdk.util.WindowedValue.IS_RETRACTION;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.runners.core.MergingStateAccessor;
import org.apache.beam.runners.core.ReduceFn;
import org.apache.beam.runners.core.StateAccessor;
import org.apache.beam.runners.core.StateMerging;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

/**
 * Prototype implementation of retractions-aware GABW for DirectRunner.
 */
class RetractingReduceFn<K, InputT, W extends BoundedWindow>
    extends ReduceFn<K, InputT, Iterable<InputT>, W> {

  private static final String BUFFER_NAME = "buf";
  private static final String RETRACTIONS_BUFFER_NAME = BUFFER_NAME + "_retracted";
  private static final String NEW_ITEMS_BUFFER_NAME = BUFFER_NAME + "_new";

  private final StateTag<BagState<InputT>> bufferTag;
  private final StateTag<BagState<InputT>> retractedTag;
  private final StateTag<BagState<InputT>> newTag;

  public static <K, T, W extends BoundedWindow> RetractingReduceFn<K, T, W> of(
      Coder<T> coder) {

    return new RetractingReduceFn<>(
        makeSystemTagInternal(bag(BUFFER_NAME, coder)),
        makeSystemTagInternal(bag(RETRACTIONS_BUFFER_NAME, coder)),
        makeSystemTagInternal(bag(NEW_ITEMS_BUFFER_NAME, coder)));
  }

  private RetractingReduceFn(
      StateTag<BagState<InputT>> bufferTag,
      StateTag<BagState<InputT>> retractedTag,
      StateTag<BagState<InputT>> newTag) {
    this.bufferTag = bufferTag;
    this.retractedTag = retractedTag;
    this.newTag = newTag;
  }

  @Override
  public void prefetchOnMerge(MergingStateAccessor<K, W> state) {
    StateMerging.prefetchBags(state, bufferTag);
    StateMerging.prefetchBags(state, retractedTag);
    StateMerging.prefetchBags(state, newTag);
  }

  @Override
  public void onMerge(OnMergeContext c) {
    StateMerging.mergeBags(c.state(), bufferTag);
    StateMerging.mergeBags(c.state(), retractedTag);
    StateMerging.mergeBags(c.state(), newTag);
  }

  @Override
  public void processValue(ProcessValueContext c) {
    StateTag<BagState<InputT>> tag = c.isRetraction() ? retractedTag : newTag;
    c.state().access(tag).add(c.value());
  }

  @Override
  public void prefetchOnTrigger(StateAccessor<K> state) {
    state.access(bufferTag).readLater();
    state.access(retractedTag).readLater();
    state.access(newTag).readLater();
  }

  @Override
  public void onTrigger(OnTriggerContext c) {
    BagState<InputT> buffer = c.state().access(bufferTag);
    List<InputT> previousOutput = Lists.newArrayList(buffer.read());
    List<InputT> newElements = Lists.newArrayList(c.state().access(newTag).read());
    Set<InputT> retractedElements = Sets.newHashSet(c.state().access(retractedTag).read());

    List<InputT> newOutput =
        Stream
            .concat(previousOutput.stream(), newElements.stream())
            .filter(elem -> !retractedElements.contains(elem))
            .collect(Collectors.toList());

    if (previousOutput.size() > 0) {
      c.output(previousOutput, IS_RETRACTION);
    }

    clearState(c);
    newOutput.forEach(buffer::add);
    c.output(newOutput);
  }

  @Override
  public void clearState(Context c) {
    c.state().access(bufferTag).clear();
    c.state().access(retractedTag).clear();
    c.state().access(newTag).clear();
  }

  @Override
  public ReadableState<Boolean> isEmpty(StateAccessor<K> state) {
    return new ReadableState<Boolean>() {
      @Override
      public Boolean read() {
        return state.access(bufferTag).isEmpty().read()
               && state.access(retractedTag).isEmpty().read()
               && state.access(newTag).isEmpty().read();
      }

      @Override
      public ReadableState<Boolean> readLater() {
        return this;
      }
    };
  }
}

