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
package org.apache.beam.runners.dataflow.worker;

import java.util.List;
import java.util.Set;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.StepContext;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.dataflow.worker.util.ValueInEmptyWindows;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.joda.time.Instant;

/**
 * A customized {@link DoFnRunner} that handles side inputs for a {@link KeyedWorkItem} input {@link
 * GroupAlsoByWindowFn}.
 *
 * <p>Input elements are postponed if the corresponding side inputs are not ready. It requires input
 * elements' windows are pre-expended.
 *
 * <p>{@link KeyedWorkItem KeyedWorkItems} are always in empty windows.
 *
 * @param <K> key type
 * @param <InputT> input value element type
 * @param <OutputT> output element type
 * @param <W> window type
 */
public class StreamingKeyedWorkItemSideInputDoFnRunner<K, InputT, OutputT, W extends BoundedWindow>
    implements DoFnRunner<KeyedWorkItem<K, InputT>, OutputT> {
  private final DoFnRunner<KeyedWorkItem<K, InputT>, OutputT> simpleDoFnRunner;
  private final StreamingSideInputFetcher<InputT, W> sideInputFetcher;
  private final StateTag<ValueState<K>> keyAddr;
  private final StepContext stepContext;
  private final Set<K> storedKeys;

  public StreamingKeyedWorkItemSideInputDoFnRunner(
      DoFnRunner<KeyedWorkItem<K, InputT>, OutputT> simpleDoFnRunner,
      Coder<K> keyCoder,
      StreamingSideInputFetcher<InputT, W> sideInputFetcher,
      StepContext stepContext) {
    this.simpleDoFnRunner = simpleDoFnRunner;
    this.sideInputFetcher = sideInputFetcher;
    this.keyAddr = StateTags.makeSystemTagInternal(StateTags.value("key", keyCoder));
    this.stepContext = stepContext;
    this.storedKeys = Sets.newHashSet();
  }

  @Override
  public void startBundle() {
    simpleDoFnRunner.startBundle();

    // Find the set of ready windows.
    Set<W> readyWindows = sideInputFetcher.getReadyWindows();

    Iterable<BagState<WindowedValue<InputT>>> elementsBags =
        sideInputFetcher.prefetchElements(readyWindows);
    Iterable<BagState<TimerData>> timersBags = sideInputFetcher.prefetchTimers(readyWindows);

    K key = keyValue().read();
    Iterable<TimerData> timers = Iterables.concat(Iterables.transform(timersBags, BagState::read));

    Iterable<WindowedValue<InputT>> elements =
        Iterables.concat(Iterables.transform(elementsBags, BagState::read));

    if (!isEmpty(timers, elements)) {
      simpleDoFnRunner.processElement(
          new ValueInEmptyWindows<>(KeyedWorkItems.workItem(key, timers, elements)));
      for (BagState<WindowedValue<InputT>> bag : elementsBags) {
        bag.clear();
      }
      for (BagState<TimerData> bag : timersBags) {
        bag.clear();
      }
    }
    sideInputFetcher.releaseBlockedWindows(readyWindows);
  }

  @Override
  public void processElement(WindowedValue<KeyedWorkItem<K, InputT>> elem) {
    final K key = elem.getValue().key();
    if (!storedKeys.contains(key)) {
      keyValue().write(key);
      storedKeys.add(key);
    }

    List<WindowedValue<InputT>> readyInputs =
        Lists.newArrayList(
            Iterables.filter(
                elem.getValue().elementsIterable(),
                input -> !sideInputFetcher.storeIfBlocked(input)));

    List<TimerData> readyTimers =
        Lists.newArrayList(
            Iterables.filter(
                elem.getValue().timersIterable(),
                timer -> !sideInputFetcher.storeIfBlocked(timer)));

    if (!readyTimers.isEmpty() || !readyInputs.isEmpty()) {
      KeyedWorkItem<K, InputT> keyedWorkItem =
          KeyedWorkItems.workItem(elem.getValue().key(), readyTimers, readyInputs);
      simpleDoFnRunner.processElement(elem.withValue(keyedWorkItem));
    }
  }

  @Override
  public <KeyT> void onTimer(
      String timerId,
      String timerFamilyId,
      KeyT key,
      BoundedWindow window,
      Instant timestamp,
      Instant outputTimestamp,
      TimeDomain timeDomain) {
    throw new UnsupportedOperationException(
        "Attempt to deliver a timer to a DoFn, but timers are not supported in Dataflow.");
  }

  @Override
  public void finishBundle() {
    simpleDoFnRunner.finishBundle();
    sideInputFetcher.persist();
  }

  @Override
  public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {
    simpleDoFnRunner.onWindowExpiration(window, timestamp, key);
  }

  ValueState<K> keyValue() {
    return stepContext.stateInternals().state(StateNamespaces.global(), keyAddr);
  }

  private boolean isEmpty(Iterable<TimerData> timers, Iterable<WindowedValue<InputT>> elements) {
    return !timers.iterator().hasNext() && !elements.iterator().hasNext();
  }

  @Override
  public DoFn<KeyedWorkItem<K, InputT>, OutputT> getFn() {
    return simpleDoFnRunner.getFn();
  }
}
