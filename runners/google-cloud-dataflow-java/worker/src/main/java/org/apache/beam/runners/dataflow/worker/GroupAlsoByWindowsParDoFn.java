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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.LateDataDroppingDoFnRunner;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A specialized {@link ParDoFn} for GroupAlsoByWindow related {@link GroupAlsoByWindowFn}'s. */
public class GroupAlsoByWindowsParDoFn<InputT, K, V, W extends BoundedWindow> implements ParDoFn {
  private final PipelineOptions options;

  private final SideInputReader sideInputReader;
  private final TupleTag<KV<K, Iterable<V>>> mainOutputTag;
  private final DataflowExecutionContext.DataflowStepContext stepContext;
  private final GroupAlsoByWindowFn<InputT, KV<K, Iterable<V>>> doFn;
  private final WindowingStrategy<?, W> windowingStrategy;
  private final Iterable<PCollectionView<?>> sideInputViews;
  private final Coder<InputT> inputCoder;

  // TODO: do not share this class, or refactor, in a way such that the guts need to do
  // different things based on whether it is streaming/batch, or what kind of GABW
  // function it is invoking
  private final boolean acceptsKeyedWorkItems;

  // Various DoFn helpers, null between bundles
  private @Nullable DoFnRunner<InputT, KV<K, Iterable<V>>> fnRunner;
  private @Nullable Receiver receiver;

  /**
   * Creates a {@link GroupAlsoByWindowsParDoFn} using basic information about the {@link
   * GroupAlsoByWindowFn} and the step being executed.
   */
  GroupAlsoByWindowsParDoFn(
      PipelineOptions options,
      GroupAlsoByWindowFn<InputT, KV<K, Iterable<V>>> doFn,
      WindowingStrategy<?, W> windowingStrategy,
      Iterable<PCollectionView<?>> sideInputViews,
      Coder<InputT> inputCoder,
      SideInputReader sideInputReader,
      TupleTag<KV<K, Iterable<V>>> mainOutputTag,
      DataflowExecutionContext.DataflowStepContext stepContext) {
    this.options = options;

    this.sideInputReader = sideInputReader;
    this.mainOutputTag = mainOutputTag;
    this.stepContext = stepContext;
    this.doFn = doFn;
    this.windowingStrategy = windowingStrategy;
    this.sideInputViews = sideInputViews;
    this.inputCoder = inputCoder;

    this.acceptsKeyedWorkItems = inputCoder instanceof WindmillKeyedWorkItem.FakeKeyedWorkItemCoder;
  }

  @Override
  public void startBundle(Receiver... receivers) throws Exception {
    checkState(fnRunner == null, "bundle already started (or not properly finished)");
    checkState(
        receivers.length == 1,
        "%s.startBundle() called with %s receivers, expected exactly 1. "
            + "This is a bug in the Dataflow service",
        getClass().getSimpleName(),
        receivers.length);
    receiver = receivers[0];
    fnRunner = createRunner();
    fnRunner.startBundle();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void processElement(Object untypedElem) throws Exception {
    checkState(fnRunner != null);

    // TODO: do not do this with an "if"
    if (!acceptsKeyedWorkItems) {
      // The straightforward case: we don't need to mess with timers
      fnRunner.processElement((WindowedValue<InputT>) untypedElem);
    } else {
      // Gather timers just for this step
      Coder<W> windowCoder = windowingStrategy.getWindowFn().windowCoder();
      TimerInternals.TimerData timer;
      List<TimerInternals.TimerData> firedTimers = new ArrayList<>();
      while ((timer = stepContext.getNextFiredTimer(windowCoder)) != null) {
        firedTimers.add(timer);
      }

      // Build a new input with just the timers we should process
      WindowedValue<KeyedWorkItem<K, V>> typedElem =
          (WindowedValue<KeyedWorkItem<K, V>>) untypedElem;
      KeyedWorkItem<K, V> keyedWorkItemWithAllTheTimers = typedElem.getValue();
      KeyedWorkItem<K, V> keyedWorkItemWithJustMyTimers =
          KeyedWorkItems.workItem(
              keyedWorkItemWithAllTheTimers.key(),
              firedTimers,
              keyedWorkItemWithAllTheTimers.elementsIterable());

      fnRunner.processElement(
          (WindowedValue<InputT>) typedElem.withValue(keyedWorkItemWithJustMyTimers));
    }
  }

  @Override
  public void processTimers() throws Exception {
    // TODO: call ReduceFnRunner.onTimers here, without all the intervening
    // layers, or else process timers one key at a time and have access to
    // it here to build a KeyedWorkItem
  }

  @Override
  public void finishBundle() throws Exception {
    checkState(fnRunner != null);
    fnRunner.finishBundle();
    fnRunner = null;
  }

  @Override
  public void abort() throws Exception {
    fnRunner = null;
  }

  /**
   * Composes and returns a {@link DoFnRunner} based on the parameters.
   *
   * <p>A {@code SimpleOldDoFnRunner} executes the {@link GroupAlsoByWindowFn}.
   *
   * <p>A {@link LateDataDroppingDoFnRunner} handles late data dropping for a {@link
   * StreamingGroupAlsoByWindowViaWindowSetFn}.
   *
   * <p>A {@link StreamingSideInputDoFnRunner} handles streaming side inputs.
   *
   * <p>A {@link StreamingKeyedWorkItemSideInputDoFnRunner} handles streaming side inputs for a
   * {@link StreamingGroupAlsoByWindowViaWindowSetFn}.
   */
  private DoFnRunner<InputT, KV<K, Iterable<V>>> createRunner() {
    OutputManager outputManager =
        new OutputManager() {
          @Override
          public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
            checkState(
                tag.equals(mainOutputTag),
                "Must only output to main output tag (%s), but was %s",
                tag,
                mainOutputTag);
            try {
              receiver.process(output);
            } catch (Throwable t) {
              throw new RuntimeException(t);
            }
          }
        };

    boolean hasStreamingSideInput =
        options.as(StreamingOptions.class).isStreaming() && !sideInputReader.isEmpty();

    DoFnRunner<InputT, KV<K, Iterable<V>>> basicRunner =
        new GroupAlsoByWindowFnRunner<>(
            options, doFn, sideInputReader, outputManager, mainOutputTag, stepContext);

    if (doFn instanceof StreamingGroupAlsoByWindowViaWindowSetFn) {
      DoFnRunner<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> streamingGABWRunner =
          (DoFnRunner<KeyedWorkItem<K, V>, KV<K, Iterable<V>>>) basicRunner;

      if (hasStreamingSideInput) {
        @SuppressWarnings("unchecked")
        WindmillKeyedWorkItem.FakeKeyedWorkItemCoder<K, V> keyedWorkItemCoder =
            (WindmillKeyedWorkItem.FakeKeyedWorkItemCoder<K, V>) inputCoder;
        StreamingSideInputFetcher<V, W> sideInputFetcher =
            new StreamingSideInputFetcher<>(
                sideInputViews,
                keyedWorkItemCoder.getElementCoder(),
                windowingStrategy,
                (StreamingModeExecutionContext.StreamingModeStepContext) stepContext);

        streamingGABWRunner =
            new StreamingKeyedWorkItemSideInputDoFnRunner<>(
                streamingGABWRunner,
                keyedWorkItemCoder.getKeyCoder(),
                sideInputFetcher,
                stepContext);
      }
      return (DoFnRunner<InputT, KV<K, Iterable<V>>>)
          DoFnRunners.<K, V, Iterable<V>, W>lateDataDroppingRunner(
              streamingGABWRunner, stepContext.timerInternals(), windowingStrategy);
    } else {
      if (hasStreamingSideInput) {
        return new StreamingSideInputDoFnRunner<>(
            basicRunner,
            new StreamingSideInputFetcher<>(
                sideInputViews,
                inputCoder,
                windowingStrategy,
                (StreamingModeExecutionContext.StreamingModeStepContext) stepContext));
      } else {
        return basicRunner;
      }
    }
  }
}
