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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static com.google.common.base.Preconditions.checkState;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.apache.beam.runners.core.ElementAndRestriction;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.OutputAndTimeBoundedSplittableProcessElementInvoker;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.SplittableParDo;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternalsFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Flink operator for executing splittable {@link DoFn DoFns}. Specifically, for executing
 * the {@code @ProcessElement} method of a splittable {@link DoFn}.
 */
public class SplittableDoFnOperator<
    InputT, FnOutputT, OutputT, RestrictionT, TrackerT extends RestrictionTracker<RestrictionT>>
    extends DoFnOperator<
    KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>, FnOutputT, OutputT> {

  public SplittableDoFnOperator(
      DoFn<KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>, FnOutputT> doFn,
      Coder<
          WindowedValue<
              KeyedWorkItem<String, ElementAndRestriction<InputT, RestrictionT>>>> inputCoder,
      TupleTag<FnOutputT> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      OutputManagerFactory<OutputT> outputManagerFactory,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      Collection<PCollectionView<?>> sideInputs,
      PipelineOptions options,
      Coder<?> keyCoder) {
    super(
        doFn,
        inputCoder,
        mainOutputTag,
        additionalOutputTags,
        outputManagerFactory,
        windowingStrategy,
        sideInputTagMapping,
        sideInputs,
        options,
        keyCoder);

  }

  @Override
  public void open() throws Exception {
    super.open();

    checkState(doFn instanceof SplittableParDo.ProcessFn);

    StateInternalsFactory<String> stateInternalsFactory = new StateInternalsFactory<String>() {
      @Override
      public StateInternals<String> stateInternalsForKey(String key) {
        //this will implicitly be keyed by the key of the incoming
        // element or by the key of a firing timer
        return (StateInternals<String>) stateInternals;
      }
    };
    TimerInternalsFactory<String> timerInternalsFactory = new TimerInternalsFactory<String>() {
      @Override
      public TimerInternals timerInternalsForKey(String key) {
        //this will implicitly be keyed like the StateInternalsFactory
        return timerInternals;
      }
    };

    ((SplittableParDo.ProcessFn) doFn).setStateInternalsFactory(stateInternalsFactory);
    ((SplittableParDo.ProcessFn) doFn).setTimerInternalsFactory(timerInternalsFactory);
    ((SplittableParDo.ProcessFn) doFn).setProcessElementInvoker(
        new OutputAndTimeBoundedSplittableProcessElementInvoker<>(
            doFn,
            serializedOptions.getPipelineOptions(),
            new OutputWindowedValue<FnOutputT>() {
              @Override
              public void outputWindowedValue(
                  FnOutputT output,
                  Instant timestamp,
                  Collection<? extends BoundedWindow> windows,
                  PaneInfo pane) {
                outputManager.output(
                    mainOutputTag,
                    WindowedValue.of(output, timestamp, windows, pane));
              }

              @Override
              public <AdditionalOutputT> void outputWindowedValue(
                  TupleTag<AdditionalOutputT> tag,
                  AdditionalOutputT output,
                  Instant timestamp,
                  Collection<? extends BoundedWindow> windows,
                  PaneInfo pane) {
                outputManager.output(tag, WindowedValue.of(output, timestamp, windows, pane));
              }
            },
            sideInputReader,
            Executors.newSingleThreadScheduledExecutor(Executors.defaultThreadFactory()),
            10000,
            Duration.standardSeconds(10)));
  }

  @Override
  public void fireTimer(InternalTimer<?, TimerInternals.TimerData> timer) {
    doFnRunner.processElement(WindowedValue.valueInGlobalWindow(
        KeyedWorkItems.<String, ElementAndRestriction<InputT, RestrictionT>>timersWorkItem(
            (String) stateInternals.getKey(),
            Collections.singletonList(timer.getNamespace()))));
  }
}
