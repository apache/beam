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

import static org.apache.beam.runners.core.TimerInternals.TimerData;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.GroupAlsoByWindowViaWindowSetNewDoFn;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.core.TimerInternalsFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.flink.streaming.api.operators.InternalTimer;

/**
 * Flink operator for executing window {@link DoFn DoFns}.
 */
public class WindowDoFnOperator<K, InputT, OutputT>
    extends DoFnOperator<KeyedWorkItem<K, InputT>, KV<K, OutputT>, WindowedValue<KV<K, OutputT>>> {

  private final SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> systemReduceFn;

  public WindowDoFnOperator(
      SystemReduceFn<K, InputT, ?, OutputT, BoundedWindow> systemReduceFn,
      Coder<WindowedValue<KeyedWorkItem<K, InputT>>> inputCoder,
      TupleTag<KV<K, OutputT>> mainOutputTag,
      List<TupleTag<?>> additionalOutputTags,
      OutputManagerFactory<WindowedValue<KV<K, OutputT>>> outputManagerFactory,
      WindowingStrategy<?, ?> windowingStrategy,
      Map<Integer, PCollectionView<?>> sideInputTagMapping,
      Collection<PCollectionView<?>> sideInputs,
      PipelineOptions options,
      Coder<K> keyCoder) {
    super(
        null,
        inputCoder,
        mainOutputTag,
        additionalOutputTags,
        outputManagerFactory,
        windowingStrategy,
        sideInputTagMapping,
        sideInputs,
        options,
        keyCoder);

    this.systemReduceFn = systemReduceFn;

  }

  @Override
  protected DoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> getDoFn() {
    StateInternalsFactory<K> stateInternalsFactory = new StateInternalsFactory<K>() {
      @Override
      public StateInternals<K> stateInternalsForKey(K key) {
        //this will implicitly be keyed by the key of the incoming
        // element or by the key of a firing timer
        return (StateInternals<K>) stateInternals;
      }
    };
    TimerInternalsFactory<K> timerInternalsFactory = new TimerInternalsFactory<K>() {
      @Override
      public TimerInternals timerInternalsForKey(K key) {
        //this will implicitly be keyed like the StateInternalsFactory
        return timerInternals;
      }
    };

    // we have to do the unchecked cast because GroupAlsoByWindowViaWindowSetDoFn.create
    // has the window type as generic parameter while WindowingStrategy is almost always
    // untyped.
    @SuppressWarnings("unchecked")
    DoFn<KeyedWorkItem<K, InputT>, KV<K, OutputT>> doFn =
        GroupAlsoByWindowViaWindowSetNewDoFn.create(
            windowingStrategy, stateInternalsFactory, timerInternalsFactory, sideInputReader,
                (SystemReduceFn) systemReduceFn, outputManager, mainOutputTag);
    return doFn;
  }

  @Override
  public void fireTimer(InternalTimer<?, TimerData> timer) {
    doFnRunner.processElement(WindowedValue.valueInGlobalWindow(
        KeyedWorkItems.<K, InputT>timersWorkItem(
            (K) stateInternals.getKey(),
            Collections.singletonList(timer.getNamespace()))));
  }

}
