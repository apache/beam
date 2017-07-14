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
package org.apache.beam.runners.jstorm.translation;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.jstorm.JStormPipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * JStorm {@link Executor} for stateful {@link DoFn}.
 * @param <OutputT>
 */
class StatefulDoFnExecutor<OutputT> extends DoFnExecutor<KV, OutputT> {
  public StatefulDoFnExecutor(
      String stepName, String description, JStormPipelineOptions pipelineOptions,
      DoFn<KV, OutputT> doFn, Coder<WindowedValue<KV>> inputCoder,
      WindowingStrategy<?, ?> windowingStrategy, TupleTag<KV> mainInputTag,
      Collection<PCollectionView<?>> sideInputs, Map<TupleTag, PCollectionView<?>>
          sideInputTagToView, TupleTag<OutputT> mainTupleTag, List<TupleTag<?>> sideOutputTags) {
    super(stepName, description, pipelineOptions, doFn, inputCoder, windowingStrategy,
        mainInputTag, sideInputs, sideInputTagToView, mainTupleTag, sideOutputTags);
  }

  @Override
  public <T> void process(TupleTag<T> tag, WindowedValue<T> elem) {
    if (mainInputTag.equals(tag)) {
      WindowedValue<KV> kvElem = (WindowedValue<KV>) elem;
      stepContext.setTimerInternals(new JStormTimerInternals(kvElem.getValue().getKey(), this,
          executorContext.getExecutorsBolt().timerService()));
      stepContext.setStateInternals(new JStormStateInternals<>(kvElem.getValue().getKey(),
          kvStoreManager, executorsBolt.timerService(), internalDoFnExecutorId));
      processMainInput(elem);
    } else {
      processSideInput(tag, elem);
    }
  }

  @Override
  public void onTimer(Object key, TimerInternals.TimerData timerData) {
    stepContext.setStateInternals(new JStormStateInternals<>(key,
        kvStoreManager, executorsBolt.timerService(), internalDoFnExecutorId));
    super.onTimer(key, timerData);
  }
}
