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

import com.google.api.client.util.Lists;
import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.KeyedWorkItems;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.dataflow.worker.util.ValueInEmptyWindows;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ParDoFn;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Receiver;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.DoFnInfo;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
/* Similar to {@link SimpleParDoFn} but for splittable ProcessFns. */
public class StreamingKeyedWorkItemSideInputParDoFn<K, InputT, OutputT, W extends BoundedWindow>
    implements ParDoFn {
  private final Coder<InputT> inputCoder;
  private final SimpleParDoFnHelpers<K, KeyedWorkItem<K, InputT>, OutputT, W> helpers;
  protected @Nullable StreamingSideInputProcessor<InputT, W> sideInputProcessor;

  StreamingKeyedWorkItemSideInputParDoFn(
      PipelineOptions options,
      DoFnInstanceManager doFnInstanceManager,
      SideInputReader sideInputReader,
      TupleTag<OutputT> mainOutputTag,
      Map<TupleTag<?>, Integer> outputTupleTagsToReceiverIndices,
      DataflowExecutionContext.DataflowStepContext stepContext,
      DataflowOperationContext operationContext,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping,
      DoFnRunnerFactory runnerFactory,
      Coder<K> keyCoder,
      Coder<InputT> inputCoder) {
    helpers =
        new SimpleParDoFnHelpers<>(
            options,
            doFnInstanceManager,
            sideInputReader,
            mainOutputTag,
            outputTupleTagsToReceiverIndices,
            stepContext,
            operationContext,
            doFnSchemaInformation,
            sideInputMapping,
            runnerFactory,
            this::onStartKey);
    this.inputCoder = inputCoder;
  }

  @Override
  public void startBundle(Receiver... receivers) throws Exception {
    helpers.startBundle(receivers);
    if (helpers.hasStreamingSideInput) {
      // There is non-trivial setup that needs to be performed for watermark propagation
      // even on empty bundles.
      helpers.reallyStartBundle();
    }
  }

  protected void onStartKey(@Nullable K key) {
    if (helpers.hasStreamingSideInput) {
      sideInputProcessor =
          new StreamingSideInputProcessor<>(
              new StreamingSideInputFetcher<InputT, W>(
                  helpers.fnInfo.getSideInputViews(),
                  inputCoder,
                  (WindowingStrategy<?, W>) helpers.fnInfo.getWindowingStrategy(),
                  (StreamingModeExecutionContext.StreamingModeStepContext)
                      helpers.userStepContext));
    }

    if (sideInputProcessor != null) {
      boolean hasState = helpers.hasState();
      if (key != null) {
        sideInputProcessor.tryUnblockElementsAndTimers(
            (unblockedElements, unblockedTimers) -> {
              if (!Iterables.isEmpty(unblockedElements) || !Iterables.isEmpty(unblockedTimers)) {
                helpers.fnRunner.processElement(
                    new ValueInEmptyWindows<>(
                        KeyedWorkItems.workItem(key, unblockedTimers, unblockedElements)));
              }
              if (hasState) {
                List<W> windows =
                    (List<W>)
                        StreamSupport.stream(unblockedElements.spliterator(), false)
                            .flatMap(wv -> wv.getWindows().stream())
                            .collect(Collectors.toList());
                // These elements are now processed. Register cleanup timers for all the unblocked
                // windows.
                helpers.registerStateCleanup(
                    (WindowingStrategy<?, W>) getDoFnInfo().getWindowingStrategy(), windows);
              }
            });
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void processElement(Object untypedElem) throws Exception {
    WindowedValue<KeyedWorkItem<K, InputT>> typedElem =
        (WindowedValue<KeyedWorkItem<K, InputT>>) untypedElem;
    helpers.processElement(typedElem.getValue().key(), typedElem, this::onProcessWindowedValue);
  }

  @Override
  public void processTimers() throws Exception {
    // Note: We need to get windowCoder to decode the timers.  If we haven't already deserialized
    // the fnInfo, we peek at a new instance to retrieve that. If this extra deserialization becomes
    // excessively costly, we could either (1) have the DoFnInstanceManager remember the associated
    // windowCoder (allowing us to get it without a DoFnInfo instance) or (2) check whether timers
    // exist without actually decoding them.
    Coder<BoundedWindow> windowCoder =
        (Coder<BoundedWindow>)
            (helpers.fnInfo != null ? helpers.fnInfo : helpers.doFnInstanceManager.peek())
                .getWindowingStrategy()
                .getWindowFn()
                .windowCoder();
    helpers.processTimers(
        SimpleParDoFnHelpers.TimerType.FAIL_USER,
        helpers.userStepContext,
        windowCoder,
        () -> sideInputProcessor);
    helpers.processTimers(
        SimpleParDoFnHelpers.TimerType.SYSTEM,
        helpers.stepContext,
        windowCoder,
        () -> sideInputProcessor);
  }

  @Override
  public void finishKey(Object key) throws Exception {
    helpers.finishKey((K) key, sideInputProcessor);
    this.sideInputProcessor = null;
  }

  @Override
  public void finishBundle() throws Exception {
    helpers.finishBundle(sideInputProcessor);
  }

  @Override
  public void abort() throws Exception {
    helpers.abort();
  }

  protected void onProcessWindowedValue(WindowedValue<KeyedWorkItem<K, InputT>> elem) {
    boolean hasState = helpers.hasState();
    Collection<W> windowsProcessed;
    if (sideInputProcessor != null) {
      windowsProcessed = hasState ? Lists.newArrayList() : Collections.emptyList();
      WindowedValue<KeyedWorkItem<K, InputT>> unblocked =
          sideInputProcessor.handleProcessKeyedWorkItem(elem);
      if (!Iterables.isEmpty(unblocked.getValue().elementsIterable())
          || !Iterables.isEmpty(unblocked.getValue().timersIterable())) {
        helpers.fnRunner.processElement(unblocked);
      }
      if (hasState) {
        windowsProcessed.addAll((Collection<W>) unblocked.getWindows());
      }
    } else {
      helpers.fnRunner.processElement(elem);
      windowsProcessed = (Collection<W>) elem.getWindows();
    }
    if (hasState) {
      helpers.registerStateCleanup(
          (WindowingStrategy<?, W>) getDoFnInfo().getWindowingStrategy(), windowsProcessed);
    }
  }

  /**
   * Returns the {@link DoFnInfo} currently being used by this {@link SimpleParDoFn}.
   *
   * <p>May be null if no element has been processed yet, or if the {@link SimpleParDoFn} has
   * finished.
   */
  @VisibleForTesting
  @Nullable
  DoFnInfo<?, ?> getDoFnInfo() {
    return helpers.fnInfo;
  }
}
