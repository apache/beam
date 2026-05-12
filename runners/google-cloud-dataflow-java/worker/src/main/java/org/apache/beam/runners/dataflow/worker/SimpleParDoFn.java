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

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.runners.core.SideInputReader;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A base class providing simple set up, processing, and tear down for a wrapped {@link
 * GroupAlsoByWindowFn}.
 *
 * <p>Subclasses override just a method to provide a {@link DoFnInfo} for the wrapped {@link
 * GroupAlsoByWindowFn}.
 */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SimpleParDoFn<InputT, OutputT, W extends BoundedWindow> implements ParDoFn {
  private final SimpleParDoFnHelpers<InputT, OutputT, W> helpers;
  private @Nullable StreamingSideInputProcessor<InputT, W> sideInputProcessor;

  /** Creates a {@link SimpleParDoFn} using basic information about the step being executed. */
  SimpleParDoFn(
      PipelineOptions options,
      DoFnInstanceManager doFnInstanceManager,
      SideInputReader sideInputReader,
      TupleTag<OutputT> mainOutputTag,
      Map<TupleTag<?>, Integer> outputTupleTagsToReceiverIndices,
      DataflowExecutionContext.DataflowStepContext stepContext,
      DataflowOperationContext operationContext,
      DoFnSchemaInformation doFnSchemaInformation,
      Map<String, PCollectionView<?>> sideInputMapping,
      DoFnRunnerFactory runnerFactory) {
    this.helpers =
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
            runnerFactory);
  }

  @Override
  public void startBundle(Receiver... receivers) throws Exception {
    helpers.startBundle(receivers);
    if (helpers.hasStreamingSideInput) {
      // There is non-trivial setup that needs to be performed for watermark propagation
      // even on empty bundles.
      helpers.reallyStartBundle();
      onStartKey();
    }
  }

  protected void onStartKey() {
    // TODO(relax): This assumes single-key bundles, which will change! Refactor this to not make
    // this assumption.
    if (helpers.hasStreamingSideInput) {
      sideInputProcessor =
          new StreamingSideInputProcessor<>(
              new StreamingSideInputFetcher<InputT, W>(
                  helpers.fnInfo.getSideInputViews(),
                  helpers.fnInfo.getInputCoder(),
                  (WindowingStrategy<?, W>) helpers.fnInfo.getWindowingStrategy(),
                  (StreamingModeExecutionContext.StreamingModeStepContext)
                      helpers.userStepContext));

      boolean hasState = helpers.hasState();
      sideInputProcessor.tryUnblockElements(
          unblockedElements -> {
            for (WindowedValue<InputT> unblockedElement : unblockedElements) {
              helpers.fnRunner.processElement(unblockedElement);
              if (hasState) {
                // These elements are now processed. Register cleanup timers for all the unblocked
                // windows.
                helpers.registerStateCleanup(
                    (WindowingStrategy<?, W>) getDoFnInfo().getWindowingStrategy(),
                    (Collection<W>) unblockedElement.getWindows());
              }
            }
          });
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public void processElement(Object untypedElem) throws Exception {
    if (helpers.fnRunner == null) {
      // If we need to run reallyStartBundle in here, we need to make sure to switch the state
      // sampler into the start state.
      try (Closeable start = helpers.operationContext.enterStart()) {
        helpers.reallyStartBundle();
        onStartKey();
      }
    }
    helpers.outputsPerElementTracker.onProcessElement();

    WindowedValue<InputT> elem = (WindowedValue<InputT>) untypedElem;
    onProcessWindowedValue(elem);

    helpers.outputsPerElementTracker.onProcessElementSuccess();
  }

  protected void onProcessWindowedValue(WindowedValue<InputT> elem) {
    boolean hasState = helpers.hasState();

    Collection<W> windowsProcessed;
    if (sideInputProcessor != null) {
      windowsProcessed = hasState ? Lists.newArrayList() : Collections.emptyList();
      for (Iterator<? extends WindowedValue<InputT>> it =
              sideInputProcessor.handleProcessElement(elem);
          it.hasNext(); ) {
        WindowedValue<InputT> toProcess = it.next();
        helpers.fnRunner.processElement(toProcess);
        if (hasState) {
          windowsProcessed.addAll((Collection<W>) toProcess.getWindows());
          // If the element was blocked, don't register a cleanup timer. The timer will be
          // registered
          // when the window is unblocked ensuring that it is not processed until the element is.
        }
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
        SimpleParDoFnHelpers.TimerType.USER,
        helpers.userStepContext,
        windowCoder,
        this::onStartKey,
        () -> sideInputProcessor);
    helpers.processTimers(
        SimpleParDoFnHelpers.TimerType.SYSTEM,
        helpers.stepContext,
        windowCoder,
        this::onStartKey,
        () -> sideInputProcessor);
  }

  @Override
  public void finishBundle() throws Exception {
    helpers.finishBundle(sideInputProcessor);
    this.sideInputProcessor = null;
  }

  @Override
  public void abort() throws Exception {
    helpers.abort();
  }

  /**
   * Returns the {@link DoFnInfo} currently being used by this {@link SimpleParDoFn}.
   *
   * <p>May be null if no element has been processed yet, or if the {@link SimpleParDoFn} has
   * finished.
   */
  @VisibleForTesting
  @Nullable DoFnInfo<?, ?> getDoFnInfo() {
    return helpers.fnInfo;
  }
}
