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

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.MapCoder;
import com.google.cloud.dataflow.sdk.coders.Proto2Coder;
import com.google.cloud.dataflow.sdk.coders.SetCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill;
import com.google.cloud.dataflow.sdk.runners.worker.windmill.Windmill.GlobalDataRequest;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.ExecutionContext.StepContext;
import com.google.cloud.dataflow.sdk.util.StateFetcher.SideInputState;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.state.BagState;
import com.google.cloud.dataflow.sdk.util.state.StateNamespaces;
import com.google.cloud.dataflow.sdk.util.state.StateTag;
import com.google.cloud.dataflow.sdk.util.state.StateTags;
import com.google.cloud.dataflow.sdk.util.state.ValueState;
import com.google.cloud.dataflow.sdk.util.state.WatermarkStateInternal;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Runs a DoFn by constructing the appropriate contexts and passing them in.
 *
 * @param <InputT> the type of the DoFn's (main) input elements
 * @param <OutputT> the type of the DoFn's (main) output elements
 * @param <W> the type of the windows of the main input
 */
public class StreamingSideInputDoFnRunner<InputT, OutputT, W extends BoundedWindow>
    extends DoFnRunner<InputT, OutputT> {
  private StreamingModeExecutionContext.StepContext stepContext;
  private StreamingModeExecutionContext execContext;
  private Map<String, PCollectionView<?>> sideInputViews;

  private final StateTag<BagState<WindowedValue<InputT>>> elementsAddr;
  private final StateTag<WatermarkStateInternal> watermarkHoldingAddr;
  private final StateTag<ValueState<Map<W, Set<Windmill.GlobalDataRequest>>>> blockedMapAddr;

  private Map<W, Set<Windmill.GlobalDataRequest>> blockedMap;

  private WindowFn<?, W> windowFn;

  public StreamingSideInputDoFnRunner(
      PipelineOptions options,
      DoFnInfo<InputT, OutputT> doFnInfo,
      SideInputReader sideInputReader,
      OutputManager outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      CounterSet.AddCounterMutator addCounterMutator) throws Exception {
    super(options, doFnInfo.getDoFn(), sideInputReader, outputManager,
        mainOutputTag, sideOutputTags, stepContext,
        addCounterMutator, doFnInfo.getWindowingStrategy());
    this.stepContext = (StreamingModeExecutionContext.StepContext) stepContext;

    WindowFn<?, ? extends BoundedWindow> wildcardWindowFn =
        doFnInfo.getWindowingStrategy().getWindowFn();
    @SuppressWarnings("unchecked")
    WindowFn<?, W> typedWindowFn = (WindowFn<?, W>) wildcardWindowFn;
    this.windowFn = typedWindowFn;

    this.sideInputViews = new HashMap<>();
    for (PCollectionView<?> view : doFnInfo.getSideInputViews()) {
      sideInputViews.put(view.getTagInternal().getId(), view);
    }
    this.execContext =
        (StreamingModeExecutionContext) stepContext.getExecutionContext();

    this.blockedMapAddr = blockedMapAddr(windowFn);
    this.elementsAddr = StateTags.makeSystemTagInternal(StateTags.bag("elem",
        WindowedValue.getFullCoder(doFnInfo.getInputCoder(), windowFn.windowCoder())));
    this.watermarkHoldingAddr =
        StateTags.makeSystemTagInternal(StateTags.watermarkStateInternal("hold"));

    this.blockedMap = stepContext.stateInternals().state(StateNamespaces.global(), blockedMapAddr)
        .get().read();
    if (this.blockedMap == null) {
      this.blockedMap = new HashMap<>();
    }
  }

  @VisibleForTesting static <W extends BoundedWindow>
  StateTag<ValueState<Map<W, Set<GlobalDataRequest>>>> blockedMapAddr(WindowFn<?, W> windowFn) {
    return StateTags.value("blockedMap", MapCoder.of(
        windowFn.windowCoder(), SetCoder.of(Proto2Coder.of(Windmill.GlobalDataRequest.class))));
  }

  /**
   * Computes the set of main input windows for which all side inputs are ready and cached.
   */
  private Set<W> getReadyWindows() {
    Set<W> readyWindows = new HashSet<>();

    for (Windmill.GlobalDataId id : execContext.getSideInputNotifications()) {
      if (sideInputViews.get(id.getTag()) == null) {
        // Side input is for a different DoFn; ignore it.
        continue;
      }

      for (Map.Entry<W, Set<Windmill.GlobalDataRequest>> entry : blockedMap.entrySet()) {
        Set<Windmill.GlobalDataRequest> windowBlockedSet = entry.getValue();
        Set<Windmill.GlobalDataRequest> found = new HashSet<>();
        for (Windmill.GlobalDataRequest request : windowBlockedSet) {
          if (id.equals(request.getDataId())) {
            found.add(request);
          }
        }

        windowBlockedSet.removeAll(found);

        if (windowBlockedSet.isEmpty()) {
          // Notifications were received for all side inputs for this window.
          // Issue fetches for all the needed side inputs to make sure they are all present
          // in the local cache.  If not, note the side inputs as still being blocked.
          try {
            W window = entry.getKey();
            boolean allSideInputsCached = true;
            for (PCollectionView<?> view : sideInputViews.values()) {
              if (!stepContext.issueSideInputFetch(
                  view, window, SideInputState.KNOWN_READY)) {
                Windmill.GlobalDataRequest request = buildGlobalDataRequest(view, window);
                stepContext.addBlockingSideInput(request);
                windowBlockedSet.add(request);
                allSideInputsCached = false;
              }
            }

            if (allSideInputsCached) {
              readyWindows.add(window);
            }
          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
        }
      }
    }

    return readyWindows;
  }

  @Override
  public void startBundle() {
    super.startBundle();

    // Find the set of ready windows.
    Set<W> readyWindows = getReadyWindows();

    // Pre-fetch the elements for each of the ready windows.
    for (W window : readyWindows) {
      elementBag(window).get();
      WatermarkStateInternal watermarkHold = watermarkHold(window);
      watermarkHold.get();
      watermarkHold.clear();
    }

    // Run the DoFn code now that all side inputs are ready.
    for (W window : readyWindows) {
      blockedMap.remove(window);

      BagState<WindowedValue<InputT>> elementsBag = elementBag(window);
      Iterable<WindowedValue<InputT>> elements = elementsBag.get().read();
      try {
        for (WindowedValue<InputT> elem : elements) {
          fn.processElement(createProcessContext(elem));
        }
      } catch (Throwable t) {
        // Exception in user code.
        Throwables.propagateIfInstanceOf(t, UserCodeException.class);
        throw new UserCodeException(t);
      }

      elementsBag.clear();
    }
  }

  /**
   * Compute the set of side inputs that are not yet ready for the given main input window.
   */
  private Set<Windmill.GlobalDataRequest> computeBlockedSideInputs(W window) throws IOException {
    Set<Windmill.GlobalDataRequest> blocked = blockedMap.get(window);
    if (blocked == null) {
      for (PCollectionView<?> view : sideInputViews.values()) {
        if (!stepContext.issueSideInputFetch(view, window, SideInputState.UNKNOWN)) {
          if (blocked == null) {
            blocked = new HashSet<>();
            blockedMap.put(window, blocked);
          }
          blocked.add(buildGlobalDataRequest(view, window));
        }
      }
    }
    return blocked;
  }

  @VisibleForTesting BagState<WindowedValue<InputT>> elementBag(W window) {
    return stepContext.stateInternals()
        .state(StateNamespaces.window(windowFn.windowCoder(), window), elementsAddr);
  }

  @VisibleForTesting WatermarkStateInternal watermarkHold(W window) {
    return stepContext.stateInternals()
        .state(StateNamespaces.window(windowFn.windowCoder(), window), watermarkHoldingAddr);
  }

  @Override
  public void invokeProcessElement(WindowedValue<InputT> elem) {
    @SuppressWarnings("unchecked")
    W window = (W) Iterables.getOnlyElement(elem.getWindows());

    // This can contain user code. Wrap it in case it throws an exception.
    try {
      Set<Windmill.GlobalDataRequest> blocked = computeBlockedSideInputs(window);
      if (blocked == null) {
        fn.processElement(createProcessContext(elem));
      } else {
        elementBag(window).add(elem);
        watermarkHold(window).add(elem.getTimestamp());

        stepContext.addBlockingSideInputs(blocked);
      }
    } catch (Throwable t) {
      // Exception in user code.
      Throwables.propagateIfInstanceOf(t, UserCodeException.class);
      throw new UserCodeException(t);
    }
  }

  @Override
  public void finishBundle() {
    super.finishBundle();
    stepContext.stateInternals().state(StateNamespaces.global(), blockedMapAddr).set(blockedMap);
  }

  private <SideWindowT extends BoundedWindow> Windmill.GlobalDataRequest buildGlobalDataRequest(
      PCollectionView<?> view, BoundedWindow mainWindow) throws IOException {
    @SuppressWarnings("unchecked")
    WindowingStrategy<?, SideWindowT> sideWindowStrategy =
        (WindowingStrategy<?, SideWindowT>) view.getWindowingStrategyInternal();

    WindowFn<?, SideWindowT> sideWindowFn = sideWindowStrategy.getWindowFn();

    Coder<SideWindowT> sideInputWindowCoder = sideWindowFn.windowCoder();

    SideWindowT sideInputWindow = sideWindowFn.getSideInputWindow(mainWindow);

    ByteString.Output windowStream = ByteString.newOutput();
    sideInputWindowCoder.encode(sideInputWindow, windowStream, Coder.Context.OUTER);

    return Windmill.GlobalDataRequest.newBuilder()
        .setDataId(Windmill.GlobalDataId.newBuilder()
            .setTag(view.getTagInternal().getId())
            .setVersion(windowStream.toByteString())
            .build())
        .setExistenceWatermarkDeadline(
            TimeUnit.MILLISECONDS.toMicros(sideWindowStrategy
                .getTrigger()
                .getSpec()
                .getWatermarkThatGuaranteesFiring(sideInputWindow)
                .getMillis()))
        .build();
  }
}
