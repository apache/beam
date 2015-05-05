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
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.ExecutionContext.StepContext;
import com.google.cloud.dataflow.sdk.util.StateFetcher.SideInputState;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.base.Throwables;
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
 * @param <ReceiverT> the type of object that receives outputs
 * @param <W> the type of the windows of the main input
 */
public class StreamingSideInputDoFnRunner<InputT, OutputT, ReceiverT, W extends BoundedWindow>
    extends DoFnRunner<InputT, OutputT, ReceiverT> {
  private StepContext stepContext;
  private StreamingModeExecutionContext execContext;
  private WindowingStrategy<?, W> windowingStrategy;
  private Map<String, PCollectionView<?>> sideInputViews;
  private CodedTupleTag<Map<W, Set<Windmill.GlobalDataRequest>>> blockedMapTag;
  private Map<W, Set<Windmill.GlobalDataRequest>> blockedMap;
  private Coder<InputT> elemCoder;

  public StreamingSideInputDoFnRunner(
      PipelineOptions options,
      DoFnInfo<InputT, OutputT> doFnInfo,
      PTuple sideInputs,
      OutputManager<ReceiverT> outputManager,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      StepContext stepContext,
      CounterSet.AddCounterMutator addCounterMutator) throws Exception {
    super(options, doFnInfo.getDoFn(), sideInputs, outputManager,
        mainOutputTag, sideOutputTags, stepContext,
        addCounterMutator, doFnInfo.getWindowingStrategy());
    this.stepContext = stepContext;
    this.windowingStrategy = (WindowingStrategy) doFnInfo.getWindowingStrategy();
    this.elemCoder = doFnInfo.getInputCoder();

    this.sideInputViews = new HashMap<>();
    for (PCollectionView<?> view : doFnInfo.getSideInputViews()) {
      sideInputViews.put(view.getTagInternal().getId(), view);
    }
    this.execContext =
        (StreamingModeExecutionContext) stepContext.getExecutionContext();
    this.blockedMapTag = CodedTupleTag.of("blockedMap:", MapCoder.of(
        windowingStrategy.getWindowFn().windowCoder(),
        SetCoder.of(Proto2Coder.of(Windmill.GlobalDataRequest.class))));
    this.blockedMap = stepContext.lookup(blockedMapTag);
    if (this.blockedMap == null) {
      this.blockedMap = new HashMap<>();
    }
  }

  /**
   * Computes the set of main input windows for which all side inputs are ready and cached.
   */
  private Map<W, CodedTupleTag<WindowedValue<InputT>>> getReadyWindowTags() {
    Map<W, CodedTupleTag<WindowedValue<InputT>>> readyWindowTags = new HashMap<>();

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
              if (!execContext.issueSideInputFetch(
                  view, window, SideInputState.KNOWN_READY)) {
                Windmill.GlobalDataRequest request = buildGlobalDataRequest(view, window);
                execContext.addBlockingSideInput(request);
                windowBlockedSet.add(request);
                allSideInputsCached = false;
              }
            }

            if (allSideInputsCached) {
              readyWindowTags.put(window, getElemListTag(window));
            }
          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
        }
      }
    }

    return readyWindowTags;
  }

  @Override
  public void startBundle() {
    super.startBundle();

    // Find the set of ready windows.
    Map<W, CodedTupleTag<WindowedValue<InputT>>> readyWindowTags = getReadyWindowTags();

    // Fetch the elements for each of the ready windows.
    Map<CodedTupleTag<WindowedValue<InputT>>, Iterable<WindowedValue<InputT>>> elementsPerWindow;
    try {
      elementsPerWindow = stepContext.readTagLists(readyWindowTags.values());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    // Run the DoFn code now that all side inputs are ready.
    for (Map.Entry<W, CodedTupleTag<WindowedValue<InputT>>> entry : readyWindowTags.entrySet()) {
      blockedMap.remove(entry.getKey());

      Iterable<WindowedValue<InputT>> elements = elementsPerWindow.get(entry.getValue());
      try {
        for (WindowedValue<InputT> elem : elements) {
          fn.processElement(createProcessContext(elem));
        }
      } catch (Throwable t) {
        // Exception in user code.
        Throwables.propagateIfInstanceOf(t, UserCodeException.class);
        throw new UserCodeException(t);
      }

      stepContext.deleteTagList(entry.getValue());
    }
  }

  /**
   * Compute the set of side inputs that are not yet ready for the given main input window.
   */
  private Set<Windmill.GlobalDataRequest> computeBlockedSideInputs(W window) throws IOException {
    Set<Windmill.GlobalDataRequest> blocked = blockedMap.get(window);
    if (blocked == null) {
      for (PCollectionView<?> view : sideInputViews.values()) {
        if (!execContext.issueSideInputFetch(view, window, SideInputState.UNKNOWN)) {
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

  @Override
  public void invokeProcessElement(WindowedValue<InputT> elem) {
    // This can contain user code. Wrap it in case it throws an exception.
    try {
      W window = (W) elem.getWindows().iterator().next();

      Set<Windmill.GlobalDataRequest> blocked = computeBlockedSideInputs(window);
      if (blocked == null) {
        fn.processElement(createProcessContext(elem));
      } else {
        stepContext.writeToTagList(
            getElemListTag(window), elem, elem.getTimestamp());

        execContext.addBlockingSideInputs(blocked);
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
    try {
      stepContext.store(blockedMapTag, blockedMap);
    } catch (IOException e) {
      throw new RuntimeException("Exception while storing streaming side input info: ", e);
    }
  }

  private Windmill.GlobalDataRequest buildGlobalDataRequest(
      PCollectionView<?> view, BoundedWindow window) throws IOException {
    Coder<BoundedWindow> sideInputWindowCoder =
        view.getWindowingStrategyInternal().getWindowFn().windowCoder();

    BoundedWindow sideInputWindow =
        view.getWindowingStrategyInternal().getWindowFn().getSideInputWindow(window);

    ByteString.Output windowStream = ByteString.newOutput();
    sideInputWindowCoder.encode(sideInputWindow, windowStream, Coder.Context.OUTER);

    return Windmill.GlobalDataRequest.newBuilder()
        .setDataId(Windmill.GlobalDataId.newBuilder()
            .setTag(view.getTagInternal().getId())
            .setVersion(windowStream.toByteString())
            .build())
        .setExistenceWatermarkDeadline(
            TimeUnit.MILLISECONDS.toMicros(view.getWindowingStrategyInternal()
                .getTrigger()
                .getSpec()
                .getWatermarkCutoff(sideInputWindow)
                .getMillis()))
        .build();
  }

  private CodedTupleTag<WindowedValue<InputT>> getElemListTag(W window) throws IOException {
    return CodedTupleTag.<WindowedValue<InputT>>of(
        "e:" + CoderUtils.encodeToBase64(windowingStrategy.getWindowFn().windowCoder(), window),
        WindowedValue.getFullCoder(elemCoder, windowingStrategy.getWindowFn().windowCoder()));
  }
}
