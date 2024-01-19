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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateNamespaces.WindowNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.runners.core.TimerInternals.TimerDataCoder;
import org.apache.beam.runners.core.TimerInternals.TimerDataCoderV2;
import org.apache.beam.runners.dataflow.worker.streaming.sideinput.SideInputState;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GlobalDataRequest;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.Parser;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;

/** A class that handles streaming side inputs in a {@link DoFnRunner}. */
@SuppressWarnings({"keyfor", "nullness"}) // TODO(https://github.com/apache/beam/issues/20497)
public class StreamingSideInputFetcher<InputT, W extends BoundedWindow> {
  private StreamingModeExecutionContext.StreamingModeStepContext stepContext;
  private Map<String, PCollectionView<?>> sideInputViews;

  private final StateTag<BagState<WindowedValue<InputT>>> elementsAddr;
  private final StateTag<BagState<TimerData>> timersAddr;
  private final StateTag<BagState<TimerData>> oldTimersAddr;
  private final StateTag<WatermarkHoldState> watermarkHoldingAddr;
  private final StateTag<ValueState<Map<W, Set<Windmill.GlobalDataRequest>>>> blockedMapAddr;

  private Map<W, Set<Windmill.GlobalDataRequest>> blockedMap = null; // lazily initialized

  private final Coder<W> mainWindowCoder;

  public StreamingSideInputFetcher(
      Iterable<PCollectionView<?>> views,
      Coder<InputT> inputCoder,
      WindowingStrategy<?, W> windowingStrategy,
      StreamingModeExecutionContext.StreamingModeStepContext stepContext) {
    this.stepContext = stepContext;

    this.mainWindowCoder = windowingStrategy.getWindowFn().windowCoder();

    this.sideInputViews = new HashMap<>();
    for (PCollectionView<?> view : views) {
      sideInputViews.put(view.getTagInternal().getId(), view);
    }

    this.blockedMapAddr = blockedMapAddr(mainWindowCoder);
    this.elementsAddr =
        StateTags.makeSystemTagInternal(
            StateTags.bag("elem", WindowedValue.getFullCoder(inputCoder, mainWindowCoder)));
    this.oldTimersAddr =
        StateTags.makeSystemTagInternal(StateTags.bag("timer", TimerDataCoder.of(mainWindowCoder)));
    this.timersAddr =
        StateTags.makeSystemTagInternal(
            StateTags.bag("timerV2", TimerDataCoderV2.of(mainWindowCoder)));
    StateTag<WatermarkHoldState> watermarkTag =
        StateTags.watermarkStateInternal(
            "holdForSideinput", windowingStrategy.getTimestampCombiner());
    this.watermarkHoldingAddr = StateTags.makeSystemTagInternal(watermarkTag);
  }

  @VisibleForTesting
  static <W extends BoundedWindow>
      StateTag<ValueState<Map<W, Set<GlobalDataRequest>>>> blockedMapAddr(
          Coder<W> mainWindowCoder) {
    return StateTags.value(
        "blockedMap", MapCoder.of(mainWindowCoder, SetCoder.of(GlobalDataRequestCoder.of())));
  }

  /** Computes the set of main input windows for which all side inputs are ready and cached. */
  public Set<W> getReadyWindows() {
    Set<W> readyWindows = new HashSet<>();

    for (Windmill.GlobalDataId id : stepContext.getSideInputNotifications()) {
      if (sideInputViews.get(id.getTag()) == null) {
        // Side input is for a different DoFn; ignore it.
        continue;
      }

      for (Map.Entry<W, Set<Windmill.GlobalDataRequest>> entry : blockedMap().entrySet()) {
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
          W window = entry.getKey();
          boolean allSideInputsCached = true;
          for (PCollectionView<?> view : sideInputViews.values()) {
            if (!stepContext.issueSideInputFetch(view, window, SideInputState.KNOWN_READY)) {
              Windmill.GlobalDataRequest request = buildGlobalDataRequest(view, window);
              stepContext.addBlockingSideInput(request);
              windowBlockedSet.add(request);
              allSideInputsCached = false;
            }
          }

          if (allSideInputsCached) {
            readyWindows.add(window);
          }
        }
      }
    }

    return readyWindows;
  }

  public void prefetchBlockedMap() {
    stepContext.stateInternals().state(StateNamespaces.global(), blockedMapAddr).readLater();
  }

  public Iterable<BagState<WindowedValue<InputT>>> prefetchElements(Iterable<W> readyWindows) {
    List<BagState<WindowedValue<InputT>>> elements = Lists.newArrayList();
    for (W window : readyWindows) {
      elements.add(elementBag(window).readLater());
    }
    return elements;
  }

  public void releaseBlockedWindows(Iterable<W> windows) {
    for (W window : windows) {
      WatermarkHoldState watermarkHold = watermarkHold(window);
      watermarkHold.clear();

      blockedMap().remove(window);
    }
  }

  public Iterable<BagState<TimerData>> prefetchTimers(Iterable<W> readyWindows) {
    List<BagState<TimerData>> timers = Lists.newArrayList();
    for (W window : readyWindows) {
      timers.add(timerBag(window).readLater());
      timers.add(timerOldBag(window).readLater());
    }
    return timers;
  }

  /** Compute the set of side inputs that are not yet ready for the given main input window. */
  public boolean storeIfBlocked(WindowedValue<InputT> elem) {
    @SuppressWarnings("unchecked")
    W window = (W) Iterables.getOnlyElement(elem.getWindows());

    Set<Windmill.GlobalDataRequest> blocked = blockedMap().get(window);
    if (blocked == null) {
      for (PCollectionView<?> view : sideInputViews.values()) {
        if (!stepContext.issueSideInputFetch(view, window, SideInputState.UNKNOWN)) {
          if (blocked == null) {
            blocked = new HashSet<>();
            blockedMap().put(window, blocked);
          }
          blocked.add(buildGlobalDataRequest(view, window));
        }
      }
    }
    if (blocked != null) {
      elementBag(window).add(elem);
      watermarkHold(window).add(elem.getTimestamp());
      stepContext.addBlockingSideInputs(blocked);
      return true;
    } else {
      return false;
    }
  }

  public boolean storeIfBlocked(TimerData timer) {
    if (!(timer.getNamespace() instanceof WindowNamespace)) {
      throw new IllegalArgumentException(
          "Expected WindowNamespace, but was " + timer.getNamespace());
    }
    @SuppressWarnings("unchecked")
    WindowNamespace<W> windowNamespace = (WindowNamespace<W>) timer.getNamespace();
    W window = windowNamespace.getWindow();

    boolean blocked = false;
    for (PCollectionView<?> view : sideInputViews.values()) {
      if (!stepContext.issueSideInputFetch(view, window, SideInputState.UNKNOWN)) {
        blocked = true;
      }
    }
    if (blocked) {
      timerBag(window).add(timer);
    }
    return blocked;
  }

  public void persist() {
    if (blockedMap == null) {
      return;
    }

    ValueState<Map<W, Set<Windmill.GlobalDataRequest>>> mapState =
        stepContext.stateInternals().state(StateNamespaces.global(), blockedMapAddr);
    if (blockedMap.isEmpty()) {
      // Avoid storing the empty map so we don't leave unnecessary state behind from processing
      // the key.
      mapState.clear();
    } else {
      mapState.write(blockedMap);
    }
    blockedMap = null;
  }

  private Map<W, Set<Windmill.GlobalDataRequest>> blockedMap() {
    if (blockedMap == null) {
      blockedMap =
          stepContext.stateInternals().state(StateNamespaces.global(), blockedMapAddr).read();
      if (blockedMap == null) {
        blockedMap = new HashMap<>();
      }
    }
    return blockedMap;
  }

  @VisibleForTesting
  Set<W> getBlockedWindows() {
    return blockedMap().keySet();
  }

  @VisibleForTesting
  BagState<WindowedValue<InputT>> elementBag(W window) {
    return stepContext
        .stateInternals()
        .state(StateNamespaces.window(mainWindowCoder, window), elementsAddr);
  }

  @VisibleForTesting
  WatermarkHoldState watermarkHold(W window) {
    return stepContext
        .stateInternals()
        .state(StateNamespaces.window(mainWindowCoder, window), watermarkHoldingAddr);
  }

  @VisibleForTesting
  BagState<TimerData> timerBag(W window) {
    return stepContext
        .stateInternals()
        .state(StateNamespaces.window(mainWindowCoder, window), timersAddr);
  }

  BagState<TimerData> timerOldBag(W window) {
    return stepContext
        .stateInternals()
        .state(StateNamespaces.window(mainWindowCoder, window), oldTimersAddr);
  }

  private <SideWindowT extends BoundedWindow> Windmill.GlobalDataRequest buildGlobalDataRequest(
      PCollectionView<?> view, BoundedWindow mainWindow) {
    @SuppressWarnings("unchecked")
    WindowingStrategy<?, SideWindowT> sideWindowStrategy =
        (WindowingStrategy<?, SideWindowT>) view.getWindowingStrategyInternal();

    WindowFn<?, SideWindowT> sideWindowFn = sideWindowStrategy.getWindowFn();

    Coder<SideWindowT> sideInputWindowCoder = sideWindowFn.windowCoder();

    SideWindowT sideInputWindow =
        (SideWindowT) view.getWindowMappingFn().getSideInputWindow(mainWindow);

    ByteStringOutputStream windowStream = new ByteStringOutputStream();
    try {
      sideInputWindowCoder.encode(sideInputWindow, windowStream, Coder.Context.OUTER);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return Windmill.GlobalDataRequest.newBuilder()
        .setDataId(
            Windmill.GlobalDataId.newBuilder()
                .setTag(view.getTagInternal().getId())
                .setVersion(windowStream.toByteString())
                .build())
        .setExistenceWatermarkDeadline(
            WindmillTimeUtils.harnessToWindmillTimestamp(
                sideWindowStrategy.getTrigger().getWatermarkThatGuaranteesFiring(sideInputWindow)))
        .build();
  }

  private static class GlobalDataRequestCoder extends AtomicCoder<GlobalDataRequest> {
    private final Class<Windmill.GlobalDataRequest> protoMessageClass =
        Windmill.GlobalDataRequest.class;
    private transient Parser<Windmill.GlobalDataRequest> memoizedParser;

    public static GlobalDataRequestCoder of() {
      return new GlobalDataRequestCoder();
    }

    @Override
    public Windmill.GlobalDataRequest decode(InputStream inStream) throws IOException {
      return decode(inStream, Context.NESTED);
    }

    @Override
    public Windmill.GlobalDataRequest decode(InputStream inStream, Context context)
        throws IOException {
      if (context.isWholeStream) {
        return getParser().parseFrom(inStream);
      } else {
        return getParser().parseDelimitedFrom(inStream);
      }
    }

    @Override
    public void encode(Windmill.GlobalDataRequest value, OutputStream outStream)
        throws IOException {
      encode(value, outStream, Context.NESTED);
    }

    @Override
    public void encode(Windmill.GlobalDataRequest value, OutputStream outStream, Context context)
        throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null " + protoMessageClass.getSimpleName());
      }
      if (context.isWholeStream) {
        value.writeTo(outStream);
      } else {
        value.writeDelimitedTo(outStream);
      }
    }

    private Parser<Windmill.GlobalDataRequest> getParser() {
      if (memoizedParser == null) {
        memoizedParser = Windmill.GlobalDataRequest.getDefaultInstance().getParserForType();
      }
      return memoizedParser;
    }
  }
}
