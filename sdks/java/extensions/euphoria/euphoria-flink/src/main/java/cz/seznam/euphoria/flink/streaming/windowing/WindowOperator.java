/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.streaming.windowing;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.TimedWindow;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.StateFactory;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.MergingStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.flink.storage.Descriptors;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.HeapInternalTimerService;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class WindowOperator<KEY, WID extends Window>
        extends AbstractStreamOperator<WindowedElement<WID, Pair<?, ?>>>
        implements OneInputStreamOperator<KeyedMultiWindowedElement<WID, ?, ?>, WindowedElement<WID, Pair<?, ?>>>,
        Triggerable<KEY, WID> {

  private final Windowing<?, WID> windowing;
  private final Trigger<WID> trigger;
  private final StateFactory<?, State> stateFactory;
  private final CombinableReduceFunction<State> stateCombiner;


  // FIXME Arguable hack that ensures all remaining opened windows
  // are flushed to output when end of stream is reached. This is intended
  // behavior in test environment only. In cluster environment reaching
  // end of stream without killing the job is not likely.
  // Tracking of opened windows have a huge performance impact.
  /** True when executor is running in local test (mode) */
  private final boolean localMode;

  private transient InternalTimerService<WID> timerService;

  // tracks existing windows to flush them in case of end of stream is reached
  private transient InternalTimerService<WID> endOfStreamTimerService;

  private transient TriggerContextAdapter triggerContext;
  private transient OutputContext outputContext;
  private transient WindowedStorageProvider storageProvider;

  private transient ListStateDescriptor<Tuple2<WID, WID>> mergingWindowsDescriptor;

  private transient TypeSerializer<WID> windowSerializer;

  // cached window states by key+window
  private transient Table<KEY, WID, State> windowStates;

  public WindowOperator(Windowing<?, WID> windowing,
                        StateFactory<?, State> stateFactory,
                        CombinableReduceFunction<State> stateCombiner,
                        boolean localMode) {
    this.windowing = Objects.requireNonNull(windowing);
    this.trigger = windowing.getTrigger();
    this.stateFactory = Objects.requireNonNull(stateFactory);
    this.stateCombiner = Objects.requireNonNull(stateCombiner);
    this.localMode = localMode;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void open() throws Exception {
    super.open();

    this.windowSerializer =
            (TypeSerializer<WID>) TypeExtractor.createTypeInfo(Window.class)
                    .createSerializer(getRuntimeContext().getExecutionConfig());

    this.timerService =
            getInternalTimerService("window-timers", windowSerializer, this);
    this.endOfStreamTimerService =
            getInternalTimerService("end-of-stream-timers", windowSerializer, this);
    this.triggerContext = new TriggerContextAdapter();
    this.outputContext = new OutputContext();
    this.storageProvider = new WindowedStorageProvider<>(
            getKeyedStateBackend(), windowSerializer);

    this.windowStates = HashBasedTable.create();

    if (windowing instanceof MergingWindowing) {
      TupleSerializer<Tuple2<WID, WID>> tupleSerializer =
              new TupleSerializer<>((Class) Tuple2.class, new TypeSerializer[]{windowSerializer, windowSerializer});
      this.mergingWindowsDescriptor = new ListStateDescriptor<>("merging-window-set", tupleSerializer);
    }
  }

  private void setupEnvironment(Object key, WID window) {
    outputContext.setKey(key);
    outputContext.setWindow(window);
    storageProvider.setWindow(window);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void processElement(StreamRecord<KeyedMultiWindowedElement<WID, ?, ?>> record)
          throws Exception {

    KeyedMultiWindowedElement<WID, ?, ?> element = record.getValue();

    // drop late-comers immediately
    if (element.getTimestamp() < timerService.currentWatermark()) {
      return;
    }

    if (windowing instanceof MergingWindowing) {
      MergingWindowSet<WID> mergingWindowSet = getMergingWindowSet();

      for (WID window : element.getWindows()) {

        // when window is added to the set it may result in window merging
        WID currentWindow = mergingWindowSet.addWindow(window,
                (mergeResult, mergedWindows, stateResultWindow, mergedStateWindows) -> {

                  setupEnvironment(getCurrentKey(), mergeResult);
                  triggerContext.setWindow(mergeResult);

                  processTriggerResult(mergeResult,
                          triggerContext.onMerge(mergedWindows),
                          mergingWindowSet);

                  // clear all merged windows
                  for (WID merged : mergedWindows) {
                    storageProvider.setWindow(merged);
                    trigger.onClear(merged, triggerContext);
                    removeWindow(merged, null);
                  }

                  // FIXME This implementation relies on fact that
                  // stateCombiner function is actually "fold left".
                  // That means the result state will end up in the
                  // first state given to the combiner function.

                  // merge all mergedStateWindows into stateResultWindow
                  List<State> states = new ArrayList<>();
                  states.add(getWindowState(stateResultWindow));
                  mergedStateWindows.forEach(sw -> states.add(getWindowState(sw)));
                  stateCombiner.apply(states);

                  // remove merged window states
                  mergedStateWindows.forEach(sw -> {
                    getWindowState(sw).close();
                    removeState(sw);
                  });
        });

        setupEnvironment(getCurrentKey(), currentWindow);

        if (localMode) {
          endOfStreamTimerService.registerEventTimeTimer(currentWindow, Long.MAX_VALUE);
        }

        // process trigger
        Trigger.TriggerResult triggerResult = trigger.onElement(
                element.getTimestamp(),
                currentWindow,
                triggerContext);

        WID stateWindow = mergingWindowSet.getStateWindow(currentWindow);
        setupEnvironment(getCurrentKey(), stateWindow);
        getWindowState(stateWindow).add(element.getValue());

        processTriggerResult(currentWindow, triggerResult, mergingWindowSet);
      }
      mergingWindowSet.persist();

    } else {
      for (WID window : element.getWindows()) {
        setupEnvironment(getCurrentKey(), window);

        if (localMode) {
          // register cleanup timer for each window
          endOfStreamTimerService.registerEventTimeTimer(window, Long.MAX_VALUE);
        }

        getWindowState(window).add(element.getValue());

        // process trigger
        Trigger.TriggerResult triggerResult = trigger.onElement(
                element.getTimestamp(),
                window,
                triggerContext);

        processTriggerResult(window, triggerResult, null);
      }
    }
  }

  @Override
  public void onEventTime(InternalTimer<KEY, WID> timer) throws Exception {
    WID window = timer.getNamespace();
    Object key = timer.getKey();

    setupEnvironment(key, window);

    Trigger.TriggerResult triggerResult;
    if (timer.getTimestamp() == Long.MAX_VALUE) {
      // at the end of time flush window immediately
      triggerResult = Trigger.TriggerResult.FLUSH_AND_PURGE;
    } else {
      triggerResult = trigger.onTimer(
              timer.getTimestamp(),
              window,
              triggerContext);
    }

    MergingWindowSet<WID> mergingWindowSet = null;
    if (windowing instanceof MergingWindowing) {
      mergingWindowSet = getMergingWindowSet();
    }

    processTriggerResult(window, triggerResult, mergingWindowSet);

    if (mergingWindowSet != null) {
      mergingWindowSet.persist();
    }
  }

  @Override
  public void onProcessingTime(InternalTimer<KEY, WID> timer) throws Exception {
    throw new UnsupportedOperationException("We are not using processing time at all");
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    // FIXME relies heavily on Flink internal implementation
    // which may change in the future and break this

    // we need to advance watermark of timer services in given order
    // timerService first to fire all scheduled triggers
    ((HeapInternalTimerService) timerService).advanceWatermark(mark.getTimestamp());
    if (localMode) {
      ((HeapInternalTimerService) endOfStreamTimerService).advanceWatermark(mark.getTimestamp());
    }

    output.emitWatermark(mark);
  }

  private void processTriggerResult(WID window,
                                    Trigger.TriggerResult tr,
                                    @Nullable MergingWindowSet<WID> mergingWindowSet) {

    if (tr.isFlush() || tr.isPurge()) {
      WID stateWindow = window;
      State windowState;

      if (windowing instanceof MergingWindowing) {
        Objects.requireNonNull(mergingWindowSet);
        stateWindow = mergingWindowSet.getStateWindow(window);
        windowState = getWindowState(stateWindow);
      } else {
        windowState = getWindowState(window);
      }

      if (tr.isFlush() || tr.isPurge()) {
        if (tr.isFlush()) {
          windowState.flush();
        }

        if (tr.isPurge()) {
          windowState.close();
          trigger.onClear(window, triggerContext);
          removeWindow(window, mergingWindowSet);
          removeState(stateWindow);
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private State getWindowState(WID window) {
    State windowState = windowStates.get(getCurrentKey(), window);
    if (windowState == null) {
      windowState = stateFactory.apply(outputContext, storageProvider);
      windowStates.put((KEY) getCurrentKey(), window, windowState);
    }

    return windowState;
  }

  private MergingWindowSet<WID> getMergingWindowSet()  {
    try {
      ListState<Tuple2<WID, WID>> mergeState =
              getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE,
                      mergingWindowsDescriptor);

      return new MergingWindowSet<>((MergingWindowing) windowing, mergeState);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void removeWindow(WID window, @Nullable MergingWindowSet<WID> windowSet) {
    try {
      if (localMode) {
        endOfStreamTimerService.deleteEventTimeTimer(window, Long.MAX_VALUE);
      }
      if (windowSet != null) {
        windowSet.removeWindow(window);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void removeState(WID stateWindow) {
    windowStates.remove(getCurrentKey(), stateWindow);
  }

  // -------------------

  private class TriggerContextAdapter implements TriggerContext, TriggerContext.TriggerMergeContext {

    private WID window;

    private Collection<WID> mergedWindows;

    @Override
    @SuppressWarnings("unchecked")
    public boolean registerTimer(long stamp, Window window) {
      if (stamp <= getCurrentTimestamp()) {
        return false;
      }

      timerService.registerEventTimeTimer((WID) window, stamp);
      return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void deleteTimer(long stamp, Window window) {
      timerService.deleteEventTimeTimer((WID) window, stamp);
    }

    @Override
    public long getCurrentTimestamp() {
      return timerService.currentWatermark();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
      return storageProvider.getValueStorage(descriptor);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
      return storageProvider.getListStorage(descriptor);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void mergeStoredState(StorageDescriptor descriptor) {
      try {
        Objects.requireNonNull(mergedWindows);
        if (!(descriptor instanceof MergingStorageDescriptor)) {
          throw new IllegalStateException(
                  "Storage descriptor '" + descriptor.getName() + "' must support merging!");
        }

        if (!mergedWindows.isEmpty()) {
          if (descriptor instanceof ValueStorageDescriptor.MergingValueStorageDescriptor) {
            getKeyedStateBackend().mergePartitionedStates(window,
                    mergedWindows,
                    windowSerializer,
                    Descriptors.from((ValueStorageDescriptor.MergingValueStorageDescriptor) descriptor));
            return;
          }
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      throw new UnsupportedOperationException(descriptor + " is not supported for merging yet!");
    }

    private Trigger.TriggerResult onMerge(Iterable<WID> mergedWindows) {
      this.mergedWindows = Lists.newArrayList(mergedWindows);
      return trigger.onMerge(window, this);
    }

    private void setWindow(WID window) {
      this.window = window;
    }
  }

  private class OutputContext implements Context {

    private Object key;
    private Window window;

    private final StreamRecord reuse = new StreamRecord<>(null);

    @Override
    @SuppressWarnings("unchecked")
    public void collect(Object elem) {
      long stamp = (window instanceof TimedWindow)
              ? ((TimedWindow) window).maxTimestamp()
              : timerService.currentWatermark();

      // FIXME timestamp is duplicated here
      output.collect(reuse.replace(
              new WindowedElement<>(window, stamp, Pair.of(key, elem)),
              stamp));
    }

    @Override
    public Object getWindow() {
      return window;
    }

    public void setWindow(Window window) {
      this.window = window;
    }

    public void setKey(Object key) {
      this.key = key;
    }
  }
}
