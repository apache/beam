/**
 * Copyright 2016-2017 Seznam.cz, a.s.
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

import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.flink.accumulators.AbstractCollector;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.MergingStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateFactory;
import cz.seznam.euphoria.core.client.operator.state.StateMerger;
import cz.seznam.euphoria.core.client.operator.state.StorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.flink.accumulators.FlinkAccumulatorFactory;
import cz.seznam.euphoria.flink.storage.Descriptors;
import cz.seznam.euphoria.flink.streaming.StreamingElement;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RuntimeContext;
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
import org.apache.flink.api.java.ExecutionEnvironment;

public abstract class AbstractWindowOperator<I, KEY, WID extends Window>
        extends AbstractStreamOperator<StreamingElement<WID, Pair<?, ?>>>
        implements OneInputStreamOperator<I, StreamingElement<WID, Pair<?, ?>>>,
        Triggerable<KEY, WID> {

  private final Windowing<?, WID> windowing;
  private final Trigger<WID> trigger;
  private final StateFactory<?, ?, State<?, ?>> stateFactory;
  private final StateMerger<?, ?, State<?, ?>> stateCombiner;

  // FIXME Arguable hack that ensures all remaining opened windows
  // are flushed to output when end of stream is reached. This is intended
  // behavior in test environment only. In cluster environment reaching
  // end of stream without killing the job is not likely.
  // Tracking of opened windows have a huge performance impact.
  /** True when executor is running in local test (mode) */
  private final boolean localMode;

  private final boolean allowEarlyEmitting;

  // see {@link WindowedStorageProvider}
  private final int descriptorsCacheMaxSize;

  private final FlinkAccumulatorFactory accumulatorFactory;
  private final Settings settings;

  private transient InternalTimerService<WID> timerService;

  // tracks existing windows to flush them in case of end of stream is reached
  private transient InternalTimerService<WID> endOfStreamTimerService;

  private transient TriggerContextAdapter triggerContext;
  private transient OutputCollector outputContext;
  private transient WindowedStateContext<WID> stateContext;

  private transient ListStateDescriptor<Tuple2<WID, WID>> mergingWindowsDescriptor;

  private transient TypeSerializer<WID> windowSerializer;

  public AbstractWindowOperator(Windowing<?, WID> windowing,
                                StateFactory<?, ?, State<?, ?>> stateFactory,
                                StateMerger<?, ?, State<?, ?>> stateCombiner,
                                boolean localMode,
                                int descriptorsCacheMaxSize,
                                boolean allowEarlyEmitting,
                                FlinkAccumulatorFactory accumulatorFactory,
                                Settings settings) {
    this.windowing = Objects.requireNonNull(windowing);
    this.trigger = windowing.getTrigger();
    this.stateFactory = Objects.requireNonNull(stateFactory);
    this.stateCombiner = Objects.requireNonNull(stateCombiner);
    this.localMode = localMode;
    this.descriptorsCacheMaxSize = descriptorsCacheMaxSize;
    this.allowEarlyEmitting = allowEarlyEmitting;
    this.accumulatorFactory = Objects.requireNonNull(accumulatorFactory);
    this.settings = Objects.requireNonNull(settings);
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
    this.outputContext = new OutputCollector(
        accumulatorFactory, settings, getRuntimeContext());
    this.stateContext = new WindowedStateContext<>(
        ExecutionEnvironment.getExecutionEnvironment().getConfig(),
        settings, getKeyedStateBackend(), windowSerializer,
        descriptorsCacheMaxSize);

    if (windowing instanceof MergingWindowing) {
      TupleSerializer<Tuple2<WID, WID>> tupleSerializer = new TupleSerializer<>(
          (Class) Tuple2.class, new TypeSerializer[]{windowSerializer, windowSerializer});
      this.mergingWindowsDescriptor = new ListStateDescriptor<>("merging-window-set", tupleSerializer);
    }
  }

  private void setupEnvironment(Object key, WID window) {
    outputContext.setKey(key);
    outputContext.setWindow(window);
    stateContext.setWindow(window);
  }

  /**
   * Extracts the data element from the input stream record.
   *
   * @param record input stream record
   *
   * @return extracted data element fro Flink stream record
   * @throws Exception on error
   */
  protected abstract KeyedMultiWindowedElement<WID, KEY, ?> recordValue(
      StreamRecord<I> record) throws Exception;

  @Override
  @SuppressWarnings("unchecked")
  public void processElement(StreamRecord<I> record)
          throws Exception {

    // drop late-comers immediately
    if (record.getTimestamp() < timerService.currentWatermark()) {
      return;
    }

    KeyedMultiWindowedElement<WID, KEY, ?> element = recordValue(record);

    if (windowing instanceof MergingWindowing) {
      MergingWindowSet<WID> mergingWindowSet = getMergingWindowSet();

      for (WID window : element.getWindows()) {

        // when window is added to the set it may result in window merging
        WID currentWindow = mergingWindowSet.addWindow(window,
                (mergeResult, mergedWindows, stateResultWindow, mergedStateWindows) -> {

                  setupEnvironment(getCurrentKey(), mergeResult);
                  triggerContext.setWindow(mergeResult);

                  // merge trigger states
                  triggerContext.onMerge(mergedWindows);

                  // clear all merged windows
                  for (WID merged : mergedWindows) {
                    stateContext.setWindow(merged);
                    trigger.onClear(merged, triggerContext);
                    removeWindow(merged, null);
                  }

                  // merge all mergedStateWindows into stateResultWindow
                  {
                    List<State> states = new ArrayList<>();
                    mergedStateWindows.forEach(sw -> states.add(getWindowState(sw)));
                    stateCombiner.merge(getWindowState(stateResultWindow), (List) states);
                  }

                  // remove merged window states
                  mergedStateWindows.forEach(sw -> {
                    getWindowState(sw).close();
                  });
        });

        setupEnvironment(getCurrentKey(), currentWindow);

        if (localMode) {
          endOfStreamTimerService.registerEventTimeTimer(currentWindow, Long.MAX_VALUE);
        }

        // process trigger
        Trigger.TriggerResult triggerResult = trigger.onElement(
                record.getTimestamp(),
                currentWindow,
                triggerContext);

        WID stateWindow = mergingWindowSet.getStateWindow(currentWindow);
        setupEnvironment(getCurrentKey(), stateWindow);
        State stateWindowState = getWindowState(stateWindow);
        stateWindowState.add(element.getValue());

        processTriggerResult(currentWindow, stateWindowState, triggerResult, mergingWindowSet);
      }
      mergingWindowSet.persist();

    } else {
      for (WID window : element.getWindows()) {
        setupEnvironment(getCurrentKey(), window);

        if (localMode) {
          // register cleanup timer for each window
          endOfStreamTimerService.registerEventTimeTimer(window, Long.MAX_VALUE);
        }

        State windowState = getWindowState(window);
        windowState.add(element.getValue());

        // process trigger
        Trigger.TriggerResult triggerResult = trigger.onElement(
                record.getTimestamp(),
                window,
                triggerContext);

        processTriggerResult(window, windowState, triggerResult, null);
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
      triggerResult = trigger.onTimer(timer.getTimestamp(), window, triggerContext);
    }

    MergingWindowSet<WID> mergingWindowSet = null;
    if (windowing instanceof MergingWindowing) {
      mergingWindowSet = getMergingWindowSet();
    }

    processTriggerResult(window, null, triggerResult, mergingWindowSet);

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

  @SuppressWarnings("unchecked")
  private void processTriggerResult(WID window,
                                    // ~ @windowState the state of `window` to
                                    // use; if `null` the state will be fetched
                                    @Nullable  State windowState,
                                    Trigger.TriggerResult tr,
                                    @Nullable MergingWindowSet<WID> mergingWindowSet) {

    if (tr.isFlush() || tr.isPurge()) {
      if (windowState == null) {
        if (windowing instanceof MergingWindowing) {
          Objects.requireNonNull(mergingWindowSet);
          windowState = getWindowState(mergingWindowSet.getStateWindow(window));
        } else {
          windowState = getWindowState(window);
        }
      }

      if (tr.isFlush()) {
        windowState.flush(outputContext);
      }

      if (tr.isPurge()) {
        windowState.close();
        stateContext.setWindow(window);
        trigger.onClear(window, triggerContext);
        removeWindow(window, mergingWindowSet);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private State getWindowState(WID window) {
    stateContext.setWindow(window);
    return stateFactory.createState(stateContext, allowEarlyEmitting ? outputContext : null);
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
      return stateContext.getStorageProvider().getValueStorage(descriptor);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
      return stateContext.getStorageProvider().getListStorage(descriptor);
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

    private void onMerge(Iterable<WID> mergedWindows) {
      this.mergedWindows = Lists.newArrayList(mergedWindows);
      trigger.onMerge(window, this);
    }

    private void setWindow(WID window) {
      this.window = window;
    }
  }

  private class OutputCollector extends AbstractCollector {

    private Object key;
    private Window window;

    private final StreamRecord reuse = new StreamRecord<>(null);

    public OutputCollector(FlinkAccumulatorFactory accumulatorFactory,
                           Settings settings,
                           RuntimeContext flinkContext) {
      super(accumulatorFactory, settings, flinkContext);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void collect(Object elem) {
      long stamp = Math.min(timerService.currentWatermark(), window.maxTimestamp() - 1);
      output.collect(reuse.replace(
              new StreamingElement<>(window, Pair.of(key, elem)),
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
