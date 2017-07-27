package org.apache.beam.runners.mapreduce.translation;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Iterables;
import java.util.Collection;
import java.util.List;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.NullSideInputReader;
import org.apache.beam.runners.core.OutputWindowedValue;
import org.apache.beam.runners.core.ReduceFnRunner;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.construction.TriggerTranslation;
import org.apache.beam.runners.core.triggers.ExecutableTriggerStateMachine;
import org.apache.beam.runners.core.triggers.TriggerStateMachines;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Instant;

/**
 * The default batch implementation, if no specialized "fast path" implementation is applicable.
 */
public class GroupAlsoByWindowsViaOutputBufferDoFn<K, InputT, OutputT, W extends BoundedWindow>
    extends DoFn<KV<K, Iterable<WindowedValue<InputT>>>, KV<K, OutputT>> {

  private final WindowingStrategy<Object, W> windowingStrategy;
  private final SystemReduceFn<K, InputT, ?, OutputT, W> reduceFn;
  private final TupleTag<KV<K, OutputT>> mainTag;
  private transient DoFnRunners.OutputManager outputManager;

  public GroupAlsoByWindowsViaOutputBufferDoFn(
      WindowingStrategy<Object, W> windowingStrategy,
      SystemReduceFn<K, InputT, ?, OutputT, W> reduceFn,
      TupleTag<KV<K, OutputT>> mainTag,
      DoFnRunners.OutputManager outputManager) {
    this.windowingStrategy = checkNotNull(windowingStrategy, "windowingStrategy");
    this.reduceFn = checkNotNull(reduceFn, "reduceFn");
    this.mainTag = checkNotNull(mainTag, "mainTag");
    this.outputManager = checkNotNull(outputManager, "outputManager");
  }

  @ProcessElement
  public void processElement(ProcessContext c) throws Exception {
    K key = c.element().getKey();
    // Used with Batch, we know that all the data is available for this key. We can't use the
    // timer manager from the context because it doesn't exist. So we create one and emulate the
    // watermark, knowing that we have all data and it is in timestamp order.
    InMemoryTimerInternals timerInternals = new InMemoryTimerInternals();
    ReduceFnRunner<K, InputT, OutputT, W> runner = new ReduceFnRunner<>(
        key,
        windowingStrategy,
        ExecutableTriggerStateMachine.create(
            TriggerStateMachines.stateMachineForTrigger(
                TriggerTranslation.toProto(windowingStrategy.getTrigger()))),
        InMemoryStateInternals.forKey(key),
        timerInternals,
        outputWindowedValue(),
        NullSideInputReader.empty(),
        reduceFn,
        c.getPipelineOptions());

    Iterable<List<WindowedValue<InputT>>> chunks =
        Iterables.partition(c.element().getValue(), 1000);
    for (Iterable<WindowedValue<InputT>> chunk : chunks) {
      // Process the chunk of elements.
      runner.processElements(chunk);

      // Then, since elements are sorted by their timestamp, advance the input watermark
      // to the first element, and fire any timers that may have been scheduled.
      // TODO: re-enable once elements are sorted.
      // timerInternals.advanceInputWatermark(chunk.iterator().next().getTimestamp());

      // Fire any processing timers that need to fire
      timerInternals.advanceProcessingTime(Instant.now());

      // Leave the output watermark undefined. Since there's no late data in batch mode
      // there's really no need to track it as we do for streaming.
    }

    // Finish any pending windows by advancing the input watermark to infinity.
    timerInternals.advanceInputWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE);

    // Finally, advance the processing time to infinity to fire any timers.
    timerInternals.advanceProcessingTime(BoundedWindow.TIMESTAMP_MAX_VALUE);

    runner.onTimers(timerInternals.getTimers(TimeDomain.EVENT_TIME));
    runner.onTimers(timerInternals.getTimers(TimeDomain.PROCESSING_TIME));
    runner.onTimers(timerInternals.getTimers(TimeDomain.SYNCHRONIZED_PROCESSING_TIME));

    runner.persist();
  }

  private OutputWindowedValue<KV<K, OutputT>> outputWindowedValue() {
    return new OutputWindowedValue<KV<K, OutputT>>() {
      @Override
      public void outputWindowedValue(
          KV<K, OutputT> output,
          Instant timestamp,
          Collection<? extends BoundedWindow> windows,
          PaneInfo pane) {
        outputManager.output(mainTag,
            WindowedValue.of(output, timestamp, windows, pane));
      }

      @Override
      public <AdditionalOutputT> void outputWindowedValue(
          TupleTag<AdditionalOutputT> tag,
          AdditionalOutputT output,
          Instant timestamp,
          Collection<? extends BoundedWindow> windows,
          PaneInfo pane) {
        outputManager.output(tag,
            WindowedValue.of(output, timestamp, windows, pane));
      }
    };
  }
}
