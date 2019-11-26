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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.beam.runners.core.metrics.ExecutionStateSampler;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ElementExecutionTracker;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.PeekingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dataflow-specific implementation of {@link ElementExecutionTracker}.
 *
 * <p>Each processed element through each step in the fused stage gets tracked as a {@code
 * ElementExecution}. Processing time is sampled using {@link ExecutionStateSampler} and distributed
 * amongst all elements which are executed since the last sample period.
 *
 * <p>Elements are processed in potentially many fragments of execution as we move up and down the
 * stage graph via outputting. Each fragment of execution is counted equally for attributing sampled
 * processing time.
 *
 * <p>When an element is finished processing it is held in the {@code doneJournal} collection until
 * the next sampling round in order to calculate the final processing time. Eventually the total
 * element processing time is reported to the counter and the state is cleaned up.
 *
 * <p>This is not implemented in {@link ElementExecutionTracker} itself because it uses
 * Beam/Dataflow-specific classes, such as {@link PipelineOptions} which cannot be shared with
 * Flume.
 */
public class DataflowElementExecutionTracker extends ElementExecutionTracker {
  @VisibleForTesting
  public static final String TIME_PER_ELEMENT_EXPERIMENT = "time_per_element_counter";

  private static final Logger LOG = LoggerFactory.getLogger(DataflowElementExecutionTracker.class);

  public static ElementExecutionTracker create(
      CounterFactory counterFactory, PipelineOptions options) {
    // TODO: Remove once feature has launched.
    if (!hasExperiment(options, TIME_PER_ELEMENT_EXPERIMENT)) {
      return ElementExecutionTracker.NO_OP_INSTANCE;
    }

    // TODO: Remove log statement when functionality is enabled by default.
    LOG.info("{} counter enabled.", ElementExecutionTracker.COUNTER_NAME);

    return new DataflowElementExecutionTracker(counterFactory);
  }

  private static boolean hasExperiment(PipelineOptions options, String experiment) {
    List<String> experiments = options.as(DataflowPipelineDebugOptions.class).getExperiments();
    return experiments != null && experiments.contains(experiment);
  }

  /**
   * Tracking object for the execution of a single input element in a step.
   *
   * <p>Each {@link ElementExecution} instance represents a distinct element, implicitly represented
   * by the instance identity. As such, {@link ElementExecution} instances compare using reference
   * equality rather than value equality.
   */
  private static class ElementExecution {
    /** Marker execution to represent when there is no element currently being processed. */
    static final ElementExecution IDLE = new ElementExecution();

    /** Only empty for {@see IDLE}. */
    final Optional<NameContext> step;

    ElementExecution(NameContext step) {
      this.step = Optional.of(step);
    }

    /** Only used for {@see IDLE}. */
    private ElementExecution() {
      step = Optional.empty();
    }

    @Override
    public String toString() {
      if (this == ElementExecution.IDLE) {
        return "IDLE_EXECUTION";
      }

      return MoreObjects.toStringHelper(this)
          .add("step", step)
          .add("executionId", System.identityHashCode(this))
          .toString();
    }
  }

  private final ExecutionJournalWriter executionWriter;
  private final ExecutionJournalReader executionReader;

  private DataflowElementExecutionTracker(CounterFactory counterFactory) {
    ReaderWriterState readerWriterState = new ReaderWriterState();
    executionWriter = new ExecutionJournalWriter(readerWriterState);
    executionReader = new ExecutionJournalReader(counterFactory, readerWriterState);
  }

  @Override
  public void enter(NameContext step) {
    executionWriter.startProcessing(step);
  }

  @Override
  public void exit() {
    executionWriter.doneProcessing();
  }

  @Override
  public void takeSample(long millisSinceLastSample) {
    executionReader.takeSample(Duration.ofMillis(millisSinceLastSample));
  }

  /** State shared between the {@link ExecutionJournalWriter} and {@link ExecutionJournalReader}. */
  private static class ReaderWriterState {
    /**
     * Journal of fragments of execution per element to count for attributing processing time. Each
     * time we transition up or down the stage fusion graph we add an execution fragment for the
     * currently processing element with an incremented snapshot version. Each snapshot version must
     * have a representative value in the {@code executionJournal}, or {@see IDLE_EXECUTION} to
     * represent completion of processing.
     */
    private final Journal<ElementExecution> executionJournal;

    /**
     * Elements which have completed processing but are still pending final timing calculation. The
     * elements are moved here when they exit the {@code executionStack} and are held here until the
     * counter value can be reported in the next sampling round.
     */
    private final Journal<ElementExecution> doneJournal;

    /**
     * Monotonically increasing snapshot version number which tracks the latest execution state
     * ready to be reported. Snapshot versions are associated with values in the {@code
     * executionJournal} and {@code doneJournal}.
     *
     * <p>This value written by the single {@link ExecutionJournalWriter} thread, and read by the
     * {@link ExecutionJournalReader} thread.
     */
    private volatile long latestSnapshot;

    private ReaderWriterState() {
      executionJournal = new Journal<>();
      doneJournal = new Journal<>();
      latestSnapshot = 0L;
    }
  }

  /**
   * Writes journal entries on element processing state changes.
   *
   * <p>Write operations are executed by a single thread inside the stage operation hot loop and
   * must be highly performant. {@link ExecutionJournalWriter} methods are thread-safe only for
   * state shared with the {@link ExecutionJournalReader} thread.
   */
  private static class ExecutionJournalWriter {
    /**
     * Execution stack of processing elements. Elements are pushed onto the stack when we enter the
     * process() function and popped on process() return. This stack mirrors the actual Java runtime
     * stack and contains the step + element context in order to attribute sampled execution time.
     */
    private final Deque<ElementExecution> executionStack;

    private final ReaderWriterState sharedState;

    public ExecutionJournalWriter(ReaderWriterState sharedState) {
      this.sharedState = sharedState;
      this.executionStack = new ArrayDeque<>();

      addExecution(ElementExecution.IDLE);
    }

    /** Create and journal a new {@link ElementExecution} to track a processing element. */
    public void startProcessing(NameContext step) {
      ElementExecution execution = new ElementExecution(step);
      addExecution(execution);
    }

    private void addExecution(ElementExecution execution) {
      long nextSnapshot = sharedState.latestSnapshot + 1;
      executionStack.addLast(execution);
      sharedState.executionJournal.add(execution, nextSnapshot);
      sharedState.latestSnapshot = nextSnapshot;
    }

    /**
     * Indicates that the execution thread has exited the process method for an element.
     *
     * <p>When an element is finished processing, it is popped from the execution stack, but will be
     * tracked in the {@code doneJournal} collection until the next sampling round in order to
     * account timing for the final fragment of execution.
     */
    public void doneProcessing() {
      // The execution stack in initialized with the IDLE execution which should never be removed.
      checkState(executionStack.size() > 1, "No processing elements currently tracked.");

      ElementExecution execution = executionStack.removeLast();
      long nextSnapshot = sharedState.latestSnapshot + 1;
      sharedState.doneJournal.add(execution, nextSnapshot);
      ElementExecution nextElement = executionStack.getLast();
      sharedState.executionJournal.add(nextElement, nextSnapshot);
      sharedState.latestSnapshot = nextSnapshot;
    }
  }

  /**
   * Accounts sampled time to processed elements based on execution journal entries.
   *
   * <p>Read operations are executed by a single thread in charge of state sampling, and are
   * generally called more infrequently than {@link ExecutionJournalWriter} operations. {@link
   * ExecutionJournalReader} methods are thread-safe only for state shared with the {@link
   * ExecutionJournalWriter} thread.
   */
  private static class ExecutionJournalReader {
    /**
     * Accumulated execution time per element. Once an element has finished processing and execution
     * time has been attributed, the total execution time is reported via the counter and removed
     * from the collection.
     */
    private final Map<ElementExecution, Duration> executionTimes;

    private final CounterFactory counterFactory;
    private final ReaderWriterState sharedState;

    /** Cache of per-step distribution counters. */
    private final Map<NameContext, Counter<Long, ?>> counterCache;

    public ExecutionJournalReader(CounterFactory counterFactory, ReaderWriterState sharedState) {
      this.sharedState = sharedState;
      executionTimes = new HashMap<>();
      this.counterFactory = counterFactory;
      counterCache = new HashMap<>();
    }

    /**
     * Account the specified processing time duration to elements which have processed since the
     * last sampling round, and report counters for completed elements.
     */
    public void takeSample(Duration sampleTime) {
      long snapshot = sharedState.latestSnapshot;
      attributeProcessingTime(sampleTime, snapshot);

      // TODO: If possible, this should report tentative counter values before they are
      // finalized. Currently, root steps in a stage fusion will not have counters reported until an
      // element is completely processed through the stage subgraph.
      reportCounters(snapshot);
      pruneJournals(snapshot);
    }

    /**
     * Attribute processing time to elements from {@code executionJournal} up to the specified
     * snapshot.
     */
    private void attributeProcessingTime(Duration duration, long snapshot) {
      // TODO: This algorithm is used to compute "per-element-processing-time" counter
      // values, but a slightly different algorithm is used for msec counters. Values for both
      // counters should be derived from the same algorithm to avoid unexpected discrepancies.

      // Calculate total execution counts
      int totalExecutions = 0;
      Map<ElementExecution, Integer> executionsPerElement = new HashMap<>();
      for (ElementExecution execution : sharedState.executionJournal.readUntil(snapshot)) {
        totalExecutions++;
        if (execution != ElementExecution.IDLE) {
          executionsPerElement.compute(execution, (unused, count) -> count == null ? 1 : count + 1);
        }
      }

      // Attribute processing time
      final int totalExecutionsFinal = totalExecutions;
      for (Map.Entry<ElementExecution, Integer> executionCount : executionsPerElement.entrySet()) {
        executionTimes.compute(
            executionCount.getKey(),
            (unused, total) -> {
              int numExecutions = executionCount.getValue();
              Duration attributedSampleTime =
                  duration.dividedBy(totalExecutionsFinal).multipliedBy(numExecutions);
              return total == null ? attributedSampleTime : total.plus(attributedSampleTime);
            });
      }
    }

    /** Report counter values for done elements up to the given snapshot. */
    private void reportCounters(long snapshot) {
      // Report counter values for completed elements
      for (ElementExecution execution : sharedState.doneJournal.readUntil(snapshot)) {
        checkState(
            execution.step.isPresent(),
            "Unexpected execution in doneJournal with empty step: %s",
            execution);
        Counter<Long, ?> counter = getCounter(execution.step.get());
        counter.addValue(executionTimes.get(execution).toMillis());
      }
    }

    /** Retrieve the per-element processing time counter for the specified step. */
    private Counter<Long, ?> getCounter(NameContext step) {
      return counterCache.computeIfAbsent(
          step,
          s ->
              counterFactory.distribution(
                  ElementExecutionTracker.COUNTER_NAME.withOriginalName(s)));
    }

    /**
     * Prune journal entries which have been accounted.
     *
     * @param snapshot Snapshot value which has been accounted.
     */
    private void pruneJournals(long snapshot) {
      sharedState.doneJournal.pruneUntil(snapshot);

      // Keep the currently executing element in the journal so its remaining execution time
      // is counted in the next sampling round.
      sharedState.executionJournal.pruneUntil(snapshot - 1);
    }
  }

  /**
   * Concurrent queue-based data structure for passing journaled events between a single journal
   * reader and single journal writer.
   *
   * <p>Each event is journaled with an externally-managed snapshot version. Snapshot versions are
   * unique and monotonically increasing.
   */
  private static class Journal<T> {
    private final ConcurrentLinkedQueue<SnapshottedItem<T>> queue;
    private long maxSnapshot;

    public Journal() {
      queue = new ConcurrentLinkedQueue<>();
      maxSnapshot = Long.MIN_VALUE;
    }

    /** Add a new event to the journal with the given snapshot. */
    public void add(T item, long snapshot) {
      if (snapshot <= maxSnapshot) {
        throw new IllegalArgumentException(
            String.format(
                "Timestamps must be monotonically increasing. "
                    + "Specified snapshot '%d' is not greater than max snapshot '%d'",
                snapshot, maxSnapshot));
      }
      queue.add(new SnapshottedItem<>(item, snapshot));
      maxSnapshot = snapshot;
    }

    /**
     * Retrieve an iterable of events from the journal up to and including the specified snapshot
     * version.
     *
     * <p>Returns an empty iterable if the journal is empty or there are no events less than the
     * specified snapshot version.
     *
     * <p>The returned iterable returns a view of the queue at the time of construction, consistent
     * with {@link ConcurrentLinkedQueue#iterator()}.
     */
    public Iterable<T> readUntil(long snapshot) {
      // Implement our own iterator in order to short-circuit early once we've seen a snapshot
      // version greater than the value specified.
      // Streams will have takeWhile() in JDK9, and Guava may some day have
      // Iterables.limit(Predicate<T>): https://github.com/google/guava/issues/477
      PeekingIterator<SnapshottedItem<T>> iter = Iterators.peekingIterator(queue.iterator());
      return () ->
          new Iterator<T>() {
            @Override
            public boolean hasNext() {
              return iter.hasNext() && iter.peek().snapshot <= snapshot;
            }

            @Override
            public T next() {
              SnapshottedItem<T> next = iter.next();
              checkValidSnapshot(next);
              return next.item;
            }

            @Override
            public void remove() {
              iter.remove();
            }

            private void checkValidSnapshot(SnapshottedItem<T> next) {
              if (next.snapshot > snapshot) {
                throw new NoSuchElementException();
              }
            }
          };
    }

    /** Prune events from the journal up to and including the specified snapshot. */
    public void pruneUntil(long snapshot) {
      Iterator<SnapshottedItem<T>> iterator = queue.iterator();
      while (iterator.hasNext()) {
        if (iterator.next().snapshot <= snapshot) {
          iterator.remove();
        } else {
          break;
        }
      }
    }

    private static class SnapshottedItem<E> {
      private final E item;
      private final long snapshot;

      public SnapshottedItem(E item, long snapshot) {
        this.item = item;
        this.snapshot = snapshot;
      }
    }
  }
}
