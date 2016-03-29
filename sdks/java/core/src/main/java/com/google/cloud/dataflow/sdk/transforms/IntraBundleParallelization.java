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

package com.google.cloud.dataflow.sdk.transforms;

import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.util.WindowingInternals;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import org.joda.time.Instant;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provides multi-threading of {@link DoFn}s, using threaded execution to
 * process multiple elements concurrently within a bundle.
 *
 * <p>Note, that each Dataflow worker will already process multiple bundles
 * concurrently and usage of this class is meant only for cases where processing
 * elements from within a bundle is limited by blocking calls.
 *
 * <p>CPU intensive or IO intensive tasks are in general a poor fit for parallelization.
 * This is because a limited resource that is already maximally utilized does not
 * benefit from sub-division of work. The parallelization will increase the amount of time
 * to process each element yet the throughput for processing will remain relatively the same.
 * For example, if the local disk (an IO resource) has a maximum write rate of 10 MiB/s,
 * and processing each element requires to write 20 MiBs to disk, then processing one element
 * to disk will take 2 seconds. Yet processing 3 elements concurrently (each getting an equal
 * share of the maximum write rate) will take at least 6 seconds to complete (there is additional
 * overhead in the extra parallelization).
 *
 * <p>To parallelize a {@link DoFn} to 10 threads:
 * <pre>{@code
 * PCollection<T> data = ...;
 * data.apply(
 *   IntraBundleParallelization.of(new MyDoFn())
 *                             .withMaxParallelism(10)));
 * }</pre>
 *
 * <p>An uncaught exception from the wrapped {@link DoFn} will result in the exception
 * being rethrown in later calls to {@link MultiThreadedIntraBundleProcessingDoFn#processElement}
 * or a call to {@link MultiThreadedIntraBundleProcessingDoFn#finishBundle}.
 */
public class IntraBundleParallelization {
  /**
   * Creates a {@link IntraBundleParallelization} {@link PTransform} for the given
   * {@link DoFn} that processes elements using multiple threads.
   *
   * <p>Note that the specified {@code doFn} needs to be thread safe.
   */
  public static <InputT, OutputT> Bound<InputT, OutputT> of(DoFn<InputT, OutputT> doFn) {
    return new Unbound().of(doFn);
  }

  /**
   * Creates a {@link IntraBundleParallelization} {@link PTransform} with the specified
   * maximum concurrency level.
   */
  public static Unbound withMaxParallelism(int maxParallelism) {
    return new Unbound().withMaxParallelism(maxParallelism);
  }

  /**
   * An incomplete {@code IntraBundleParallelization} transform, with unbound input/output types.
   *
   * <p>Before being applied, {@link IntraBundleParallelization.Unbound#of} must be
   * invoked to specify the {@link DoFn} to invoke, which will also
   * bind the input/output types of this {@code PTransform}.
   */
  public static class Unbound {
    private final int maxParallelism;

    Unbound() {
      this(DEFAULT_MAX_PARALLELISM);
    }

    Unbound(int maxParallelism) {
      Preconditions.checkArgument(maxParallelism > 0,
          "Expected parallelism factor greater than zero, received %s.", maxParallelism);
      this.maxParallelism = maxParallelism;
    }

    /**
     * Returns a new {@link IntraBundleParallelization} {@link PTransform} like this one
     * with the specified maximum concurrency level.
     */
    public Unbound withMaxParallelism(int maxParallelism) {
      return new Unbound(maxParallelism);
    }

    /**
     * Returns a new {@link IntraBundleParallelization} {@link PTransform} like this one
     * with the specified {@link DoFn}.
     *
     * <p>Note that the specified {@code doFn} needs to be thread safe.
     */
    public <InputT, OutputT> Bound<InputT, OutputT> of(DoFn<InputT, OutputT> doFn) {
      return new Bound<>(doFn, maxParallelism);
    }
  }

  /**
   * A {@code PTransform} that, when applied to a {@code PCollection<InputT>},
   * invokes a user-specified {@code DoFn<InputT, OutputT>} on all its elements,
   * with all its outputs collected into an output
   * {@code PCollection<OutputT>}.
   *
   * <p>Note that the specified {@code doFn} needs to be thread safe.
   *
   * @param <InputT> the type of the (main) input {@code PCollection} elements
   * @param <OutputT> the type of the (main) output {@code PCollection} elements
   */
  public static class Bound<InputT, OutputT>
      extends PTransform<PCollection<? extends InputT>, PCollection<OutputT>> {
    private final DoFn<InputT, OutputT> doFn;
    private final int maxParallelism;

    Bound(DoFn<InputT, OutputT> doFn, int maxParallelism) {
      Preconditions.checkArgument(maxParallelism > 0,
          "Expected parallelism factor greater than zero, received %s.", maxParallelism);
      this.doFn = doFn;
      this.maxParallelism = maxParallelism;
    }

    /**
     * Returns a new {@link IntraBundleParallelization} {@link PTransform} like this one
     * with the specified maximum concurrency level.
     */
    public Bound<InputT, OutputT> withMaxParallelism(int maxParallelism) {
      return new Bound<>(doFn, maxParallelism);
    }

    /**
     * Returns a new {@link IntraBundleParallelization} {@link PTransform} like this one
     * with the specified {@link DoFn}.
     *
     * <p>Note that the specified {@code doFn} needs to be thread safe.
     */
    public <NewInputT, NewOutputT> Bound<NewInputT, NewOutputT>
        of(DoFn<NewInputT, NewOutputT> doFn) {
      return new Bound<>(doFn, maxParallelism);
    }

    @Override
    public PCollection<OutputT> apply(PCollection<? extends InputT> input) {
      return input.apply(
          ParDo.of(new MultiThreadedIntraBundleProcessingDoFn<>(doFn, maxParallelism)));
    }
  }

  /**
   * A multi-threaded {@code DoFn} wrapper.
   *
   * @see IntraBundleParallelization#of(DoFn)
   *
   * @param <InputT> the type of the (main) input elements
   * @param <OutputT> the type of the (main) output elements
   */
  public static class MultiThreadedIntraBundleProcessingDoFn<InputT, OutputT>
      extends DoFn<InputT, OutputT> {

    public MultiThreadedIntraBundleProcessingDoFn(DoFn<InputT, OutputT> doFn, int maxParallelism) {
      Preconditions.checkArgument(maxParallelism > 0,
          "Expected parallelism factor greater than zero, received %s.", maxParallelism);
      this.doFn = doFn;
      this.maxParallelism = maxParallelism;
    }

    @Override
    public void startBundle(Context c) throws Exception {
      doFn.startBundle(c);

      executor = c.getPipelineOptions().as(GcsOptions.class).getExecutorService();
      workTickets = new Semaphore(maxParallelism);
      failure = new AtomicReference<>();
    }

    @Override
    public void processElement(final ProcessContext c) throws Exception {
      try {
        workTickets.acquire();
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while scheduling work", e);
      }

      if (failure.get() != null) {
        throw Throwables.propagate(failure.get());
      }

      executor.submit(new Runnable() {
        @Override
        public void run() {
          try {
            doFn.processElement(new WrappedContext(c));
          } catch (Throwable t) {
            failure.compareAndSet(null, t);
            Throwables.propagateIfPossible(t);
            throw new AssertionError("Unexpected checked exception: " + t);
          } finally {
            workTickets.release();
          }
        }
      });
    }

    @Override
    public void finishBundle(Context c) throws Exception {
      // Acquire all the work tickets to guarantee that all the previous
      // processElement calls have finished.
      workTickets.acquire(maxParallelism);
      if (failure.get() != null) {
        throw Throwables.propagate(failure.get());
      }
      doFn.finishBundle(c);
    }

    @Override
    protected TypeDescriptor<InputT> getInputTypeDescriptor() {
      return doFn.getInputTypeDescriptor();
    }

    @Override
    protected TypeDescriptor<OutputT> getOutputTypeDescriptor() {
      return doFn.getOutputTypeDescriptor();
    }

    /////////////////////////////////////////////////////////////////////////////

    /**
     * Wraps a DoFn context, forcing single-thread output so that threads don't
     * propagate through to downstream functions.
     */
    private class WrappedContext extends ProcessContext {
      private final ProcessContext context;

      WrappedContext(ProcessContext context) {
        this.context = context;
      }

      @Override
      public InputT element() {
        return context.element();
      }

      @Override
      public PipelineOptions getPipelineOptions() {
        return context.getPipelineOptions();
      }

      @Override
      public <T> T sideInput(PCollectionView<T> view) {
        return context.sideInput(view);
      }

      @Override
      public void output(OutputT output) {
        synchronized (MultiThreadedIntraBundleProcessingDoFn.this) {
          context.output(output);
        }
      }

      @Override
      public void outputWithTimestamp(OutputT output, Instant timestamp) {
        synchronized (MultiThreadedIntraBundleProcessingDoFn.this) {
          context.outputWithTimestamp(output, timestamp);
        }
      }

      @Override
      public <T> void sideOutput(TupleTag<T> tag, T output) {
        synchronized (MultiThreadedIntraBundleProcessingDoFn.this) {
          context.sideOutput(tag, output);
        }
      }

      @Override
      public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
        synchronized (MultiThreadedIntraBundleProcessingDoFn.this) {
          context.sideOutputWithTimestamp(tag, output, timestamp);
        }
      }

      @Override
      public Instant timestamp() {
        return context.timestamp();
      }

      @Override
      public BoundedWindow window() {
        return context.window();
      }

      @Override
      public PaneInfo pane() {
        return context.pane();
      }

      @Override
      public WindowingInternals<InputT, OutputT> windowingInternals() {
        return context.windowingInternals();
      }

      @Override
      protected <AggInputT, AggOutputT> Aggregator<AggInputT, AggOutputT> createAggregatorInternal(
          String name, CombineFn<AggInputT, ?, AggOutputT> combiner) {
        return context.createAggregatorInternal(name, combiner);
      }
    }

    private final DoFn<InputT, OutputT> doFn;
    private int maxParallelism;

    private transient ExecutorService executor;
    private transient Semaphore workTickets;
    private transient AtomicReference<Throwable> failure;
  }

  /**
   * Default maximum for number of concurrent elements to process.
   */
  private static final int DEFAULT_MAX_PARALLELISM = 16;
}
