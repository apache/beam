/*
 * Copyright (C) 2014 Google Inc.
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

import com.google.api.client.util.Throwables;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.RateLimiter;

import org.joda.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provides rate-limiting of user functions, using threaded execution and a
 * {@link com.google.common.util.concurrent.RateLimiter} to process elements
 * at the desired rate.
 *
 * <p> For example, to limit each worker to 10 requests per second:
 * <pre>{@code
 * PCollection<T> data = ...;
 * data.apply(
 *   RateLimiting.perWorker(new MyDoFn())
 *               .withRateLimit(10)));
 * }</pre>
 *
 * <p> An uncaught exception from the wrapped DoFn will result in the exception
 * being rethrown in later calls to {@link RateLimitingDoFn#processElement}
 * or a call to {@link RateLimitingDoFn#finishBundle}.
 *
 * <p> Rate limiting is provided as a PTransform
 * ({@link RateLimitingTransform}), and also as a {@code DoFn}
 * ({@link RateLimitingDoFn}).
 */
public class RateLimiting {

  /**
   * Creates a new per-worker rate-limiting transform for the given
   * {@link com.google.cloud.dataflow.sdk.transforms.DoFn}.
   *
   * <p> The default behavior is to process elements with multiple threads,
   * but no rate limit is applied.
   *
   * <p> Use {@link RateLimitingTransform#withRateLimit} to limit the processing
   * rate, and {@link RateLimitingTransform#withMaxParallelism} to control the
   * maximum concurrent processing limit.
   *
   * <p> Aside from the above, the {@code DoFn} will be executed in the same manner
   * as in {@link ParDo}.
   *
   * <p> Rate limiting is applied independently per-worker.
   */
  public static <I, O> RateLimitingTransform<I, O> perWorker(DoFn<I, O> doFn) {
    return new RateLimitingTransform<>(doFn);
  }

  /**
   * A {@link PTransform} which applies rate limiting to a {@link DoFn}.
   *
   * @param <I> the type of the (main) input elements
   * @param <O> the type of the (main) output elements
   */
  @SuppressWarnings("serial")
  public static class RateLimitingTransform<I, O>
      extends PTransform<PCollection<? extends I>, PCollection<O>> {
    private final DoFn<I, O> doFn;
    private double rate = 0.0;
    // TODO: set default based on num cores, or based on rate limit?
    private int maxParallelism = DEFAULT_MAX_PARALLELISM;

    public RateLimitingTransform(DoFn<I, O> doFn) {
      this.doFn = doFn;
    }

    /**
     * Modifies this {@code RateLimitingTransform}, specifying a maximum
     * per-worker element processing rate.
     *
     * <p> A rate of {@code N} corresponds to {@code N} elements per second.
     * This rate is on a per-worker basis, so the overall rate of the job
     * depends upon the number of workers.
     *
     * <p> This rate limit may not be reachable unless there is sufficient
     * parallelism.
     *
     * <p> A rate of <= 0.0 disables rate limiting.
     */
    public RateLimitingTransform<I, O> withRateLimit(
        double maxElementsPerSecond) {
      this.rate = maxElementsPerSecond;
      return this;
    }

    /**
     * Modifies this {@code RateLimitingTransform}, specifying a maximum
     * per-worker parallelism.
     *
     * <p> This determines how many concurrent elements will be processed by the
     * wrapped {@code DoFn}.
     *
     * <p> The desired amount of parallelism depends upon the type of work.  For
     * CPU-intensive work, a good starting point is to use the number of cores:
     * {@code Runtime.getRuntime().availableProcessors()}.
     */
    public RateLimitingTransform<I, O> withMaxParallelism(int max) {
      this.maxParallelism = max;
      return this;
    }

    @Override
    public PCollection<O> apply(PCollection<? extends I> input) {
      return input.apply(
          ParDo.of(new RateLimitingDoFn<>(doFn, rate, maxParallelism)));
    }
  }

  /**
   * A rate-limiting {@code DoFn} wrapper.
   *
   * @see RateLimiting#perWorker(DoFn)
   *
   * @param <I> the type of the (main) input elements
   * @param <O> the type of the (main) output elements
   */
  public static class RateLimitingDoFn<I, O> extends DoFn<I, O> {
    private static final Logger LOG = LoggerFactory.getLogger(RateLimitingDoFn.class);

    public RateLimitingDoFn(DoFn<I, O> doFn, double rateLimit,
        int maxParallelism) {
      this.doFn = doFn;
      this.rate = rateLimit;
      this.maxParallelism = maxParallelism;
    }

    @Override
    public void startBundle(Context c) throws Exception {
      doFn.startBundle(c);

      if (rate > 0.0) {
        limiter = RateLimiter.create(rate);
      }
      executor = Executors.newCachedThreadPool();
      workTickets = new Semaphore(maxParallelism);
      failure = new AtomicReference<>();
    }

    @Override
    public void processElement(final ProcessContext c) throws Exception {
      // Apply rate limiting up front, controlling the availability of work for
      // the thread pool.  This allows us to use an auto-scaling thread pool,
      // which adapts the parallelism to the available work.
      // The semaphore is used to avoid overwhelming the executor, by bounding
      // the number of outstanding elements.
      if (limiter != null) {
        limiter.acquire();
      }
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
      executor.shutdown();
      // Log a periodic progress report until the queue has drained.
      while (true) {
        try {
          if (executor.awaitTermination(30, TimeUnit.SECONDS)) {
            if (failure.get() != null) {
              // Handle failure propagation outside of the try/catch block.
              break;
            }
            doFn.finishBundle(c);
            return;
          }
          int outstanding = workTickets.getQueueLength()
              + maxParallelism - workTickets.availablePermits();
          LOG.info("RateLimitingDoFn backlog: {}", outstanding);
        } catch (InterruptedException e) {
          throw Throwables.propagate(e);
        }
      }

      throw Throwables.propagate(failure.get());
    }

    @Override
    TypeToken<I> getInputTypeToken() {
      return doFn.getInputTypeToken();
    }

    @Override
    TypeToken<O> getOutputTypeToken() {
      return doFn.getOutputTypeToken();
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
      public I element() {
        return context.element();
      }

      @Override
      public KeyedState keyedState() {
        return context.keyedState();
      }

      @Override
      public PipelineOptions getPipelineOptions() {
        return context.getPipelineOptions();
      }

      @Override
      public <T> T sideInput(PCollectionView<T, ?> view) {
        return context.sideInput(view);
      }

      @Override
      public void output(O output) {
        synchronized (RateLimitingDoFn.this) {
          context.output(output);
        }
      }

      @Override
      public void outputWithTimestamp(O output, Instant timestamp) {
        synchronized (RateLimitingDoFn.this) {
          context.outputWithTimestamp(output, timestamp);
        }
      }

      @Override
      public <T> void sideOutput(TupleTag<T> tag, T output) {
        synchronized (RateLimitingDoFn.this) {
          context.sideOutput(tag, output);
        }
      }

      @Override
      public <T> void sideOutputWithTimestamp(TupleTag<T> tag, T output, Instant timestamp) {
        synchronized (RateLimitingDoFn.this) {
          context.sideOutputWithTimestamp(tag, output, timestamp);
        }
      }

      @Override
      public <AI, AA, AO> Aggregator<AI> createAggregator(
          String name, Combine.CombineFn<? super AI, AA, AO> combiner) {
        return context.createAggregator(name, combiner);
      }

      @Override
      public <AI, AO> Aggregator<AI> createAggregator(
          String name, SerializableFunction<Iterable<AI>, AO> combiner) {
        return context.createAggregator(name, combiner);
      }

      @Override
      public Instant timestamp() {
        return context.timestamp();
      }

      @Override
      public Collection<? extends BoundedWindow> windows() {
        return context.windows();
      }
    }

    private final DoFn<I, O> doFn;
    private double rate;
    private int maxParallelism;

    private transient RateLimiter limiter;
    private transient ExecutorService executor;
    private transient Semaphore workTickets;
    private transient AtomicReference<Throwable> failure;
  }

  /**
   * Default maximum for number of concurrent elements to process.
   */
  @VisibleForTesting
  static final int DEFAULT_MAX_PARALLELISM = 16;
}
