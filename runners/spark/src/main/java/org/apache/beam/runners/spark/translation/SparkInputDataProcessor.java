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
package org.apache.beam.runners.spark.translation;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.CheckForNull;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.AbstractIterator;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import scala.Tuple2;

/**
 * Processes Spark's input data iterators using Beam's {@link
 * org.apache.beam.runners.core.DoFnRunner}.
 */
public interface SparkInputDataProcessor<FnInputT, FnOutputT, OutputT> {

  /**
   * @return {@link OutputManager} to be used by {@link org.apache.beam.runners.core.DoFnRunner} for
   *     emitting processing results
   */
  OutputManager getOutputManager();

  /**
   * Creates a transformation which processes input partition data and returns output results as
   * {@link Iterator}.
   *
   * @param input input partition iterator
   * @param ctx current processing context
   */
  <K> Iterator<OutputT> createOutputIterator(
      Iterator<WindowedValue<FnInputT>> input, SparkProcessContext<K, FnInputT, FnOutputT> ctx);

  /**
   * Creates {@link SparkInputDataProcessor} which does processing in calling thread. It is doing so
   * by processing input element completely and then iterating over the output retrieved from that
   * processing. The result of processing one element must fit into memory.
   */
  static <FnInputT, FnOutputT>
      SparkInputDataProcessor<FnInputT, FnOutputT, Tuple2<TupleTag<?>, WindowedValue<?>>>
          createUnbounded() {
    return new UnboundedSparkInputDataProcessor<>();
  }

  /**
   * Creates {@link SparkInputDataProcessor} which does process input elements in separate thread
   * and observes produced outputs via bounded queue in other thread. This does not require results
   * of processing one element to fit into the memory.
   */
  static <FnInputT, FnOutputT>
      SparkInputDataProcessor<FnInputT, FnOutputT, Tuple2<TupleTag<?>, WindowedValue<?>>>
          createBounded() {
    return new BoundedSparkInputDataProcessor<>();
  }
}

class UnboundedSparkInputDataProcessor<FnInputT, FnOutputT>
    implements SparkInputDataProcessor<FnInputT, FnOutputT, Tuple2<TupleTag<?>, WindowedValue<?>>> {

  private final UnboundedDoFnOutputManager outputManager = new UnboundedDoFnOutputManager();

  @Override
  public OutputManager getOutputManager() {
    return outputManager;
  }

  @Override
  public <K> Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>> createOutputIterator(
      Iterator<WindowedValue<FnInputT>> input, SparkProcessContext<K, FnInputT, FnOutputT> ctx) {
    return new UnboundedInOutIterator<>(input, ctx);
  }

  private static class UnboundedDoFnOutputManager
      implements OutputManager, Iterable<Tuple2<TupleTag<?>, WindowedValue<?>>> {

    private final ArrayDeque<Tuple2<TupleTag<?>, WindowedValue<?>>> outputs = new ArrayDeque<>();

    public void clear() {
      outputs.clear();
    }

    @Override
    public Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>> iterator() {
      return outputs.iterator();
    }

    @Override
    public synchronized <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      outputs.addLast(new Tuple2<>(tag, output));
    }
  }

  private class UnboundedInOutIterator<K>
      extends AbstractIterator<Tuple2<TupleTag<?>, WindowedValue<?>>> {

    private final Iterator<WindowedValue<FnInputT>> inputIterator;
    private final SparkProcessContext<K, FnInputT, FnOutputT> ctx;
    private Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>> outputIterator;
    private boolean isBundleStarted;
    private boolean isBundleFinished;

    UnboundedInOutIterator(
        Iterator<WindowedValue<FnInputT>> iterator,
        SparkProcessContext<K, FnInputT, FnOutputT> ctx) {
      this.inputIterator = iterator;
      this.ctx = ctx;
      this.outputIterator = outputManager.iterator();
    }

    @Override
    protected @CheckForNull Tuple2<TupleTag<?>, WindowedValue<?>> computeNext() {
      try {
        // Process each element from the (input) iterator, which produces, zero, one or more
        // output elements (of type V) in the output iterator. Note that the output
        // collection (and iterator) is reset between each call to processElement, so the
        // collection only holds the output values for each call to processElement, rather
        // than for the whole partition (which would use too much memory).
        if (!isBundleStarted) {
          isBundleStarted = true;
          // call startBundle() before beginning to process the partition.
          ctx.getDoFnRunner().startBundle();
        }

        while (true) {
          if (outputIterator.hasNext()) {
            return outputIterator.next();
          }

          outputManager.clear();
          if (inputIterator.hasNext()) {
            // grab the next element and process it.
            ctx.getDoFnRunner().processElement(inputIterator.next());
            outputIterator = outputManager.iterator();
          } else if (ctx.getTimerDataIterator().hasNext()) {
            fireTimer(ctx.getTimerDataIterator().next());
            outputIterator = outputManager.iterator();
          } else {
            // no more input to consume, but finishBundle can produce more output
            if (!isBundleFinished) {
              isBundleFinished = true;
              ctx.getDoFnRunner().finishBundle();
              outputIterator = outputManager.iterator();
              continue; // try to consume outputIterator from start of loop
            }
            DoFnInvokers.invokerFor(ctx.getDoFn()).invokeTeardown();
            return endOfData();
          }
        }
      } catch (final RuntimeException re) {
        DoFnInvokers.invokerFor(ctx.getDoFn()).invokeTeardown();
        throw re;
      }
    }

    private void fireTimer(TimerInternals.TimerData timer) {
      StateNamespace namespace = timer.getNamespace();
      checkArgument(namespace instanceof StateNamespaces.WindowNamespace);
      BoundedWindow window = ((StateNamespaces.WindowNamespace) namespace).getWindow();
      ctx.getDoFnRunner()
          .onTimer(
              timer.getTimerId(),
              timer.getTimerFamilyId(),
              ctx.getKey(),
              window,
              timer.getTimestamp(),
              timer.getOutputTimestamp(),
              timer.getDomain());
    }
  }
}

class BoundedSparkInputDataProcessor<FnInputT, FnOutputT>
    implements SparkInputDataProcessor<FnInputT, FnOutputT, Tuple2<TupleTag<?>, WindowedValue<?>>> {

  private final BoundedDoFnOutputManager outputManager = new BoundedDoFnOutputManager();

  @Override
  public OutputManager getOutputManager() {
    return outputManager;
  }

  @Override
  public <K> Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>> createOutputIterator(
      Iterator<WindowedValue<FnInputT>> input, SparkProcessContext<K, FnInputT, FnOutputT> ctx) {
    return new BoundedInOutIterator<>(input, ctx);
  }

  /**
   * Output manager which can hold limited number of output elements. If capacity is reached, then
   * attempt to output more elements will block until some elements are consumed.
   */
  private static class BoundedDoFnOutputManager
      implements OutputManager, Iterable<Tuple2<TupleTag<?>, WindowedValue<?>>> {

    private final LinkedBlockingQueue<Tuple2<TupleTag<?>, WindowedValue<?>>> queue =
        new LinkedBlockingQueue<>(500);
    private volatile boolean stopped = false;

    public void stop() {
      stopped = true;
    }

    @Override
    public Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>> iterator() {
      return new Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>>() {

        private @Nullable Tuple2<TupleTag<?>, WindowedValue<?>> next = null;

        @Override
        public boolean hasNext() {
          // expect elements appearing in queue until stop() is invoked.
          // after that, no more inputs can arrive, so just drain the queue
          try {
            while (next == null && !(stopped && queue.isEmpty())) {
              next = queue.poll(1000, TimeUnit.MILLISECONDS);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          }
          return next != null;
        }

        @Override
        @SuppressWarnings({"nullness"})
        public Tuple2<TupleTag<?>, WindowedValue<?>> next() {
          if (next == null && !hasNext()) {
            throw new NoSuchElementException();
          }
          Tuple2<TupleTag<?>, WindowedValue<?>> value = next;
          next = null;
          return value;
        }
      };
    }

    @Override
    public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      try {
        Preconditions.checkState(!stopped, "Output called on already stopped manager");
        queue.put(new Tuple2<>(tag, output));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }
  }

  private class BoundedInOutIterator<K, InputT, OutputT>
      extends AbstractIterator<Tuple2<TupleTag<?>, WindowedValue<?>>> {

    private final SparkProcessContext<K, InputT, OutputT> ctx;
    private final Iterator<WindowedValue<InputT>> inputIterator;
    private final Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>> outputIterator;
    private final ExecutorService executorService;
    private @Nullable Future<?> outputProducerTask = null;

    private volatile @MonotonicNonNull RuntimeException inputConsumeFailure = null;

    BoundedInOutIterator(
        Iterator<WindowedValue<InputT>> iterator, SparkProcessContext<K, InputT, OutputT> ctx) {
      this.inputIterator = iterator;
      this.ctx = ctx;
      this.outputIterator = outputManager.iterator();
      this.executorService =
          Executors.newSingleThreadScheduledExecutor(
              new ThreadFactoryBuilder()
                  .setNameFormat("bounded-in/out-iterator-" + ctx.getStepName() + "-%d")
                  .setDaemon(true)
                  .build());
    }

    @Override
    protected @CheckForNull Tuple2<TupleTag<?>, WindowedValue<?>> computeNext() {

      if (outputProducerTask == null) {
        outputProducerTask = startOutputProducerTask();
      }

      boolean hasNext = outputIterator.hasNext();
      if (inputConsumeFailure != null) {
        executorService.shutdown();
        throw inputConsumeFailure;
      }

      if (hasNext) {
        return outputIterator.next();
      } else {
        executorService.shutdown();
        return endOfData();
      }
    }

    private void fireTimer(TimerInternals.TimerData timer) {
      StateNamespace namespace = timer.getNamespace();
      checkArgument(namespace instanceof StateNamespaces.WindowNamespace);
      BoundedWindow window = ((StateNamespaces.WindowNamespace) namespace).getWindow();
      ctx.getDoFnRunner()
          .onTimer(
              timer.getTimerId(),
              timer.getTimerFamilyId(),
              ctx.getKey(),
              window,
              timer.getTimestamp(),
              timer.getOutputTimestamp(),
              timer.getDomain());
    }

    private Future<?> startOutputProducerTask() {
      return executorService.submit(
          () -> {
            try {
              ctx.getDoFnRunner().startBundle();
              while (true) {
                if (inputIterator.hasNext()) {
                  // grab the next element and process it.
                  WindowedValue<InputT> next = inputIterator.next();
                  ctx.getDoFnRunner().processElement(next);

                } else if (ctx.getTimerDataIterator().hasNext()) {
                  fireTimer(ctx.getTimerDataIterator().next());
                } else {

                  // no more input to consume, but finishBundle can produce more output
                  ctx.getDoFnRunner().finishBundle();
                  DoFnInvokers.invokerFor(ctx.getDoFn()).invokeTeardown();

                  outputManager.stop();
                  break;
                }
              }

            } catch (RuntimeException ex) {
              inputConsumeFailure = ex;
              DoFnInvokers.invokerFor(ctx.getDoFn()).invokeTeardown();
              outputManager.stop();
            }
          });
    }
  }
}
