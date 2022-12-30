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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.DoFnRunners.OutputManager;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.AbstractIterator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.LinkedListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import scala.Tuple2;

/** Processes Spark partitions using Beam's {@link org.apache.beam.runners.core.DoFnRunner}. */
interface SparkInputDataProcessor<FnInputT, FnOutputT, OutputT> {

  /**
   * @return {@link OutputManager} to be used by {@link org.apache.beam.runners.core.DoFnRunner} for
   *     emitting processing results
   */
  OutputManager getOutputManager();

  /**
   * Processes input partition data and return results as {@link Iterable}.
   *
   * @param input input partition iterator
   * @param ctx current processing context
   */
  <K> Iterable<OutputT> process(
      Iterator<WindowedValue<FnInputT>> input, SparkProcessContext<K, FnInputT, FnOutputT> ctx);

  /**
   * Creates a synchronous {@link SparkInputDataProcessor} which does processing in calling thread.
   * It is doing so by processing fully input element and then iterating over the output retrieved
   * from that processing. The result of processing one element fit into memory.
   */
  static <FnInputT, FnOutputT>
      SparkInputDataProcessor<FnInputT, FnOutputT, Tuple2<TupleTag<?>, WindowedValue<?>>>
          createSync() {
    return new SyncSparkInputDataProcessor<>();
  }

  /**
   * Create and asynchronous {@link SparkInputDataProcessor} which does process input elements in
   * separate thread and observes produced outputs asynchronously. This does not require results of
   * processing one element to fit into the memory.
   */
  static <FnInputT, FnOutputT>
      SparkInputDataProcessor<FnInputT, FnOutputT, Tuple2<TupleTag<?>, WindowedValue<?>>>
          createAsync() {
    return new AsyncSparkInputDataProcessor<>();
  }
}

class SyncSparkInputDataProcessor<FnInputT, FnOutputT>
    implements SparkInputDataProcessor<FnInputT, FnOutputT, Tuple2<TupleTag<?>, WindowedValue<?>>> {

  private final UnboundedDoFnOutputManager outputManager = new UnboundedDoFnOutputManager();

  @Override
  public OutputManager getOutputManager() {
    return outputManager;
  }

  @Override
  public <K> Iterable<Tuple2<TupleTag<?>, WindowedValue<?>>> process(
      Iterator<WindowedValue<FnInputT>> input, SparkProcessContext<K, FnInputT, FnOutputT> ctx) {
    if (!input.hasNext()) {
      return Collections.emptyList();
    }
    return () -> new SyncInOutIterator<>(input, ctx);
  }

  private static class UnboundedDoFnOutputManager
      implements OutputManager, Iterable<Tuple2<TupleTag<?>, WindowedValue<?>>> {

    private final Multimap<TupleTag<?>, WindowedValue<?>> outputs = LinkedListMultimap.create();

    public void clear() {
      outputs.clear();
    }

    @Override
    public Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>> iterator() {
      Iterator<Map.Entry<TupleTag<?>, WindowedValue<?>>> entryIter = outputs.entries().iterator();
      return Iterators.transform(entryIter, this.entryToTupleFn());
    }

    private <K, V> Function<Map.Entry<K, V>, Tuple2<K, V>> entryToTupleFn() {
      return en -> {
        if (en == null) {
          return null;
        } else {
          return new Tuple2<>(en.getKey(), en.getValue());
        }
      };
    }

    @Override
    public synchronized <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      outputs.put(tag, output);
    }
  }

  private class SyncInOutIterator<K>
      extends AbstractIterator<Tuple2<TupleTag<?>, WindowedValue<?>>> {

    private final Iterator<WindowedValue<FnInputT>> inputIterator;
    private final SparkProcessContext<K, FnInputT, FnOutputT> ctx;
    private Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>> outputIterator;
    private boolean isBundleStarted;
    private boolean isBundleFinished;

    SyncInOutIterator(
        Iterator<WindowedValue<FnInputT>> iterator,
        SparkProcessContext<K, FnInputT, FnOutputT> ctx) {
      this.inputIterator = iterator;
      this.ctx = ctx;
      this.outputIterator = outputManager.iterator();
      ;
    }

    @Override
    protected Tuple2<TupleTag<?>, WindowedValue<?>> computeNext() {
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

class AsyncSparkInputDataProcessor<FnInputT, FnOutputT>
    implements SparkInputDataProcessor<FnInputT, FnOutputT, Tuple2<TupleTag<?>, WindowedValue<?>>> {

  private final BlockingDoFnOutputManager outputManager = new BlockingDoFnOutputManager();

  @Override
  public OutputManager getOutputManager() {
    return outputManager;
  }

  @Override
  public <K> Iterable<Tuple2<TupleTag<?>, WindowedValue<?>>> process(
      Iterator<WindowedValue<FnInputT>> input, SparkProcessContext<K, FnInputT, FnOutputT> ctx) {
    if (!input.hasNext()) {
      return Collections.emptyList();
    }
    return () -> new AsyncInOutIterator<>(input, ctx);
  }

  /**
   * Output manager which can hold limited number of output elements. If capacity is reached, then
   * attempt to output more elements will block until some elements are consumed.
   */
  private class BlockingDoFnOutputManager
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

        @Override
        public boolean hasNext() {
          // expect elements appearing in queue until stop() is invoked.
          // after that, no more inputs can arrive, so just drain the queue
          while (true) {
            if (queue.isEmpty()) {
              try {
                // Wait for a bit before checking again if more data is available
                Thread.sleep(20);
                if (stopped) {
                  return !queue.isEmpty();
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            } else {
              return true;
            }
          }
        }

        @Override
        public Tuple2<TupleTag<?>, WindowedValue<?>> next() {
          Tuple2<TupleTag<?>, WindowedValue<?>> poll;
          try {
            poll = queue.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
          return poll;
        }
      };
    }

    @Override
    public synchronized <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
      try {
        if (stopped) {
          throw new IllegalStateException("Output called on already stopped manager");
        }
        queue.put(new Tuple2<>(tag, output));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private class AsyncInOutIterator<K, InputT, OutputT>
      extends AbstractIterator<Tuple2<TupleTag<?>, WindowedValue<?>>> {

    private final SparkProcessContext<K, InputT, OutputT> ctx;
    private final Iterator<WindowedValue<InputT>> inputIterator;
    private final Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>> outputIterator;
    private final ExecutorService executorService =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat("async-in/out-iterator-%d")
                .setDaemon(true)
                .build());
    private @Nullable Future<?> consumeTask = null;

    private volatile @Nullable RuntimeException inputConsumeFailure = null;

    AsyncInOutIterator(
        Iterator<WindowedValue<InputT>> iterator, SparkProcessContext<K, InputT, OutputT> ctx) {
      this.inputIterator = iterator;
      this.ctx = ctx;
      this.outputIterator = outputManager.iterator();
    }

    @Override
    protected Tuple2<TupleTag<?>, WindowedValue<?>> computeNext() {

      if (consumeTask == null) {
        consumeTask =
            executorService.submit(
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

      while (true) {
        boolean hasNext = outputIterator.hasNext();
        RuntimeException failure = inputConsumeFailure;
        if (failure != null) {
          executorService.shutdown();
          throw failure;
        }

        if (hasNext) {
          return outputIterator.next();
        } else {
          executorService.shutdown();
          return endOfData();
        }
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
