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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.scalaIterator;
import static org.apache.beam.runners.spark.structuredstreaming.translation.utils.ScalaInterop.tuple;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.function.Supplier;
import javax.annotation.CheckForNull;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.DoFnRunnerFactory.DoFnRunnerWithTeardown;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.AbstractIterator;
import org.checkerframework.checker.nullness.qual.NonNull;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Iterator;

/**
 * Abstract factory to create a {@link DoFnPartitionIt DoFn partition iterator} using a customizable
 * {@link DoFnRunners.OutputManager}.
 */
abstract class DoFnPartitionIteratorFactory<InT, FnOutT, OutT extends @NonNull Object>
    implements Function1<Iterator<WindowedValue<InT>>, Iterator<OutT>>, Serializable {

  protected final DoFnRunnerFactory<InT, FnOutT> factory;
  protected final Supplier<PipelineOptions> options;
  private final MetricsAccumulator metrics;

  private DoFnPartitionIteratorFactory(
      Supplier<PipelineOptions> options,
      MetricsAccumulator metrics,
      DoFnRunnerFactory<InT, FnOutT> factory) {
    this.options = options;
    this.metrics = metrics;
    this.factory = factory;
  }

  /**
   * {@link DoFnPartitionIteratorFactory} emitting a single output of type {@link WindowedValue} of
   * {@link OutT}.
   */
  static <InT, OutT> DoFnPartitionIteratorFactory<InT, ?, WindowedValue<OutT>> singleOutput(
      Supplier<PipelineOptions> options,
      MetricsAccumulator metrics,
      DoFnRunnerFactory<InT, OutT> factory) {
    return new SingleOut<>(options, metrics, factory);
  }

  /**
   * {@link DoFnPartitionIteratorFactory} emitting multiple outputs encoded as tuple of column index
   * and {@link WindowedValue} of {@link OutT}, where column index corresponds to the index of a
   * {@link TupleTag#getId()} in {@code tagColIdx}.
   */
  static <InT, FnOutT, OutT>
      DoFnPartitionIteratorFactory<InT, ?, Tuple2<Integer, WindowedValue<OutT>>> multiOutput(
          Supplier<PipelineOptions> options,
          MetricsAccumulator metrics,
          DoFnRunnerFactory<InT, FnOutT> factory,
          Map<String, Integer> tagColIdx) {
    return new MultiOut<>(options, metrics, factory, tagColIdx);
  }

  @Override
  public Iterator<OutT> apply(Iterator<WindowedValue<InT>> it) {
    return it.hasNext()
        ? scalaIterator(new DoFnPartitionIt(it))
        : (Iterator<OutT>) Iterator.empty();
  }

  /** Output manager emitting outputs of type {@link OutT} to the buffer. */
  abstract DoFnRunners.OutputManager outputManager(Deque<OutT> buffer);

  /**
   * {@link DoFnPartitionIteratorFactory} emitting a single output of type {@link WindowedValue} of
   * {@link OutT}.
   */
  private static class SingleOut<InT, OutT>
      extends DoFnPartitionIteratorFactory<InT, OutT, WindowedValue<OutT>> {
    private SingleOut(
        Supplier<PipelineOptions> options,
        MetricsAccumulator metrics,
        DoFnRunnerFactory<InT, OutT> factory) {
      super(options, metrics, factory);
    }

    @Override
    DoFnRunners.OutputManager outputManager(Deque<WindowedValue<OutT>> buffer) {
      return new DoFnRunners.OutputManager() {
        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
          buffer.add((WindowedValue<OutT>) output);
        }
      };
    }
  }

  /**
   * {@link DoFnPartitionIteratorFactory} emitting multiple outputs encoded as tuple of column index
   * and {@link WindowedValue} of {@link OutT}, where column index corresponds to the index of a
   * {@link TupleTag#getId()} in {@link #tagColIdx}.
   */
  private static class MultiOut<InT, FnOutT, OutT>
      extends DoFnPartitionIteratorFactory<InT, FnOutT, Tuple2<Integer, WindowedValue<OutT>>> {
    private final Map<String, Integer> tagColIdx;

    public MultiOut(
        Supplier<PipelineOptions> options,
        MetricsAccumulator metrics,
        DoFnRunnerFactory<InT, FnOutT> factory,
        Map<String, Integer> tagColIdx) {
      super(options, metrics, factory);
      this.tagColIdx = tagColIdx;
    }

    @Override
    DoFnRunners.OutputManager outputManager(Deque<Tuple2<Integer, WindowedValue<OutT>>> buffer) {
      return new DoFnRunners.OutputManager() {
        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
          // Additional unused outputs can be skipped here. In that case columnIdx is null.
          Integer columnIdx = tagColIdx.get(tag.getId());
          if (columnIdx != null) {
            buffer.add(tuple(columnIdx, (WindowedValue<OutT>) output));
          }
        }
      };
    }
  }

  // FIXME Add support for TimerInternals.TimerData
  /**
   * Partition iterator that lazily processes each element from the (input) iterator on demand
   * producing zero, one or more output elements as output (via an internal buffer).
   *
   * <p>When initializing the iterator for a partition {@code setup} followed by {@code startBundle}
   * is called.
   */
  private class DoFnPartitionIt extends AbstractIterator<OutT> {
    private final Deque<OutT> buffer = new ArrayDeque<>();
    private final DoFnRunnerWithTeardown<InT, ?> doFnRunner;
    private final Iterator<WindowedValue<InT>> partitionIt;
    private boolean isBundleFinished;

    private DoFnPartitionIt(Iterator<WindowedValue<InT>> partitionIt) {
      this.partitionIt = partitionIt;
      this.doFnRunner = factory.create(options.get(), metrics, outputManager(buffer));
    }

    @Override
    protected @CheckForNull OutT computeNext() {
      try {
        while (true) {
          if (!buffer.isEmpty()) {
            return buffer.remove();
          }
          if (partitionIt.hasNext()) {
            // grab the next element and process it.
            doFnRunner.processElement(partitionIt.next());
          } else {
            if (!isBundleFinished) {
              isBundleFinished = true;
              doFnRunner.finishBundle();
              continue; // finishBundle can produce more output
            }
            doFnRunner.teardown();
            return endOfData();
          }
        }
      } catch (RuntimeException re) {
        doFnRunner.teardown();
        throw re;
      }
    }
  }
}
