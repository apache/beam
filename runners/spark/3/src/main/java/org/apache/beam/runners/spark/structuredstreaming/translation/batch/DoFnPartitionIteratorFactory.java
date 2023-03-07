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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.CachedSideInputReader;
import org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions.NoOpStepContext;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.ParDo.MultiOutput;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.AbstractIterator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
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
  protected final String stepName;
  protected final DoFn<InT, FnOutT> doFn;
  protected final DoFnSchemaInformation doFnSchema;
  protected final Supplier<PipelineOptions> options;
  protected final Coder<InT> coder;
  protected final WindowingStrategy<?, ?> windowingStrategy;
  protected final TupleTag<FnOutT> mainOutput;
  protected final List<TupleTag<?>> additionalOutputs;
  protected final Map<TupleTag<?>, Coder<?>> outputCoders;
  protected final Map<String, PCollectionView<?>> sideInputs;
  protected final SideInputReader sideInputReader;

  private final MetricsAccumulator metrics;

  private DoFnPartitionIteratorFactory(
      AppliedPTransform<PCollection<? extends InT>, ?, MultiOutput<InT, FnOutT>> appliedPT,
      Supplier<PipelineOptions> options,
      PCollection<InT> input,
      SideInputReader sideInputReader,
      MetricsAccumulator metrics) {
    this.stepName = appliedPT.getFullName();
    this.doFn = appliedPT.getTransform().getFn();
    this.doFnSchema = ParDoTranslation.getSchemaInformation(appliedPT);
    this.options = options;
    this.coder = input.getCoder();
    this.windowingStrategy = input.getWindowingStrategy();
    this.mainOutput = appliedPT.getTransform().getMainOutputTag();
    this.additionalOutputs = additionalOutputs(appliedPT.getTransform());
    this.outputCoders = outputCoders(appliedPT.getOutputs());
    this.sideInputs = appliedPT.getTransform().getSideInputs();
    this.sideInputReader = sideInputReader;
    this.metrics = metrics;
  }

  /**
   * {@link DoFnPartitionIteratorFactory} emitting a single output of type {@link WindowedValue} of
   * {@link OutT}.
   */
  static <InT, OutT> DoFnPartitionIteratorFactory<InT, ?, WindowedValue<OutT>> singleOutput(
      AppliedPTransform<PCollection<? extends InT>, ?, MultiOutput<InT, OutT>> appliedPT,
      Supplier<PipelineOptions> options,
      PCollection<InT> input,
      SideInputReader sideInputReader,
      MetricsAccumulator metrics) {
    return new SingleOut<>(appliedPT, options, input, sideInputReader, metrics);
  }

  /**
   * {@link DoFnPartitionIteratorFactory} emitting multiple outputs encoded as tuple of column index
   * and {@link WindowedValue} of {@link OutT}, where column index corresponds to the index of a
   * {@link TupleTag#getId()} in {@code tagColIdx}.
   */
  static <InT, FnOutT, OutT>
      DoFnPartitionIteratorFactory<InT, ?, Tuple2<Integer, WindowedValue<OutT>>> multiOutput(
          AppliedPTransform<PCollection<? extends InT>, ?, MultiOutput<InT, FnOutT>> appliedPT,
          Supplier<PipelineOptions> options,
          PCollection<InT> input,
          SideInputReader sideInputReader,
          MetricsAccumulator metrics,
          Map<String, Integer> tagColIdx) {
    return new MultiOut<>(appliedPT, options, input, sideInputReader, metrics, tagColIdx);
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
        AppliedPTransform<PCollection<? extends InT>, ?, MultiOutput<InT, OutT>> appliedPT,
        Supplier<PipelineOptions> options,
        PCollection<InT> input,
        SideInputReader sideInputReader,
        MetricsAccumulator metrics) {
      super(appliedPT, options, input, sideInputReader, metrics);
    }

    @Override
    DoFnRunners.OutputManager outputManager(Deque<WindowedValue<OutT>> buffer) {
      return new DoFnRunners.OutputManager() {
        @Override
        public <T> void output(TupleTag<T> tag, WindowedValue<T> output) {
          // SingleOut will only ever emmit the mainOutput. Though, there might be additional
          // outputs which are skipped if unused to avoid caching.
          if (mainOutput.equals(tag)) {
            buffer.add((WindowedValue<OutT>) output);
          }
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
        AppliedPTransform<PCollection<? extends InT>, ?, MultiOutput<InT, FnOutT>> appliedPT,
        Supplier<PipelineOptions> options,
        PCollection<InT> input,
        SideInputReader sideInputReader,
        MetricsAccumulator metrics,
        Map<String, Integer> tagColIdx) {
      super(appliedPT, options, input, sideInputReader, metrics);
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
    private final DoFnRunner<InT, ?> doFnRunner = metricsRunner(simpleRunner(buffer));
    private final Iterator<WindowedValue<InT>> partitionIt;
    private boolean isBundleFinished;

    private DoFnPartitionIt(Iterator<WindowedValue<InT>> partitionIt) {
      this.partitionIt = partitionIt;
      // Before starting to iterate over the partition, invoke setup and then startBundle
      DoFnInvokers.tryInvokeSetupFor(doFn, options.get());
      try {
        doFnRunner.startBundle();
      } catch (RuntimeException re) {
        DoFnInvokers.invokerFor(doFn).invokeTeardown();
        throw re;
      }
    }

    @Override
    protected OutT computeNext() {
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
            DoFnInvokers.invokerFor(doFn).invokeTeardown();
            return endOfData();
          }
        }
      } catch (RuntimeException re) {
        DoFnInvokers.invokerFor(doFn).invokeTeardown();
        throw re;
      }
    }
  }

  private DoFnRunner<InT, FnOutT> simpleRunner(Deque<OutT> buffer) {
    return DoFnRunners.simpleRunner(
        options.get(),
        doFn,
        CachedSideInputReader.of(sideInputReader, sideInputs.values()),
        outputManager(buffer),
        mainOutput,
        additionalOutputs,
        new NoOpStepContext(),
        coder,
        outputCoders,
        windowingStrategy,
        doFnSchema,
        sideInputs);
  }

  private DoFnRunner<InT, FnOutT> metricsRunner(DoFnRunner<InT, FnOutT> runner) {
    return new DoFnRunnerWithMetrics<>(stepName, runner, metrics);
  }

  private static Map<TupleTag<?>, Coder<?>> outputCoders(Map<TupleTag<?>, PCollection<?>> outputs) {
    Map<TupleTag<?>, Coder<?>> coders = Maps.newHashMapWithExpectedSize(outputs.size());
    for (Map.Entry<TupleTag<?>, PCollection<?>> e : outputs.entrySet()) {
      coders.put(e.getKey(), e.getValue().getCoder());
    }
    return coders;
  }

  private static List<TupleTag<?>> additionalOutputs(MultiOutput<?, ?> transform) {
    List<TupleTag<?>> tags = transform.getAdditionalOutputTags().getAll();
    return tags.isEmpty() ? Collections.emptyList() : new ArrayList<>(tags);
  }
}
