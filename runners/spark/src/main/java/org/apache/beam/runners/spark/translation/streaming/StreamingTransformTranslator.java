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
package org.apache.beam.runners.spark.translation.streaming;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.AssignWindowsDoFn;
import org.apache.beam.runners.spark.aggregators.AccumulatorSingleton;
import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.apache.beam.runners.spark.io.ConsoleIO;
import org.apache.beam.runners.spark.io.CreateStream;
import org.apache.beam.runners.spark.io.SparkUnboundedSource;
import org.apache.beam.runners.spark.translation.BoundedDataset;
import org.apache.beam.runners.spark.translation.Dataset;
import org.apache.beam.runners.spark.translation.DoFnFunction;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.GroupCombineFunctions;
import org.apache.beam.runners.spark.translation.MultiDoFnFunction;
import org.apache.beam.runners.spark.translation.SparkKeyedCombineFn;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.runners.spark.translation.SparkRuntimeContext;
import org.apache.beam.runners.spark.translation.TransformEvaluator;
import org.apache.beam.runners.spark.translation.TranslationUtils;
import org.apache.beam.runners.spark.translation.WindowingHelpers;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;


/**
 * Supports translation between a Beam transform, and Spark's operations on DStreams.
 */
final class StreamingTransformTranslator {

  private StreamingTransformTranslator() {
  }

  private static <T> TransformEvaluator<ConsoleIO.Write.Unbound<T>> print() {
    return new TransformEvaluator<ConsoleIO.Write.Unbound<T>>() {
      @Override
      public void evaluate(ConsoleIO.Write.Unbound<T> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaDStream<WindowedValue<T>> dstream =
            ((UnboundedDataset<T>) (context).borrowDataset(transform)).getDStream();
        dstream.map(WindowingHelpers.<T>unwindowFunction()).print(transform.getNum());
      }
    };
  }

  private static <T> TransformEvaluator<Read.Unbounded<T>> readUnbounded() {
    return new TransformEvaluator<Read.Unbounded<T>>() {
      @Override
      public void evaluate(Read.Unbounded<T> transform, EvaluationContext context) {
        context.putDataset(transform,
            new UnboundedDataset<>(SparkUnboundedSource.read(context.getStreamingContext(),
                context.getRuntimeContext(), transform.getSource())));
      }
    };
  }

  private static <T> TransformEvaluator<CreateStream.QueuedValues<T>> createFromQueue() {
    return new TransformEvaluator<CreateStream.QueuedValues<T>>() {
      @Override
      public void evaluate(CreateStream.QueuedValues<T> transform, EvaluationContext context) {
        Iterable<Iterable<T>> values = transform.getQueuedValues();
        Coder<T> coder = context.getOutput(transform).getCoder();
        context.putUnboundedDatasetFromQueue(transform, values, coder);
      }
    };
  }

  private static <T> TransformEvaluator<Flatten.FlattenPCollectionList<T>> flattenPColl() {
    return new TransformEvaluator<Flatten.FlattenPCollectionList<T>>() {
      @SuppressWarnings("unchecked")
      @Override
      public void evaluate(Flatten.FlattenPCollectionList<T> transform, EvaluationContext context) {
        PCollectionList<T> pcs = context.getInput(transform);
        // since this is a streaming pipeline, at least one of the PCollections to "flatten" are
        // unbounded, meaning it represents a DStream.
        // So we could end up with an unbounded unified DStream.
        final List<JavaRDD<WindowedValue<T>>> rdds = new ArrayList<>();
        final List<JavaDStream<WindowedValue<T>>> dStreams = new ArrayList<>();
        for (PCollection<T> pcol : pcs.getAll()) {
         Dataset dataset = context.borrowDataset(pcol);
          if (dataset instanceof UnboundedDataset) {
            dStreams.add(((UnboundedDataset<T>) dataset).getDStream());
          } else {
            rdds.add(((BoundedDataset<T>) dataset).getRDD());
          }
        }
        // start by unifying streams into a single stream.
        JavaDStream<WindowedValue<T>> unifiedStreams =
            context.getStreamingContext().union(dStreams.remove(0), dStreams);
        // now unify in RDDs.
        if (rdds.size() > 0) {
          JavaDStream<WindowedValue<T>> joined = unifiedStreams.transform(
              new Function<JavaRDD<WindowedValue<T>>, JavaRDD<WindowedValue<T>>>() {
            @Override
            public JavaRDD<WindowedValue<T>> call(JavaRDD<WindowedValue<T>> streamRdd)
                throws Exception {
              return new JavaSparkContext(streamRdd.context()).union(streamRdd, rdds);
            }
          });
          context.putDataset(transform, new UnboundedDataset<>(joined));
        } else {
          context.putDataset(transform, new UnboundedDataset<>(unifiedStreams));
        }
      }
    };
  }

  private static <T, W extends BoundedWindow> TransformEvaluator<Window.Bound<T>> window() {
    return new TransformEvaluator<Window.Bound<T>>() {
      @Override
      public void evaluate(Window.Bound<T> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        WindowFn<? super T, W> windowFn = (WindowFn<? super T, W>) transform.getWindowFn();
        @SuppressWarnings("unchecked")
        JavaDStream<WindowedValue<T>> dStream =
            ((UnboundedDataset<T>) context.borrowDataset(transform)).getDStream();
        // get the right window durations.
        Duration windowDuration;
        Duration slideDuration;
        if (windowFn instanceof FixedWindows) {
          windowDuration = Durations.milliseconds(((FixedWindows) windowFn).getSize().getMillis());
          slideDuration = windowDuration;
        } else if (windowFn instanceof SlidingWindows) {
          SlidingWindows slidingWindows = (SlidingWindows) windowFn;
          windowDuration = Durations.milliseconds(slidingWindows.getSize().getMillis());
          slideDuration = Durations.milliseconds(slidingWindows.getPeriod().getMillis());
        } else {
          throw new UnsupportedOperationException(String.format("WindowFn %s is not supported.",
              windowFn.getClass().getCanonicalName()));
        }
        JavaDStream<WindowedValue<T>> windowedDStream =
            dStream.window(windowDuration, slideDuration);
        //--- then we apply windowing to the elements
        if (TranslationUtils.skipAssignWindows(transform, context)) {
          context.putDataset(transform, new UnboundedDataset<>(windowedDStream));
        } else {
          final OldDoFn<T, T> addWindowsDoFn = new AssignWindowsDoFn<>(windowFn);
          final SparkRuntimeContext runtimeContext = context.getRuntimeContext();
          JavaDStream<WindowedValue<T>> outStream = windowedDStream.transform(
              new Function<JavaRDD<WindowedValue<T>>, JavaRDD<WindowedValue<T>>>() {
            @Override
            public JavaRDD<WindowedValue<T>> call(JavaRDD<WindowedValue<T>> rdd) throws Exception {
              final Accumulator<NamedAggregators> accum =
                AccumulatorSingleton.getInstance(new JavaSparkContext(rdd.context()));
              return rdd.mapPartitions(
                new DoFnFunction<>(accum, addWindowsDoFn, runtimeContext, null, null));
            }
          });
          context.putDataset(transform, new UnboundedDataset<>(outStream));
        }
      }
    };
  }

  private static <K, V> TransformEvaluator<GroupByKey<K, V>> groupByKey() {
    return new TransformEvaluator<GroupByKey<K, V>>() {
      @Override
      public void evaluate(GroupByKey<K, V> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaDStream<WindowedValue<KV<K, V>>> dStream =
            ((UnboundedDataset<KV<K, V>>) context.borrowDataset(transform)).getDStream();

        @SuppressWarnings("unchecked")
        final KvCoder<K, V> coder = (KvCoder<K, V>) context.getInput(transform).getCoder();

        final SparkRuntimeContext runtimeContext = context.getRuntimeContext();
        final WindowingStrategy<?, ?> windowingStrategy =
            context.getInput(transform).getWindowingStrategy();

        JavaDStream<WindowedValue<KV<K, Iterable<V>>>> outStream =
            dStream.transform(new Function<JavaRDD<WindowedValue<KV<K, V>>>,
                JavaRDD<WindowedValue<KV<K, Iterable<V>>>>>() {
          @Override
          public JavaRDD<WindowedValue<KV<K, Iterable<V>>>> call(
              JavaRDD<WindowedValue<KV<K, V>>> rdd) throws Exception {
            final Accumulator<NamedAggregators> accum =
                AccumulatorSingleton.getInstance(new JavaSparkContext(rdd.context()));
            return GroupCombineFunctions.groupByKey(rdd, accum, coder, runtimeContext,
                windowingStrategy);
          }
        });
        context.putDataset(transform, new UnboundedDataset<>(outStream));
      }
    };
  }

  private static <K, InputT, OutputT> TransformEvaluator<Combine.GroupedValues<K, InputT, OutputT>>
  combineGrouped() {
    return new TransformEvaluator<Combine.GroupedValues<K, InputT, OutputT>>() {
      @SuppressWarnings("unchecked")
      @Override
      public void evaluate(Combine.GroupedValues<K, InputT, OutputT> transform,
                           EvaluationContext context) {
        // get the applied combine function.
        PCollection<? extends KV<K, ? extends Iterable<InputT>>> input =
            context.getInput(transform);
        WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
        final CombineWithContext.KeyedCombineFnWithContext<K, InputT, ?, OutputT> fn =
            (CombineWithContext.KeyedCombineFnWithContext<K, InputT, ?, OutputT>)
                CombineFnUtil.toFnWithContext(transform.getFn());

        JavaDStream<WindowedValue<KV<K, Iterable<InputT>>>> dStream =
            ((UnboundedDataset<KV<K, Iterable<InputT>>>) context.borrowDataset(transform))
                .getDStream();

        SparkKeyedCombineFn<K, InputT, ?, OutputT> combineFnWithContext =
            new SparkKeyedCombineFn<>(fn, context.getRuntimeContext(),
                TranslationUtils.getSideInputs(transform.getSideInputs(), context),
                windowingStrategy);
        context.putDataset(transform, new UnboundedDataset<>(dStream.map(new TranslationUtils
            .CombineGroupedValues<>(
            combineFnWithContext))));
      }
    };
  }

  private static <InputT, AccumT, OutputT> TransformEvaluator<Combine.Globally<InputT, OutputT>>
  combineGlobally() {
    return new TransformEvaluator<Combine.Globally<InputT, OutputT>>() {

      @SuppressWarnings("unchecked")
      @Override
      public void evaluate(Combine.Globally<InputT, OutputT> transform, EvaluationContext context) {
        final PCollection<InputT> input = context.getInput(transform);
        // serializable arguments to pass.
        final Coder<InputT> iCoder = context.getInput(transform).getCoder();
        final Coder<OutputT> oCoder = context.getOutput(transform).getCoder();
        final CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn =
            (CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT>)
                CombineFnUtil.toFnWithContext(transform.getFn());
        final WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
        final SparkRuntimeContext runtimeContext = context.getRuntimeContext();
        final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, BroadcastHelper<?>>> sideInputs =
            TranslationUtils.getSideInputs(transform.getSideInputs(), context);
        final boolean hasDefault = transform.isInsertDefault();

        JavaDStream<WindowedValue<InputT>> dStream =
            ((UnboundedDataset<InputT>) context.borrowDataset(transform)).getDStream();

        JavaDStream<WindowedValue<OutputT>> outStream = dStream.transform(
            new Function<JavaRDD<WindowedValue<InputT>>, JavaRDD<WindowedValue<OutputT>>>() {
          @Override
          public JavaRDD<WindowedValue<OutputT>> call(JavaRDD<WindowedValue<InputT>> rdd)
              throws Exception {
            return GroupCombineFunctions.combineGlobally(rdd, combineFn, iCoder, oCoder,
                runtimeContext, windowingStrategy, sideInputs, hasDefault);
          }
        });

        context.putDataset(transform, new UnboundedDataset<>(outStream));
      }
    };
  }

  private static <K, InputT, AccumT, OutputT>
  TransformEvaluator<Combine.PerKey<K, InputT, OutputT>> combinePerKey() {
    return new TransformEvaluator<Combine.PerKey<K, InputT, OutputT>>() {
      @SuppressWarnings("unchecked")
      @Override
      public void evaluate(final Combine.PerKey<K, InputT, OutputT> transform,
                           final EvaluationContext context) {
        final PCollection<KV<K, InputT>> input = context.getInput(transform);
        // serializable arguments to pass.
        final KvCoder<K, InputT> inputCoder =
            (KvCoder<K, InputT>) context.getInput(transform).getCoder();
        final CombineWithContext.KeyedCombineFnWithContext<K, InputT, AccumT, OutputT> combineFn =
            (CombineWithContext.KeyedCombineFnWithContext<K, InputT, AccumT, OutputT>)
                CombineFnUtil.toFnWithContext(transform.getFn());
        final WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
        final SparkRuntimeContext runtimeContext = context.getRuntimeContext();
        final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, BroadcastHelper<?>>> sideInputs =
            TranslationUtils.getSideInputs(transform.getSideInputs(), context);

        JavaDStream<WindowedValue<KV<K, InputT>>> dStream =
            ((UnboundedDataset<KV<K, InputT>>) context.borrowDataset(transform)).getDStream();

        JavaDStream<WindowedValue<KV<K, OutputT>>> outStream =
            dStream.transform(new Function<JavaRDD<WindowedValue<KV<K, InputT>>>,
                JavaRDD<WindowedValue<KV<K, OutputT>>>>() {
          @Override
          public JavaRDD<WindowedValue<KV<K, OutputT>>> call(
              JavaRDD<WindowedValue<KV<K, InputT>>> rdd) throws Exception {
            return GroupCombineFunctions.combinePerKey(rdd, combineFn, inputCoder, runtimeContext,
                windowingStrategy, sideInputs);
          }
        });
        context.putDataset(transform, new UnboundedDataset<>(outStream));
      }
    };
  }

  private static <InputT, OutputT> TransformEvaluator<ParDo.Bound<InputT, OutputT>> parDo() {
    return new TransformEvaluator<ParDo.Bound<InputT, OutputT>>() {
      @SuppressWarnings("unchecked")
      @Override
      public void evaluate(final ParDo.Bound<InputT, OutputT> transform,
                           final EvaluationContext context) {
        final SparkRuntimeContext runtimeContext = context.getRuntimeContext();
        final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, BroadcastHelper<?>>> sideInputs =
            TranslationUtils.getSideInputs(transform.getSideInputs(), context);
        final WindowFn<Object, ?> windowFn =
            (WindowFn<Object, ?>) context.getInput(transform).getWindowingStrategy().getWindowFn();
        JavaDStream<WindowedValue<InputT>> dStream =
            ((UnboundedDataset<InputT>) context.borrowDataset(transform)).getDStream();

        JavaDStream<WindowedValue<OutputT>> outStream =
            dStream.transform(new Function<JavaRDD<WindowedValue<InputT>>,
                JavaRDD<WindowedValue<OutputT>>>() {
          @Override
          public JavaRDD<WindowedValue<OutputT>> call(JavaRDD<WindowedValue<InputT>> rdd) throws
              Exception {
            final Accumulator<NamedAggregators> accum =
                AccumulatorSingleton.getInstance(new JavaSparkContext(rdd.context()));
            return rdd.mapPartitions(
                new DoFnFunction<>(accum, transform.getFn(), runtimeContext, sideInputs, windowFn));
          }
        });

        context.putDataset(transform, new UnboundedDataset<>(outStream));
      }
    };
  }

  private static <InputT, OutputT> TransformEvaluator<ParDo.BoundMulti<InputT, OutputT>>
  multiDo() {
    return new TransformEvaluator<ParDo.BoundMulti<InputT, OutputT>>() {
      @Override
      public void evaluate(final ParDo.BoundMulti<InputT, OutputT> transform,
                           final EvaluationContext context) {
        final SparkRuntimeContext runtimeContext = context.getRuntimeContext();
        final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, BroadcastHelper<?>>> sideInputs =
            TranslationUtils.getSideInputs(transform.getSideInputs(), context);
        @SuppressWarnings("unchecked")
        final WindowFn<Object, ?> windowFn =
            (WindowFn<Object, ?>) context.getInput(transform).getWindowingStrategy().getWindowFn();
        @SuppressWarnings("unchecked")
        JavaDStream<WindowedValue<InputT>> dStream =
            ((UnboundedDataset<InputT>) context.borrowDataset(transform)).getDStream();
        JavaPairDStream<TupleTag<?>, WindowedValue<?>> all = dStream.transformToPair(
            new Function<JavaRDD<WindowedValue<InputT>>,
                JavaPairRDD<TupleTag<?>, WindowedValue<?>>>() {
          @Override
          public JavaPairRDD<TupleTag<?>, WindowedValue<?>> call(
              JavaRDD<WindowedValue<InputT>> rdd) throws Exception {
            final Accumulator<NamedAggregators> accum =
                AccumulatorSingleton.getInstance(new JavaSparkContext(rdd.context()));
            return rdd.mapPartitionsToPair(new MultiDoFnFunction<>(accum, transform.getFn(),
                runtimeContext, transform.getMainOutputTag(), sideInputs, windowFn));
          }
        }).cache();
        PCollectionTuple pct = context.getOutput(transform);
        for (Map.Entry<TupleTag<?>, PCollection<?>> e : pct.getAll().entrySet()) {
          @SuppressWarnings("unchecked")
          JavaPairDStream<TupleTag<?>, WindowedValue<?>> filtered =
              all.filter(new TranslationUtils.TupleTagFilter(e.getKey()));
          @SuppressWarnings("unchecked")
          // Object is the best we can do since different outputs can have different tags
          JavaDStream<WindowedValue<Object>> values =
              (JavaDStream<WindowedValue<Object>>)
                  (JavaDStream<?>) TranslationUtils.dStreamValues(filtered);
          context.putDataset(e.getValue(), new UnboundedDataset<>(values));
        }
      }
    };
  }

  private static final Map<Class<? extends PTransform>, TransformEvaluator<?>> EVALUATORS = Maps
      .newHashMap();

  static {
    EVALUATORS.put(Read.Unbounded.class, readUnbounded());
    EVALUATORS.put(GroupByKey.class, groupByKey());
    EVALUATORS.put(Combine.GroupedValues.class, combineGrouped());
    EVALUATORS.put(Combine.Globally.class, combineGlobally());
    EVALUATORS.put(Combine.PerKey.class, combinePerKey());
    EVALUATORS.put(ParDo.Bound.class, parDo());
    EVALUATORS.put(ParDo.BoundMulti.class, multiDo());
    EVALUATORS.put(ConsoleIO.Write.Unbound.class, print());
    EVALUATORS.put(CreateStream.QueuedValues.class, createFromQueue());
    EVALUATORS.put(Window.Bound.class, window());
    EVALUATORS.put(Flatten.FlattenPCollectionList.class, flattenPColl());
  }

  /**
   * Translator matches Beam transformation with the appropriate evaluator.
   */
  public static class Translator implements SparkPipelineTranslator {

    private final SparkPipelineTranslator batchTranslator;

    Translator(SparkPipelineTranslator batchTranslator) {
      this.batchTranslator = batchTranslator;
    }

    @Override
    public boolean hasTranslation(Class<? extends PTransform<?, ?>> clazz) {
      // streaming includes rdd/bounded transformations as well
      return EVALUATORS.containsKey(clazz) || batchTranslator.hasTranslation(clazz);
    }

    @Override
    public <TransformT extends PTransform<?, ?>> TransformEvaluator<TransformT>
        translateBounded(Class<TransformT> clazz) {
      TransformEvaluator<TransformT> transformEvaluator = batchTranslator.translateBounded(clazz);
      checkState(transformEvaluator != null,
          "No TransformEvaluator registered for BOUNDED transform %s", clazz);
      return transformEvaluator;
    }

    @Override
    public <TransformT extends PTransform<?, ?>> TransformEvaluator<TransformT>
        translateUnbounded(Class<TransformT> clazz) {
      @SuppressWarnings("unchecked") TransformEvaluator<TransformT> transformEvaluator =
          (TransformEvaluator<TransformT>) EVALUATORS.get(clazz);
      checkState(transformEvaluator != null,
          "No TransformEvaluator registered for for UNBOUNDED transform %s", clazz);
      return transformEvaluator;
    }
  }
}
