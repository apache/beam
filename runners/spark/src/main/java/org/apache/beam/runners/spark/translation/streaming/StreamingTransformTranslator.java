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
import org.apache.spark.streaming.api.java.JavaDStreamLike;
import org.apache.spark.streaming.api.java.JavaPairDStream;



/**
 * Supports translation between a Beam transform, and Spark's operations on DStreams.
 */
public final class StreamingTransformTranslator {

  private StreamingTransformTranslator() {
  }

  private static <T> TransformEvaluator<ConsoleIO.Write.Unbound<T>> print() {
    return new TransformEvaluator<ConsoleIO.Write.Unbound<T>>() {
      @Override
      public void evaluate(ConsoleIO.Write.Unbound<T> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaDStreamLike<WindowedValue<T>, ?, JavaRDD<WindowedValue<T>>> dstream =
            (JavaDStreamLike<WindowedValue<T>, ?, JavaRDD<WindowedValue<T>>>)
            ((StreamingEvaluationContext) context).getStream(transform);
        dstream.map(WindowingHelpers.<T>unwindowFunction()).print(transform.getNum());
      }
    };
  }

  private static <T> TransformEvaluator<Read.Unbounded<T>> readUnbounded() {
    return new TransformEvaluator<Read.Unbounded<T>>() {
      @Override
      public void evaluate(Read.Unbounded<T> transform, EvaluationContext context) {
        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        sec.setStream(transform, SparkUnboundedSource.read(sec.getStreamingContext(),
            sec.getRuntimeContext(), transform.getSource()));
      }
    };
  }

  private static <T> TransformEvaluator<CreateStream.QueuedValues<T>> createFromQueue() {
    return new TransformEvaluator<CreateStream.QueuedValues<T>>() {
      @Override
      public void evaluate(CreateStream.QueuedValues<T> transform, EvaluationContext context) {
        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        Iterable<Iterable<T>> values = transform.getQueuedValues();
        Coder<T> coder = sec.getOutput(transform).getCoder();
        sec.setDStreamFromQueue(transform, values, coder);
      }
    };
  }

  private static <T> TransformEvaluator<Flatten.FlattenPCollectionList<T>> flattenPColl() {
    return new TransformEvaluator<Flatten.FlattenPCollectionList<T>>() {
      @SuppressWarnings("unchecked")
      @Override
      public void evaluate(Flatten.FlattenPCollectionList<T> transform, EvaluationContext context) {
        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        PCollectionList<T> pcs = sec.getInput(transform);
        // since this is a streaming pipeline, at least one of the PCollections to "flatten" are
        // unbounded, meaning it represents a DStream.
        // So we could end up with an unbounded unified DStream.
        final List<JavaRDD<WindowedValue<T>>> rdds = new ArrayList<>();
        final List<JavaDStream<WindowedValue<T>>> dStreams = new ArrayList<>();
        for (PCollection<T> pcol: pcs.getAll()) {
          if (sec.hasStream(pcol)) {
            dStreams.add((JavaDStream<WindowedValue<T>>) sec.getStream(pcol));
          } else {
            rdds.add((JavaRDD<WindowedValue<T>>) context.getRDD(pcol));
          }
        }
        // start by unifying streams into a single stream.
        JavaDStream<WindowedValue<T>> unifiedStreams =
            sec.getStreamingContext().union(dStreams.remove(0), dStreams);
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
          sec.setStream(transform, joined);
        } else {
          sec.setStream(transform, unifiedStreams);
        }
      }
    };
  }

  private static <T, W extends BoundedWindow> TransformEvaluator<Window.Bound<T>> window() {
    return new TransformEvaluator<Window.Bound<T>>() {
      @Override
      public void evaluate(Window.Bound<T> transform, EvaluationContext context) {
        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        @SuppressWarnings("unchecked")
        WindowFn<? super T, W> windowFn = (WindowFn<? super T, W>) transform.getWindowFn();
        @SuppressWarnings("unchecked")
        JavaDStream<WindowedValue<T>> dStream =
            (JavaDStream<WindowedValue<T>>) sec.getStream(transform);
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
          sec.setStream(transform, windowedDStream);
        } else {
          final OldDoFn<T, T> addWindowsDoFn = new AssignWindowsDoFn<>(windowFn);
          final SparkRuntimeContext runtimeContext = sec.getRuntimeContext();
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
          sec.setStream(transform, outStream);
        }
      }
    };
  }

  private static <K, V> TransformEvaluator<GroupByKey<K, V>> groupByKey() {
    return new TransformEvaluator<GroupByKey<K, V>>() {
      @Override
      public void evaluate(GroupByKey<K, V> transform, EvaluationContext context) {
        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;

        @SuppressWarnings("unchecked")
        JavaDStream<WindowedValue<KV<K, V>>> dStream =
            (JavaDStream<WindowedValue<KV<K, V>>>) sec.getStream(transform);

        @SuppressWarnings("unchecked")
        final KvCoder<K, V> coder = (KvCoder<K, V>) sec.getInput(transform).getCoder();

        final SparkRuntimeContext runtimeContext = sec.getRuntimeContext();
        final WindowingStrategy<?, ?> windowingStrategy =
            sec.getInput(transform).getWindowingStrategy();

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
        sec.setStream(transform, outStream);
      }
    };
  }

  private static <K, InputT, OutputT> TransformEvaluator<Combine.GroupedValues<K, InputT, OutputT>>
  combineGrouped() {
    return new TransformEvaluator<Combine.GroupedValues<K, InputT, OutputT>>() {
      @Override
      public void evaluate(Combine.GroupedValues<K, InputT, OutputT> transform,
                           EvaluationContext context) {
        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        // get the applied combine function.
        PCollection<? extends KV<K, ? extends Iterable<InputT>>> input =
            sec.getInput(transform);
        WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
        @SuppressWarnings("unchecked")
        final CombineWithContext.KeyedCombineFnWithContext<K, InputT, ?, OutputT> fn =
            (CombineWithContext.KeyedCombineFnWithContext<K, InputT, ?, OutputT>)
                CombineFnUtil.toFnWithContext(transform.getFn());

        @SuppressWarnings("unchecked")
        JavaDStream<WindowedValue<KV<K, Iterable<InputT>>>> dStream =
            (JavaDStream<WindowedValue<KV<K, Iterable<InputT>>>>) sec.getStream(transform);

        SparkKeyedCombineFn<K, InputT, ?, OutputT> combineFnWithContext  =
            new SparkKeyedCombineFn<>(fn, sec.getRuntimeContext(),
                TranslationUtils.getSideInputs(transform.getSideInputs(), context),
                    windowingStrategy);
        sec.setStream(transform, dStream.map(new TranslationUtils.CombineGroupedValues<>(
            combineFnWithContext)));
      }
    };
  }

  private static <InputT, AccumT, OutputT> TransformEvaluator<Combine.Globally<InputT, OutputT>>
  combineGlobally() {
    return new TransformEvaluator<Combine.Globally<InputT, OutputT>>() {

      @Override
      public void evaluate(Combine.Globally<InputT, OutputT> transform, EvaluationContext context) {
        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        final PCollection<InputT> input = sec.getInput(transform);
        // serializable arguments to pass.
        final Coder<InputT> iCoder = sec.getInput(transform).getCoder();
        final Coder<OutputT> oCoder = sec.getOutput(transform).getCoder();
        @SuppressWarnings("unchecked")
        final CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn =
            (CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT>)
                CombineFnUtil.toFnWithContext(transform.getFn());
        final WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
        final SparkRuntimeContext runtimeContext = sec.getRuntimeContext();
        final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, BroadcastHelper<?>>> sideInputs =
            TranslationUtils.getSideInputs(transform.getSideInputs(), context);
        final boolean hasDefault = transform.isInsertDefault();

        @SuppressWarnings("unchecked")
        JavaDStream<WindowedValue<InputT>> dStream =
            (JavaDStream<WindowedValue<InputT>>) sec.getStream(transform);

        JavaDStream<WindowedValue<OutputT>> outStream = dStream.transform(
            new Function<JavaRDD<WindowedValue<InputT>>, JavaRDD<WindowedValue<OutputT>>>() {
          @Override
          public JavaRDD<WindowedValue<OutputT>> call(JavaRDD<WindowedValue<InputT>> rdd)
              throws Exception {
            return GroupCombineFunctions.combineGlobally(rdd, combineFn, iCoder, oCoder,
                runtimeContext, windowingStrategy, sideInputs, hasDefault);
          }
        });

        sec.setStream(transform, outStream);
      }
    };
  }

  private static <K, InputT, AccumT, OutputT>
  TransformEvaluator<Combine.PerKey<K, InputT, OutputT>> combinePerKey() {
    return new TransformEvaluator<Combine.PerKey<K, InputT, OutputT>>() {
      @Override
      public void evaluate(final Combine.PerKey<K, InputT, OutputT> transform,
                           final EvaluationContext context) {
        StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        final PCollection<KV<K, InputT>> input = sec.getInput(transform);
        // serializable arguments to pass.
        @SuppressWarnings("unchecked")
        final KvCoder<K, InputT> inputCoder =
            (KvCoder<K, InputT>) sec.getInput(transform).getCoder();
        @SuppressWarnings("unchecked")
        final CombineWithContext.KeyedCombineFnWithContext<K, InputT, AccumT, OutputT> combineFn =
            (CombineWithContext.KeyedCombineFnWithContext<K, InputT, AccumT, OutputT>)
                CombineFnUtil.toFnWithContext(transform.getFn());
        final WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
        final SparkRuntimeContext runtimeContext = sec.getRuntimeContext();
        final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, BroadcastHelper<?>>> sideInputs =
            TranslationUtils.getSideInputs(transform.getSideInputs(), context);

        @SuppressWarnings("unchecked")
        JavaDStream<WindowedValue<KV<K, InputT>>> dStream =
                (JavaDStream<WindowedValue<KV<K, InputT>>>) sec.getStream(transform);

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
        sec.setStream(transform, outStream);
      }
    };
  }

  private static <InputT, OutputT> TransformEvaluator<ParDo.Bound<InputT, OutputT>> parDo() {
    return new TransformEvaluator<ParDo.Bound<InputT, OutputT>>() {
      @Override
      public void evaluate(final ParDo.Bound<InputT, OutputT> transform,
                           final EvaluationContext context) {
        final StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        final SparkRuntimeContext runtimeContext = sec.getRuntimeContext();
        final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, BroadcastHelper<?>>> sideInputs =
            TranslationUtils.getSideInputs(transform.getSideInputs(), context);
        @SuppressWarnings("unchecked")
        final WindowFn<Object, ?> windowFn =
            (WindowFn<Object, ?>) sec.getInput(transform).getWindowingStrategy().getWindowFn();
        @SuppressWarnings("unchecked")
        JavaDStream<WindowedValue<InputT>> dStream =
            (JavaDStream<WindowedValue<InputT>>) sec.getStream(transform);

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

        sec.setStream(transform, outStream);
      }
    };
  }

  private static <InputT, OutputT> TransformEvaluator<ParDo.BoundMulti<InputT, OutputT>>
  multiDo() {
    return new TransformEvaluator<ParDo.BoundMulti<InputT, OutputT>>() {
      @Override
      public void evaluate(final ParDo.BoundMulti<InputT, OutputT> transform,
                           final EvaluationContext context) {
        final StreamingEvaluationContext sec = (StreamingEvaluationContext) context;
        final SparkRuntimeContext runtimeContext = sec.getRuntimeContext();
        final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, BroadcastHelper<?>>> sideInputs =
            TranslationUtils.getSideInputs(transform.getSideInputs(), context);
        @SuppressWarnings("unchecked")
        final WindowFn<Object, ?> windowFn =
            (WindowFn<Object, ?>) sec.getInput(transform).getWindowingStrategy().getWindowFn();
        @SuppressWarnings("unchecked")
        JavaDStream<WindowedValue<InputT>> dStream =
            (JavaDStream<WindowedValue<InputT>>) sec.getStream(transform);
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
        PCollectionTuple pct = sec.getOutput(transform);
        for (Map.Entry<TupleTag<?>, PCollection<?>> e : pct.getAll().entrySet()) {
          @SuppressWarnings("unchecked")
          JavaPairDStream<TupleTag<?>, WindowedValue<?>> filtered =
              all.filter(new TranslationUtils.TupleTagFilter(e.getKey()));
          @SuppressWarnings("unchecked")
          // Object is the best we can do since different outputs can have different tags
          JavaDStream<WindowedValue<Object>> values =
              (JavaDStream<WindowedValue<Object>>)
                  (JavaDStream<?>) TranslationUtils.dStreamValues(filtered);
          sec.setStream(e.getValue(), values);
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
