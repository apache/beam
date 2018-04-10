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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.runners.spark.translation.TranslationUtils.avoidRddSerialization;
import static org.apache.beam.runners.spark.translation.TranslationUtils.rejectSplittable;

import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.Map;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.metrics.MetricsContainerStepMap;
import org.apache.beam.runners.spark.aggregators.AggregatorsAccumulator;
import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.io.SourceRDD;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.util.SideInputBroadcast;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.CombineFnUtil;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * Supports translation between a Beam transform, and Spark's operations on RDDs.
 */
public final class TransformTranslator {

  private TransformTranslator() {
  }

  private static <T> TransformEvaluator<Flatten.PCollections<T>> flattenPColl() {
    return new TransformEvaluator<Flatten.PCollections<T>>() {
      @SuppressWarnings("unchecked")
      @Override
      public void evaluate(Flatten.PCollections<T> transform, EvaluationContext context) {
        Collection<PValue> pcs = context.getInputs(transform).values();
        JavaRDD<WindowedValue<T>> unionRDD;
        if (pcs.isEmpty()) {
          unionRDD = context.getSparkContext().emptyRDD();
        } else {
          JavaRDD<WindowedValue<T>>[] rdds = new JavaRDD[pcs.size()];
          int index = 0;
          for (PValue pc : pcs) {
            checkArgument(
                pc instanceof PCollection,
                "Flatten had non-PCollection value in input: %s of type %s",
                pc,
                pc.getClass().getSimpleName());
            rdds[index] = ((BoundedDataset<T>) context.borrowDataset(pc)).getRDD();
            index++;
          }
          unionRDD = context.getSparkContext().union(rdds);
        }
        context.putDataset(transform, new BoundedDataset<>(unionRDD));
      }

      @Override
      public String toNativeString() {
        return "sparkContext.union(...)";
      }
    };
  }

  private static <K, V, W extends BoundedWindow> TransformEvaluator<GroupByKey<K, V>> groupByKey() {
    return new TransformEvaluator<GroupByKey<K, V>>() {
      @Override
      public void evaluate(GroupByKey<K, V> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaRDD<WindowedValue<KV<K, V>>> inRDD =
            ((BoundedDataset<KV<K, V>>) context.borrowDataset(transform)).getRDD();
        @SuppressWarnings("unchecked")
        final KvCoder<K, V> coder = (KvCoder<K, V>) context.getInput(transform).getCoder();
        final Accumulator<NamedAggregators> accum = AggregatorsAccumulator.getInstance();
        @SuppressWarnings("unchecked")
        final WindowingStrategy<?, W> windowingStrategy =
            (WindowingStrategy<?, W>) context.getInput(transform).getWindowingStrategy();
        @SuppressWarnings("unchecked")
        final WindowFn<Object, W> windowFn = (WindowFn<Object, W>) windowingStrategy.getWindowFn();

        // --- coders.
        final Coder<K> keyCoder = coder.getKeyCoder();
        final WindowedValue.WindowedValueCoder<V> wvCoder =
            WindowedValue.FullWindowedValueCoder.of(coder.getValueCoder(), windowFn.windowCoder());

        // --- group by key only.
        JavaRDD<WindowedValue<KV<K, Iterable<WindowedValue<V>>>>> groupedByKey =
            GroupCombineFunctions.groupByKeyOnly(inRDD, keyCoder, wvCoder);

        // --- now group also by window.
        // for batch, GroupAlsoByWindow uses an in-memory StateInternals.
        JavaRDD<WindowedValue<KV<K, Iterable<V>>>> groupedAlsoByWindow =
            groupedByKey.flatMap(
                new SparkGroupAlsoByWindowViaOutputBufferFn<>(
                    windowingStrategy,
                    new TranslationUtils.InMemoryStateInternalsFactory<>(),
                    SystemReduceFn.buffering(coder.getValueCoder()),
                    context.getSerializableOptions(),
                    accum));

        context.putDataset(transform, new BoundedDataset<>(groupedAlsoByWindow));
      }

      @Override
      public String toNativeString() {
        return "groupByKey()";
      }
    };
  }

  private static <K, InputT, OutputT> TransformEvaluator<Combine.GroupedValues<K, InputT, OutputT>>
      combineGrouped() {
    return new TransformEvaluator<Combine.GroupedValues<K, InputT, OutputT>>() {
      @Override
      public void evaluate(
          Combine.GroupedValues<K, InputT, OutputT> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        CombineWithContext.CombineFnWithContext<InputT, ?, OutputT> combineFn =
            (CombineWithContext.CombineFnWithContext<InputT, ?, OutputT>)
                CombineFnUtil.toFnWithContext(transform.getFn());
        final SparkKeyedCombineFn<K, InputT, ?, OutputT> sparkCombineFn =
            new SparkKeyedCombineFn<>(
                combineFn,
                context.getSerializableOptions(),
                TranslationUtils.getSideInputs(transform.getSideInputs(), context),
                context.getInput(transform).getWindowingStrategy());

        @SuppressWarnings("unchecked")
        JavaRDD<WindowedValue<KV<K, Iterable<InputT>>>> inRDD =
            ((BoundedDataset<KV<K, Iterable<InputT>>>) context.borrowDataset(transform)).getRDD();

        JavaRDD<WindowedValue<KV<K, OutputT>>> outRDD =
            inRDD.map(
                in ->
                    WindowedValue.of(
                        KV.of(in.getValue().getKey(), sparkCombineFn.apply(in)),
                        in.getTimestamp(),
                        in.getWindows(),
                        in.getPane()));
        context.putDataset(transform, new BoundedDataset<>(outRDD));
      }

      @Override
      public String toNativeString() {
        return "map(new <fn>())";
      }
    };
  }

  private static <InputT, AccumT, OutputT> TransformEvaluator<Combine.Globally<InputT, OutputT>>
      combineGlobally() {
    return new TransformEvaluator<Combine.Globally<InputT, OutputT>>() {

      @Override
      public void evaluate(Combine.Globally<InputT, OutputT> transform, EvaluationContext context) {
        final PCollection<InputT> input = context.getInput(transform);
        final Coder<InputT> iCoder = context.getInput(transform).getCoder();
        final Coder<OutputT> oCoder = context.getOutput(transform).getCoder();
        final WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
        @SuppressWarnings("unchecked")
        final CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn =
            (CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT>)
                CombineFnUtil.toFnWithContext(transform.getFn());
        final WindowedValue.FullWindowedValueCoder<OutputT> wvoCoder =
            WindowedValue.FullWindowedValueCoder.of(
                oCoder, windowingStrategy.getWindowFn().windowCoder());
        final boolean hasDefault = transform.isInsertDefault();

        final SparkGlobalCombineFn<InputT, AccumT, OutputT> sparkCombineFn =
            new SparkGlobalCombineFn<>(
                combineFn,
                context.getSerializableOptions(),
                TranslationUtils.getSideInputs(transform.getSideInputs(), context),
                windowingStrategy);
        final Coder<AccumT> aCoder;
        try {
          aCoder = combineFn.getAccumulatorCoder(context.getPipeline().getCoderRegistry(), iCoder);
        } catch (CannotProvideCoderException e) {
          throw new IllegalStateException("Could not determine coder for accumulator", e);
        }

        @SuppressWarnings("unchecked")
        JavaRDD<WindowedValue<InputT>> inRdd =
            ((BoundedDataset<InputT>) context.borrowDataset(transform)).getRDD();

        JavaRDD<WindowedValue<OutputT>> outRdd;

        Optional<Iterable<WindowedValue<AccumT>>> maybeAccumulated =
            GroupCombineFunctions.combineGlobally(
                inRdd, sparkCombineFn, iCoder, aCoder, windowingStrategy);

        if (maybeAccumulated.isPresent()) {
          Iterable<WindowedValue<OutputT>> output =
              sparkCombineFn.extractOutput(maybeAccumulated.get());
          outRdd =
              context
                  .getSparkContext()
                  .parallelize(CoderHelpers.toByteArrays(output, wvoCoder))
                  .map(CoderHelpers.fromByteFunction(wvoCoder));
        } else {
          // handle empty input RDD, which will naturally skip the entire execution
          // as Spark will not run on empty RDDs.
          JavaSparkContext jsc = new JavaSparkContext(inRdd.context());
          if (hasDefault) {
            OutputT defaultValue = combineFn.defaultValue();
            outRdd =
                jsc.parallelize(Lists.newArrayList(CoderHelpers.toByteArray(defaultValue, oCoder)))
                    .map(CoderHelpers.fromByteFunction(oCoder))
                    .map(WindowingHelpers.windowFunction());
          } else {
            outRdd = jsc.emptyRDD();
          }
        }

        context.putDataset(transform, new BoundedDataset<>(outRdd));
      }

      @Override
      public String toNativeString() {
        return "aggregate(..., new <fn>(), ...)";
      }
    };
  }

  private static <K, InputT, AccumT, OutputT>
  TransformEvaluator<Combine.PerKey<K, InputT, OutputT>> combinePerKey() {
    return new TransformEvaluator<Combine.PerKey<K, InputT, OutputT>>() {
      @Override
      public void evaluate(
          Combine.PerKey<K, InputT, OutputT> transform, EvaluationContext context) {
        final PCollection<KV<K, InputT>> input = context.getInput(transform);
        // serializable arguments to pass.
        @SuppressWarnings("unchecked")
        final KvCoder<K, InputT> inputCoder =
            (KvCoder<K, InputT>) context.getInput(transform).getCoder();
        @SuppressWarnings("unchecked")
        final CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn =
            (CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT>)
                CombineFnUtil.toFnWithContext(transform.getFn());
        final WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
        final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs =
            TranslationUtils.getSideInputs(transform.getSideInputs(), context);
        final SparkKeyedCombineFn<K, InputT, AccumT, OutputT> sparkCombineFn =
            new SparkKeyedCombineFn<>(
                combineFn, context.getSerializableOptions(), sideInputs, windowingStrategy);
        final Coder<AccumT> vaCoder;
        try {
          vaCoder =
              combineFn.getAccumulatorCoder(
                  context.getPipeline().getCoderRegistry(), inputCoder.getValueCoder());
        } catch (CannotProvideCoderException e) {
          throw new IllegalStateException("Could not determine coder for accumulator", e);
        }

        @SuppressWarnings("unchecked")
        JavaRDD<WindowedValue<KV<K, InputT>>> inRdd =
            ((BoundedDataset<KV<K, InputT>>) context.borrowDataset(transform)).getRDD();

        JavaPairRDD<K, Iterable<WindowedValue<KV<K, AccumT>>>> accumulatePerKey =
            GroupCombineFunctions.combinePerKey(
                inRdd,
                sparkCombineFn,
                inputCoder.getKeyCoder(),
                inputCoder.getValueCoder(),
                vaCoder,
                windowingStrategy);

        JavaRDD<WindowedValue<KV<K, OutputT>>> outRdd =
            accumulatePerKey
                .flatMapValues(sparkCombineFn::extractOutput)
                .map(TranslationUtils.fromPairFunction())
                .map(TranslationUtils.toKVByWindowInValue());

        context.putDataset(transform, new BoundedDataset<>(outRdd));
      }

      @Override
      public String toNativeString() {
        return "combineByKey(..., new <fn>(), ...)";
      }
    };
  }

  private static <InputT, OutputT> TransformEvaluator<ParDo.MultiOutput<InputT, OutputT>> parDo() {
    return new TransformEvaluator<ParDo.MultiOutput<InputT, OutputT>>() {
      @Override
      @SuppressWarnings("unchecked")
      public void evaluate(
          ParDo.MultiOutput<InputT, OutputT> transform, EvaluationContext context) {
        String stepName = context.getCurrentTransform().getFullName();
        DoFn<InputT, OutputT> doFn = transform.getFn();
        rejectSplittable(doFn);
        JavaRDD<WindowedValue<InputT>> inRDD =
            ((BoundedDataset<InputT>) context.borrowDataset(transform)).getRDD();
        WindowingStrategy<?, ?> windowingStrategy =
            context.getInput(transform).getWindowingStrategy();
        Accumulator<MetricsContainerStepMap> metricsAccum = MetricsAccumulator.getInstance();

        JavaPairRDD<TupleTag<?>, WindowedValue<?>> all;

        DoFnSignature signature = DoFnSignatures.getSignature(transform.getFn().getClass());
        boolean stateful = signature.stateDeclarations().size() > 0
            || signature.timerDeclarations().size() > 0;

        MultiDoFnFunction<InputT, OutputT> multiDoFnFunction = new MultiDoFnFunction<>(
            metricsAccum,
            stepName,
            doFn,
            context.getSerializableOptions(),
            transform.getMainOutputTag(),
            transform.getAdditionalOutputTags().getAll(),
            TranslationUtils.getSideInputs(transform.getSideInputs(), context),
            windowingStrategy,
            stateful);

        if (stateful) {
          // Based on the fact that the signature is stateful, DoFnSignatures ensures
          // that it is also keyed
          all = statefulParDoTransform(
              (KvCoder) context.getInput(transform).getCoder(),
              windowingStrategy.getWindowFn().windowCoder(),
              (JavaRDD) inRDD,
              (MultiDoFnFunction) multiDoFnFunction);
        } else {
          all = inRDD.mapPartitionsToPair(multiDoFnFunction);
        }

        Map<TupleTag<?>, PValue> outputs = context.getOutputs(transform);
        if (outputs.size() > 1) {
          StorageLevel level = StorageLevel.fromString(context.storageLevel());
          if (avoidRddSerialization(level)) {
            // if it is memory only reduce the overhead of moving to bytes
            all = all.persist(level);
          } else {
            // Caching can cause Serialization, we need to code to bytes
            // more details in https://issues.apache.org/jira/browse/BEAM-2669
            Map<TupleTag<?>, Coder<WindowedValue<?>>> coderMap =
                TranslationUtils.getTupleTagCoders(outputs);
            all = all
                .mapToPair(TranslationUtils.getTupleTagEncodeFunction(coderMap))
                .persist(level)
                .mapToPair(TranslationUtils.getTupleTagDecodeFunction(coderMap));
          }
        }
        for (Map.Entry<TupleTag<?>, PValue> output : outputs.entrySet()) {
          JavaPairRDD<TupleTag<?>, WindowedValue<?>> filtered =
              all.filter(new TranslationUtils.TupleTagFilter(output.getKey()));
          // Object is the best we can do since different outputs can have different tags
          JavaRDD<WindowedValue<Object>> values =
              (JavaRDD<WindowedValue<Object>>) (JavaRDD<?>) filtered.values();
          context.putDataset(output.getValue(), new BoundedDataset<>(values), false);
        }
      }

      @Override
      public String toNativeString() {
        return "mapPartitions(new <fn>())";
      }
    };
  }

  private static <K, V, OutputT> JavaPairRDD<TupleTag<?>, WindowedValue<?>> statefulParDoTransform(
      KvCoder<K, V> kvCoder,
      Coder<? extends BoundedWindow> windowCoder,
      JavaRDD<WindowedValue<KV<K, V>>> kvInRDD,
      MultiDoFnFunction<KV<K, V>, OutputT> doFnFunction) {
    Coder<K> keyCoder = kvCoder.getKeyCoder();

    final WindowedValue.WindowedValueCoder<V> wvCoder = WindowedValue.FullWindowedValueCoder.of(
        kvCoder.getValueCoder(), windowCoder);

    JavaRDD<WindowedValue<KV<K, Iterable<WindowedValue<V>>>>> groupRDD =
        GroupCombineFunctions.groupByKeyOnly(kvInRDD, keyCoder, wvCoder);

    return groupRDD
        .map(
            input -> {
              final K key = input.getValue().getKey();
              Iterable<WindowedValue<V>> value = input.getValue().getValue();
              return FluentIterable.from(value)
                  .transform(
                      windowedValue ->
                          windowedValue.withValue(KV.of(key, windowedValue.getValue())))
                  .iterator();
            })
        .flatMapToPair(doFnFunction);
  }

  private static <T> TransformEvaluator<Read.Bounded<T>> readBounded() {
    return new TransformEvaluator<Read.Bounded<T>>() {
      @Override
      public void evaluate(Read.Bounded<T> transform, EvaluationContext context) {
        String stepName = context.getCurrentTransform().getFullName();
        final JavaSparkContext jsc = context.getSparkContext();
        // create an RDD from a BoundedSource.
        JavaRDD<WindowedValue<T>> input =
            new SourceRDD.Bounded<>(
                    jsc.sc(), transform.getSource(), context.getSerializableOptions(), stepName)
                .toJavaRDD();
        // cache to avoid re-evaluation of the source by Spark's lazy DAG evaluation.
        context.putDataset(transform, new BoundedDataset<>(input), true);
      }

      @Override
      public String toNativeString() {
        return "sparkContext.<readFrom(<source>)>()";
      }
    };
  }

  private static <T, W extends BoundedWindow> TransformEvaluator<Window.Assign<T>> window() {
    return new TransformEvaluator<Window.Assign<T>>() {
      @Override
      public void evaluate(Window.Assign<T> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaRDD<WindowedValue<T>> inRDD =
            ((BoundedDataset<T>) context.borrowDataset(transform)).getRDD();

        if (TranslationUtils.skipAssignWindows(transform, context)) {
          context.putDataset(transform, new BoundedDataset<>(inRDD));
        } else {
          context.putDataset(transform, new BoundedDataset<>(
              inRDD.map(new SparkAssignWindowFn<>(transform.getWindowFn()))));
        }
      }

      @Override
      public String toNativeString() {
        return "map(new <windowFn>())";
      }
    };
  }

  private static <T> TransformEvaluator<Create.Values<T>> create() {
    return new TransformEvaluator<Create.Values<T>>() {
      @Override
      public void evaluate(Create.Values<T> transform, EvaluationContext context) {
        Iterable<T> elems = transform.getElements();
        // Use a coder to convert the objects in the PCollection to byte arrays, so they
        // can be transferred over the network.
        Coder<T> coder = context.getOutput(transform).getCoder();
        context.putBoundedDatasetFromValues(transform, elems, coder);
      }

      @Override
      public String toNativeString() {
        return "sparkContext.parallelize(Arrays.asList(...))";
      }
    };
  }

  private static <ReadT, WriteT> TransformEvaluator<View.CreatePCollectionView<ReadT, WriteT>>
  createPCollView() {
    return new TransformEvaluator<View.CreatePCollectionView<ReadT, WriteT>>() {
      @Override
      public void evaluate(View.CreatePCollectionView<ReadT, WriteT> transform,
                           EvaluationContext context) {
        Iterable<? extends WindowedValue<?>> iter =
            context.getWindowedValues(context.getInput(transform));
        PCollectionView<WriteT> output = transform.getView();
        Coder<Iterable<WindowedValue<?>>> coderInternal =
            (Coder) IterableCoder.of(
                WindowedValue.getFullCoder(
                    output.getCoderInternal(),
                    output.getWindowingStrategyInternal().getWindowFn().windowCoder()));

        @SuppressWarnings("unchecked")
        Iterable<WindowedValue<?>> iterCast =  (Iterable<WindowedValue<?>>) iter;

        context.putPView(output, iterCast, coderInternal);
      }

      @Override
      public String toNativeString() {
        return "<createPCollectionView>";
      }
    };
  }

  private static <K, V, W extends BoundedWindow> TransformEvaluator<Reshuffle<K, V>> reshuffle() {
    return new TransformEvaluator<Reshuffle<K, V>>() {
      @Override public void evaluate(Reshuffle<K, V> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaRDD<WindowedValue<KV<K, V>>> inRDD =
            ((BoundedDataset<KV<K, V>>) context.borrowDataset(transform)).getRDD();
        @SuppressWarnings("unchecked")
        final WindowingStrategy<?, W> windowingStrategy =
            (WindowingStrategy<?, W>) context.getInput(transform).getWindowingStrategy();
        @SuppressWarnings("unchecked")
        final KvCoder<K, V> coder = (KvCoder<K, V>) context.getInput(transform).getCoder();
        @SuppressWarnings("unchecked")
        final WindowFn<Object, W> windowFn = (WindowFn<Object, W>) windowingStrategy.getWindowFn();

        final Coder<K> keyCoder = coder.getKeyCoder();
        final WindowedValue.WindowedValueCoder<V> wvCoder =
            WindowedValue.FullWindowedValueCoder.of(coder.getValueCoder(), windowFn.windowCoder());

        JavaRDD<WindowedValue<KV<K, V>>> reshuffled =
            GroupCombineFunctions.reshuffle(inRDD, keyCoder, wvCoder);

        context.putDataset(transform, new BoundedDataset<>(reshuffled));
      }

      @Override
      public String toNativeString() {
        return "repartition(...)";
      }
    };
  }

  private static final Map<Class<? extends PTransform>, TransformEvaluator<?>> EVALUATORS = Maps
      .newHashMap();

  static {
    EVALUATORS.put(Read.Bounded.class, readBounded());
    EVALUATORS.put(ParDo.MultiOutput.class, parDo());
    EVALUATORS.put(GroupByKey.class, groupByKey());
    EVALUATORS.put(Combine.GroupedValues.class, combineGrouped());
    EVALUATORS.put(Combine.Globally.class, combineGlobally());
    EVALUATORS.put(Combine.PerKey.class, combinePerKey());
    EVALUATORS.put(Flatten.PCollections.class, flattenPColl());
    EVALUATORS.put(Create.Values.class, create());
//    EVALUATORS.put(View.AsSingleton.class, viewAsSingleton());
//    EVALUATORS.put(View.AsIterable.class, viewAsIter());
    EVALUATORS.put(View.CreatePCollectionView.class, createPCollView());
    EVALUATORS.put(Window.Assign.class, window());
    EVALUATORS.put(Reshuffle.class, reshuffle());
  }

  /**
   * Translator matches Beam transformation with the appropriate evaluator.
   */
  public static class Translator implements SparkPipelineTranslator {

    @Override
    public boolean hasTranslation(Class<? extends PTransform<?, ?>> clazz) {
      return EVALUATORS.containsKey(clazz);
    }

    @Override
    public <TransformT extends PTransform<?, ?>> TransformEvaluator<TransformT>
        translateBounded (Class<TransformT> clazz) {
      @SuppressWarnings("unchecked") TransformEvaluator<TransformT> transformEvaluator =
          (TransformEvaluator<TransformT>) EVALUATORS.get(clazz);
      checkState(transformEvaluator != null,
          "No TransformEvaluator registered for BOUNDED transform %s", clazz);
      return transformEvaluator;
    }

    @Override
    public <TransformT extends PTransform<?, ?>> TransformEvaluator<TransformT>
        translateUnbounded(Class<TransformT> clazz) {
      throw new IllegalStateException("TransformTranslator used in a batch pipeline only "
          + "supports BOUNDED transforms.");
    }
  }
}
