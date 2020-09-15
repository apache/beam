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

import static org.apache.beam.runners.spark.translation.TranslationUtils.canAvoidRddSerialization;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.io.SourceRDD;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.metrics.MetricsContainerStepMapAccumulator;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.runners.spark.util.SideInputBroadcast;
import org.apache.beam.runners.spark.util.SparkCompat;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.AbstractIterator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.FluentIterable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.storage.StorageLevel;
import org.checkerframework.checker.nullness.qual.Nullable;
import scala.Tuple2;

/** Supports translation between a Beam transform, and Spark's operations on RDDs. */
public final class TransformTranslator {

  private TransformTranslator() {}

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
        final KvCoder<K, V> coder = (KvCoder<K, V>) context.getInput(transform).getCoder();
        @SuppressWarnings("unchecked")
        final WindowingStrategy<?, W> windowingStrategy =
            (WindowingStrategy<?, W>) context.getInput(transform).getWindowingStrategy();
        @SuppressWarnings("unchecked")
        final WindowFn<Object, W> windowFn = (WindowFn<Object, W>) windowingStrategy.getWindowFn();

        // --- coders.
        final Coder<K> keyCoder = coder.getKeyCoder();
        final WindowedValue.WindowedValueCoder<V> wvCoder =
            WindowedValue.FullWindowedValueCoder.of(coder.getValueCoder(), windowFn.windowCoder());

        JavaRDD<WindowedValue<KV<K, Iterable<V>>>> groupedByKey;
        Partitioner partitioner = getPartitioner(context);
        if (GroupNonMergingWindowsFunctions.isEligibleForGroupByWindow(windowingStrategy)) {
          // we can have a memory sensitive translation for non-merging windows
          groupedByKey =
              GroupNonMergingWindowsFunctions.groupByKeyAndWindow(
                  inRDD, keyCoder, coder.getValueCoder(), windowingStrategy, partitioner);
        } else {
          // --- group by key only.
          JavaRDD<KV<K, Iterable<WindowedValue<V>>>> groupedByKeyOnly =
              GroupCombineFunctions.groupByKeyOnly(inRDD, keyCoder, wvCoder, partitioner);

          // --- now group also by window.
          // for batch, GroupAlsoByWindow uses an in-memory StateInternals.
          groupedByKey =
              groupedByKeyOnly.flatMap(
                  new SparkGroupAlsoByWindowViaOutputBufferFn<>(
                      windowingStrategy,
                      new TranslationUtils.InMemoryStateInternalsFactory<>(),
                      SystemReduceFn.buffering(coder.getValueCoder()),
                      context.getSerializableOptions()));
        }
        context.putDataset(transform, new BoundedDataset<>(groupedByKey));
      }

      @Override
      public String toNativeString() {
        return "groupByKey()";
      }
    };
  }

  private static <K, InputT, OutputT>
      TransformEvaluator<Combine.GroupedValues<KV<K, InputT>, InputT, OutputT>> combineGrouped() {
    return new TransformEvaluator<Combine.GroupedValues<KV<K, InputT>, InputT, OutputT>>() {
      @Override
      public void evaluate(
          Combine.GroupedValues<KV<K, InputT>, InputT, OutputT> transform,
          EvaluationContext context) {
        @SuppressWarnings("unchecked")
        CombineWithContext.CombineFnWithContext<InputT, ?, OutputT> combineFn =
            (CombineWithContext.CombineFnWithContext<InputT, ?, OutputT>)
                CombineFnUtil.toFnWithContext(transform.getFn());
        final SparkCombineFn<KV<K, InputT>, InputT, ?, OutputT> sparkCombineFn =
            SparkCombineFn.keyed(
                combineFn,
                context.getSerializableOptions(),
                TranslationUtils.getSideInputs(transform.getSideInputs(), context),
                context.getInput(transform).getWindowingStrategy());

        @SuppressWarnings("unchecked")
        JavaRDD<WindowedValue<KV<K, Iterable<InputT>>>> inRDD =
            ((BoundedDataset<KV<K, Iterable<InputT>>>) context.borrowDataset(transform)).getRDD();

        @SuppressWarnings("unchecked")
        JavaRDD<WindowedValue<KV<K, OutputT>>> outRDD =
            inRDD.map(
                in ->
                    WindowedValue.of(
                        KV.of(
                            in.getValue().getKey(),
                            combineFn.apply(
                                in.getValue().getValue(), sparkCombineFn.ctxtForValue(in))),
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

  private static <InputT, AccumT, OutputT>
      TransformEvaluator<Combine.Globally<InputT, OutputT>> combineGlobally() {
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

        final SparkCombineFn<InputT, InputT, AccumT, OutputT> sparkCombineFn =
            SparkCombineFn.globally(
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

        SparkCombineFn.WindowedAccumulator<InputT, InputT, AccumT, ?> accumulated =
            GroupCombineFunctions.combineGlobally(inRdd, sparkCombineFn, aCoder, windowingStrategy);

        if (!accumulated.isEmpty()) {
          Iterable<WindowedValue<OutputT>> output = sparkCombineFn.extractOutput(accumulated);
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
                    .map(WindowedValue::valueInGlobalWindow);
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
        final KvCoder<K, InputT> inputCoder =
            (KvCoder<K, InputT>) context.getInput(transform).getCoder();
        @SuppressWarnings("unchecked")
        final CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn =
            (CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT>)
                CombineFnUtil.toFnWithContext(transform.getFn());
        final WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
        final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs =
            TranslationUtils.getSideInputs(transform.getSideInputs(), context);
        final SparkCombineFn<KV<K, InputT>, InputT, AccumT, OutputT> sparkCombineFn =
            SparkCombineFn.keyed(
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

        JavaPairRDD<K, SparkCombineFn.WindowedAccumulator<KV<K, InputT>, InputT, AccumT, ?>>
            accumulatePerKey;
        accumulatePerKey =
            GroupCombineFunctions.combinePerKey(
                inRdd,
                sparkCombineFn,
                inputCoder.getKeyCoder(),
                inputCoder.getValueCoder(),
                vaCoder,
                windowingStrategy);

        JavaPairRDD<K, WindowedValue<OutputT>> kwvs =
            SparkCompat.extractOutput(accumulatePerKey, sparkCombineFn);
        JavaRDD<WindowedValue<KV<K, OutputT>>> outRdd =
            kwvs.map(new TranslationUtils.FromPairFunction())
                .map(new TranslationUtils.ToKVByWindowInValueFunction<>());

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
        checkState(
            !DoFnSignatures.signatureForDoFn(doFn).processElement().isSplittable(),
            "Not expected to directly translate splittable DoFn, should have been overridden: %s",
            doFn);
        JavaRDD<WindowedValue<InputT>> inRDD =
            ((BoundedDataset<InputT>) context.borrowDataset(transform)).getRDD();
        WindowingStrategy<?, ?> windowingStrategy =
            context.getInput(transform).getWindowingStrategy();
        MetricsContainerStepMapAccumulator metricsAccum = MetricsAccumulator.getInstance();
        Coder<InputT> inputCoder = (Coder<InputT>) context.getInput(transform).getCoder();
        Map<TupleTag<?>, Coder<?>> outputCoders = context.getOutputCoders();
        JavaPairRDD<TupleTag<?>, WindowedValue<?>> all;

        DoFnSignature signature = DoFnSignatures.getSignature(transform.getFn().getClass());
        boolean stateful =
            signature.stateDeclarations().size() > 0 || signature.timerDeclarations().size() > 0;

        DoFnSchemaInformation doFnSchemaInformation;
        doFnSchemaInformation =
            ParDoTranslation.getSchemaInformation(context.getCurrentTransform());

        Map<String, PCollectionView<?>> sideInputMapping =
            ParDoTranslation.getSideInputMapping(context.getCurrentTransform());

        MultiDoFnFunction<InputT, OutputT> multiDoFnFunction =
            new MultiDoFnFunction<>(
                metricsAccum,
                stepName,
                doFn,
                context.getSerializableOptions(),
                transform.getMainOutputTag(),
                transform.getAdditionalOutputTags().getAll(),
                inputCoder,
                outputCoders,
                TranslationUtils.getSideInputs(transform.getSideInputs().values(), context),
                windowingStrategy,
                stateful,
                doFnSchemaInformation,
                sideInputMapping);

        if (stateful) {
          // Based on the fact that the signature is stateful, DoFnSignatures ensures
          // that it is also keyed
          all =
              statefulParDoTransform(
                  (KvCoder) context.getInput(transform).getCoder(),
                  windowingStrategy.getWindowFn().windowCoder(),
                  (JavaRDD) inRDD,
                  getPartitioner(context),
                  (MultiDoFnFunction) multiDoFnFunction,
                  signature.processElement().requiresTimeSortedInput());
        } else {
          all = inRDD.mapPartitionsToPair(multiDoFnFunction);
        }

        Map<TupleTag<?>, PValue> outputs = context.getOutputs(transform);
        if (outputs.size() > 1) {
          StorageLevel level = StorageLevel.fromString(context.storageLevel());
          if (canAvoidRddSerialization(level)) {
            // if it is memory only reduce the overhead of moving to bytes
            all = all.persist(level);
          } else {
            // Caching can cause Serialization, we need to code to bytes
            // more details in https://issues.apache.org/jira/browse/BEAM-2669
            Map<TupleTag<?>, Coder<WindowedValue<?>>> coderMap =
                TranslationUtils.getTupleTagCoders(outputs);
            all =
                all.mapToPair(TranslationUtils.getTupleTagEncodeFunction(coderMap))
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
          context.putDataset(output.getValue(), new BoundedDataset<>(values));
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
      Partitioner partitioner,
      MultiDoFnFunction<KV<K, V>, OutputT> doFnFunction,
      boolean requiresSortedInput) {
    Coder<K> keyCoder = kvCoder.getKeyCoder();

    final WindowedValue.WindowedValueCoder<V> wvCoder =
        WindowedValue.FullWindowedValueCoder.of(kvCoder.getValueCoder(), windowCoder);

    if (!requiresSortedInput) {
      return GroupCombineFunctions.groupByKeyOnly(kvInRDD, keyCoder, wvCoder, partitioner)
          .map(
              input -> {
                final K key = input.getKey();
                Iterable<WindowedValue<V>> value = input.getValue();
                return FluentIterable.from(value)
                    .transform(
                        windowedValue ->
                            windowedValue.withValue(KV.of(key, windowedValue.getValue())))
                    .iterator();
              })
          .flatMapToPair(doFnFunction);
    }

    JavaPairRDD<ByteArray, byte[]> pairRDD =
        kvInRDD
            .map(new ReifyTimestampsAndWindowsFunction<>())
            .mapToPair(TranslationUtils.toPairFunction())
            .mapToPair(
                CoderHelpers.toByteFunctionWithTs(keyCoder, wvCoder, in -> in._2().getTimestamp()));

    JavaPairRDD<ByteArray, byte[]> sorted =
        pairRDD.repartitionAndSortWithinPartitions(keyPrefixPartitionerFrom(partitioner));

    return sorted.mapPartitionsToPair(wrapDoFnFromSortedRDD(doFnFunction, keyCoder, wvCoder));
  }

  private static Partitioner keyPrefixPartitionerFrom(Partitioner partitioner) {
    return new Partitioner() {
      @Override
      public int numPartitions() {
        return partitioner.numPartitions();
      }

      @Override
      public int getPartition(Object o) {
        ByteArray b = (ByteArray) o;
        return partitioner.getPartition(
            new ByteArray(Arrays.copyOfRange(b.getValue(), 0, b.getValue().length - 8)));
      }
    };
  }

  private static <K, V, OutputT>
      PairFlatMapFunction<Iterator<Tuple2<ByteArray, byte[]>>, TupleTag<?>, WindowedValue<?>>
          wrapDoFnFromSortedRDD(
              MultiDoFnFunction<KV<K, V>, OutputT> doFnFunction,
              Coder<K> keyCoder,
              Coder<WindowedValue<V>> wvCoder) {

    return (Iterator<Tuple2<ByteArray, byte[]>> in) -> {
      Iterator<Iterator<Tuple2<TupleTag<?>, WindowedValue<?>>>> mappedGroups;
      mappedGroups =
          Iterators.transform(
              splitBySameKey(in, keyCoder, wvCoder),
              group -> {
                try {
                  return doFnFunction.call(group);
                } catch (Exception ex) {
                  throw new RuntimeException(ex);
                }
              });
      return flatten(mappedGroups);
    };
  }

  @VisibleForTesting
  static <T> Iterator<T> flatten(final Iterator<Iterator<T>> toFlatten) {

    return new AbstractIterator<T>() {

      @Nullable Iterator<T> current = null;

      @Override
      protected T computeNext() {
        while (true) {
          if (current == null) {
            if (toFlatten.hasNext()) {
              current = toFlatten.next();
            } else {
              return endOfData();
            }
          }
          if (current.hasNext()) {
            return current.next();
          }
          current = null;
        }
      }
    };
  }

  @VisibleForTesting
  static <K, V> Iterator<Iterator<WindowedValue<KV<K, V>>>> splitBySameKey(
      Iterator<Tuple2<ByteArray, byte[]>> in, Coder<K> keyCoder, Coder<WindowedValue<V>> wvCoder) {

    return new AbstractIterator<Iterator<WindowedValue<KV<K, V>>>>() {

      @Nullable Tuple2<ByteArray, byte[]> read = null;

      @Override
      protected Iterator<WindowedValue<KV<K, V>>> computeNext() {
        readNext();
        if (read != null) {
          byte[] value = read._1().getValue();
          byte[] keyPart = Arrays.copyOfRange(value, 0, value.length - 8);
          K key = CoderHelpers.fromByteArray(keyPart, keyCoder);
          return createIteratorForKey(keyPart, key);
        }
        return endOfData();
      }

      private void readNext() {
        if (read == null) {
          if (in.hasNext()) {
            read = in.next();
          }
        }
      }

      private void consumed() {
        read = null;
      }

      private Iterator<WindowedValue<KV<K, V>>> createIteratorForKey(byte[] keyPart, K key) {

        return new AbstractIterator<WindowedValue<KV<K, V>>>() {
          @Override
          protected WindowedValue<KV<K, V>> computeNext() {
            readNext();
            if (read != null) {
              byte[] value = read._1().getValue();
              byte[] prefix = Arrays.copyOfRange(value, 0, value.length - 8);
              if (Arrays.equals(prefix, keyPart)) {
                WindowedValue<V> wv = CoderHelpers.fromByteArray(read._2(), wvCoder);
                consumed();
                return WindowedValue.of(
                    KV.of(key, wv.getValue()), wv.getTimestamp(), wv.getWindows(), wv.getPane());
              }
            }
            return endOfData();
          }
        };
      }
    };
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

        context.putDataset(transform, new BoundedDataset<>(input));
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
          context.putDataset(
              transform,
              new BoundedDataset<>(inRDD.map(new SparkAssignWindowFn<>(transform.getWindowFn()))));
        }
      }

      @Override
      public String toNativeString() {
        return "map(new <windowFn>())";
      }
    };
  }

  private static <ReadT, WriteT>
      TransformEvaluator<View.CreatePCollectionView<ReadT, WriteT>> createPCollView() {
    return new TransformEvaluator<View.CreatePCollectionView<ReadT, WriteT>>() {
      @Override
      public void evaluate(
          View.CreatePCollectionView<ReadT, WriteT> transform, EvaluationContext context) {
        Iterable<? extends WindowedValue<?>> iter =
            context.getWindowedValues(context.getInput(transform));
        PCollectionView<WriteT> output = transform.getView();
        Coder<Iterable<WindowedValue<?>>> coderInternal =
            (Coder)
                IterableCoder.of(
                    WindowedValue.getFullCoder(
                        output.getCoderInternal(),
                        output.getWindowingStrategyInternal().getWindowFn().windowCoder()));

        @SuppressWarnings("unchecked")
        Iterable<WindowedValue<?>> iterCast = (Iterable<WindowedValue<?>>) iter;

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
      @Override
      public void evaluate(Reshuffle<K, V> transform, EvaluationContext context) {
        @SuppressWarnings("unchecked")
        JavaRDD<WindowedValue<KV<K, V>>> inRDD =
            ((BoundedDataset<KV<K, V>>) context.borrowDataset(transform)).getRDD();
        @SuppressWarnings("unchecked")
        final WindowingStrategy<?, W> windowingStrategy =
            (WindowingStrategy<?, W>) context.getInput(transform).getWindowingStrategy();
        final KvCoder<K, V> coder = (KvCoder<K, V>) context.getInput(transform).getCoder();
        @SuppressWarnings("unchecked")
        final WindowFn<Object, W> windowFn = (WindowFn<Object, W>) windowingStrategy.getWindowFn();

        final WindowedValue.WindowedValueCoder<KV<K, V>> wvCoder =
            WindowedValue.FullWindowedValueCoder.of(coder, windowFn.windowCoder());

        JavaRDD<WindowedValue<KV<K, V>>> reshuffled =
            GroupCombineFunctions.reshuffle(inRDD, wvCoder);

        context.putDataset(transform, new BoundedDataset<>(reshuffled));
      }

      @Override
      public String toNativeString() {
        return "repartition(...)";
      }
    };
  }

  private static @Nullable Partitioner getPartitioner(EvaluationContext context) {
    Long bundleSize =
        context.getSerializableOptions().get().as(SparkPipelineOptions.class).getBundleSize();
    return (bundleSize > 0)
        ? null
        : new HashPartitioner(context.getSparkContext().defaultParallelism());
  }

  private static final Map<String, TransformEvaluator<?>> EVALUATORS = new HashMap<>();

  static {
    EVALUATORS.put(PTransformTranslation.READ_TRANSFORM_URN, readBounded());
    EVALUATORS.put(PTransformTranslation.PAR_DO_TRANSFORM_URN, parDo());
    EVALUATORS.put(PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, groupByKey());
    EVALUATORS.put(PTransformTranslation.COMBINE_GROUPED_VALUES_TRANSFORM_URN, combineGrouped());
    EVALUATORS.put(PTransformTranslation.COMBINE_GLOBALLY_TRANSFORM_URN, combineGlobally());
    EVALUATORS.put(PTransformTranslation.COMBINE_PER_KEY_TRANSFORM_URN, combinePerKey());
    EVALUATORS.put(PTransformTranslation.FLATTEN_TRANSFORM_URN, flattenPColl());
    EVALUATORS.put(PTransformTranslation.CREATE_VIEW_TRANSFORM_URN, createPCollView());
    EVALUATORS.put(PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, window());
    EVALUATORS.put(PTransformTranslation.RESHUFFLE_URN, reshuffle());
    // LinkedIn: load customized TranslationEvaluators
    EVALUATORS.putAll(TransformEvaluatorRegistrar.loadTransformEvaluators());
  }

  private static @Nullable TransformEvaluator<?> getTranslator(PTransform<?, ?> transform) {
    @Nullable String urn = PTransformTranslation.urnForTransformOrNull(transform);
    return urn == null ? null : EVALUATORS.get(urn);
  }

  /** Translator matches Beam transformation with the appropriate evaluator. */
  public static class Translator implements SparkPipelineTranslator {

    @Override
    public boolean hasTranslation(PTransform<?, ?> transform) {
      return EVALUATORS.containsKey(PTransformTranslation.urnForTransformOrNull(transform));
    }

    @Override
    public <TransformT extends PTransform<?, ?>> TransformEvaluator<TransformT> translateBounded(
        PTransform<?, ?> transform) {
      @SuppressWarnings("unchecked")
      TransformEvaluator<TransformT> transformEvaluator =
          (TransformEvaluator<TransformT>) getTranslator(transform);
      checkState(
          transformEvaluator != null,
          "No TransformEvaluator registered for BOUNDED transform %s",
          transform);
      return transformEvaluator;
    }

    @Override
    public <TransformT extends PTransform<?, ?>> TransformEvaluator<TransformT> translateUnbounded(
        PTransform<?, ?> transform) {
      throw new IllegalStateException(
          "TransformTranslator used in a batch pipeline only " + "supports BOUNDED transforms.");
    }
  }
}
