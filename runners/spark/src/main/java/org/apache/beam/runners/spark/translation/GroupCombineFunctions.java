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


import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.runners.core.GroupAlsoByWindowsViaOutputBufferDoFn;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.spark.aggregators.NamedAggregators;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.util.BroadcastHelper;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;



/**
 * A set of group/combine functions to apply to Spark {@link org.apache.spark.rdd.RDD}s.
 */
public class GroupCombineFunctions {

  /**
   * Apply {@link org.apache.beam.sdk.transforms.GroupByKey} to a Spark RDD.
   */
  public static <K, V,  W extends BoundedWindow> JavaRDD<WindowedValue<KV<K,
      Iterable<V>>>> groupByKey(JavaRDD<WindowedValue<KV<K, V>>> rdd,
                                Accumulator<NamedAggregators> accum,
                                KvCoder<K, V> coder,
                                SparkRuntimeContext runtimeContext,
                                WindowingStrategy<?, W> windowingStrategy) {
    //--- coders.
    final Coder<K> keyCoder = coder.getKeyCoder();
    final Coder<V> valueCoder = coder.getValueCoder();
    final WindowedValue.WindowedValueCoder<V> wvCoder = WindowedValue.FullWindowedValueCoder.of(
        valueCoder, windowingStrategy.getWindowFn().windowCoder());

    //--- groupByKey.
    // Use coders to convert objects in the PCollection to byte arrays, so they
    // can be transferred over the network for the shuffle.
    JavaRDD<WindowedValue<KV<K, Iterable<WindowedValue<V>>>>> groupedByKey =
        rdd.map(new ReifyTimestampsAndWindowsFunction<K, V>())
            .map(WindowingHelpers.<KV<K, WindowedValue<V>>>unwindowFunction())
            .mapToPair(TranslationUtils.<K, WindowedValue<V>>toPairFunction())
            .mapToPair(CoderHelpers.toByteFunction(keyCoder, wvCoder))
            .groupByKey()
            .mapToPair(CoderHelpers.fromByteFunctionIterable(keyCoder, wvCoder))
            // empty windows are OK here, see GroupByKey#evaluateHelper in the SDK
            .map(TranslationUtils.<K, Iterable<WindowedValue<V>>>fromPairFunction())
            .map(WindowingHelpers.<KV<K, Iterable<WindowedValue<V>>>>windowFunction());

    //--- now group also by window.
    @SuppressWarnings("unchecked")
    WindowFn<Object, W> windowFn = (WindowFn<Object, W>) windowingStrategy.getWindowFn();
    // GroupAlsoByWindow current uses a dummy in-memory StateInternals
    OldDoFn<KV<K, Iterable<WindowedValue<V>>>, KV<K, Iterable<V>>> gabwDoFn =
        new GroupAlsoByWindowsViaOutputBufferDoFn<K, V, Iterable<V>, W>(
            windowingStrategy, new TranslationUtils.InMemoryStateInternalsFactory<K>(),
                SystemReduceFn.<K, V, W>buffering(valueCoder));
    return groupedByKey.mapPartitions(new DoFnFunction<>(accum, gabwDoFn, runtimeContext, null,
        windowFn));
  }

  /**
   * Apply a composite {@link org.apache.beam.sdk.transforms.Combine.Globally} transformation.
   */
  public static <InputT, AccumT, OutputT> JavaRDD<WindowedValue<OutputT>>
  combineGlobally(JavaRDD<WindowedValue<InputT>> rdd,
                  final CombineWithContext.CombineFnWithContext<InputT, AccumT, OutputT> combineFn,
                  final Coder<InputT> iCoder,
                  final Coder<OutputT> oCoder,
                  final SparkRuntimeContext runtimeContext,
                  final WindowingStrategy<?, ?> windowingStrategy,
                  final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, BroadcastHelper<?>>>
                      sideInputs,
                  boolean hasDefault) {
    // handle empty input RDD, which will natively skip the entire execution as Spark will not
    // run on empty RDDs.
    if (rdd.isEmpty()) {
      JavaSparkContext jsc = new JavaSparkContext(rdd.context());
      if (hasDefault) {
        OutputT defaultValue = combineFn.defaultValue();
        return jsc
            .parallelize(Lists.newArrayList(CoderHelpers.toByteArray(defaultValue, oCoder)))
            .map(CoderHelpers.fromByteFunction(oCoder))
            .map(WindowingHelpers.<OutputT>windowFunction());
      } else {
        return jsc.emptyRDD();
      }
    }

    //--- coders.
    final Coder<AccumT> aCoder;
    try {
      aCoder = combineFn.getAccumulatorCoder(runtimeContext.getCoderRegistry(), iCoder);
    } catch (CannotProvideCoderException e) {
      throw new IllegalStateException("Could not determine coder for accumulator", e);
    }
    // windowed coders.
    final WindowedValue.FullWindowedValueCoder<InputT> wviCoder =
        WindowedValue.FullWindowedValueCoder.of(iCoder,
            windowingStrategy.getWindowFn().windowCoder());
    final WindowedValue.FullWindowedValueCoder<AccumT> wvaCoder =
        WindowedValue.FullWindowedValueCoder.of(aCoder,
            windowingStrategy.getWindowFn().windowCoder());
    final WindowedValue.FullWindowedValueCoder<OutputT> wvoCoder =
        WindowedValue.FullWindowedValueCoder.of(oCoder,
            windowingStrategy.getWindowFn().windowCoder());

    final SparkGlobalCombineFn<InputT, AccumT, OutputT> sparkCombineFn =
        new SparkGlobalCombineFn<>(combineFn, runtimeContext, sideInputs, windowingStrategy);
    final IterableCoder<WindowedValue<AccumT>> iterAccumCoder = IterableCoder.of(wvaCoder);


    // Use coders to convert objects in the PCollection to byte arrays, so they
    // can be transferred over the network for the shuffle.
    JavaRDD<byte[]> inRddBytes = rdd.map(CoderHelpers.toByteFunction(wviCoder));
    /*AccumT*/ byte[] acc = inRddBytes.aggregate(
        CoderHelpers.toByteArray(sparkCombineFn.zeroValue(), iterAccumCoder),
        new Function2</*AccumT*/ byte[], /*InputT*/ byte[], /*AccumT*/ byte[]>() {
          @Override
          public /*AccumT*/ byte[] call(/*AccumT*/ byte[] ab, /*InputT*/ byte[] ib)
              throws Exception {
            Iterable<WindowedValue<AccumT>> a = CoderHelpers.fromByteArray(ab, iterAccumCoder);
            WindowedValue<InputT> i = CoderHelpers.fromByteArray(ib, wviCoder);
            return CoderHelpers.toByteArray(sparkCombineFn.seqOp(a, i), iterAccumCoder);
          }
        },
        new Function2</*AccumT*/ byte[], /*AccumT*/ byte[], /*AccumT*/ byte[]>() {
          @Override
          public /*AccumT*/ byte[] call(/*AccumT*/ byte[] a1b, /*AccumT*/ byte[] a2b)
              throws Exception {
            Iterable<WindowedValue<AccumT>> a1 = CoderHelpers.fromByteArray(a1b, iterAccumCoder);
            Iterable<WindowedValue<AccumT>> a2 = CoderHelpers.fromByteArray(a2b, iterAccumCoder);
            Iterable<WindowedValue<AccumT>> merged = sparkCombineFn.combOp(a1, a2);
            return CoderHelpers.toByteArray(merged, iterAccumCoder);
          }
        }
    );
    Iterable<WindowedValue<OutputT>> output =
        sparkCombineFn.extractOutput(CoderHelpers.fromByteArray(acc, iterAccumCoder));
    return new JavaSparkContext(rdd.context()).parallelize(
        CoderHelpers.toByteArrays(output, wvoCoder)).map(CoderHelpers.fromByteFunction(wvoCoder));
  }

  /**
   * Apply a composite {@link org.apache.beam.sdk.transforms.Combine.PerKey} transformation.
   * <p>
   * This aggregation will apply Beam's {@link org.apache.beam.sdk.transforms.Combine.CombineFn}
   * via Spark's {@link JavaPairRDD#combineByKey(Function, Function2, Function2)} aggregation.
   * </p>
   * For streaming, this will be called from within a serialized context
   * (DStream's transform callback), so passed arguments need to be Serializable.
   */
  public static <K, InputT, AccumT, OutputT> JavaRDD<WindowedValue<KV<K, OutputT>>>
  combinePerKey(JavaRDD<WindowedValue<KV<K, InputT>>> rdd,
                final CombineWithContext.KeyedCombineFnWithContext<K, InputT, AccumT, OutputT>
                    combineFn,
                final KvCoder<K, InputT> inputCoder,
                final SparkRuntimeContext runtimeContext,
                final WindowingStrategy<?, ?> windowingStrategy,
                final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>,
                    BroadcastHelper<?>>> sideInputs) {
    //--- coders.
    final Coder<K> keyCoder = inputCoder.getKeyCoder();
    final Coder<InputT> viCoder = inputCoder.getValueCoder();
    final Coder<AccumT> vaCoder;
    try {
      vaCoder = combineFn.getAccumulatorCoder(runtimeContext.getCoderRegistry(), keyCoder, viCoder);
    } catch (CannotProvideCoderException e) {
      throw new IllegalStateException("Could not determine coder for accumulator", e);
    }
    // windowed coders.
    final WindowedValue.FullWindowedValueCoder<KV<K, InputT>> wkviCoder =
        WindowedValue.FullWindowedValueCoder.of(KvCoder.of(keyCoder, viCoder),
            windowingStrategy.getWindowFn().windowCoder());
    final WindowedValue.FullWindowedValueCoder<KV<K, AccumT>> wkvaCoder =
        WindowedValue.FullWindowedValueCoder.of(KvCoder.of(keyCoder, vaCoder),
            windowingStrategy.getWindowFn().windowCoder());

    // We need to duplicate K as both the key of the JavaPairRDD as well as inside the value,
    // since the functions passed to combineByKey don't receive the associated key of each
    // value, and we need to map back into methods in Combine.KeyedCombineFn, which each
    // require the key in addition to the InputT's and AccumT's being merged/accumulated.
    // Once Spark provides a way to include keys in the arguments of combine/merge functions,
    // we won't need to duplicate the keys anymore.
    // Key has to bw windowed in order to group by window as well.
    JavaPairRDD<K, WindowedValue<KV<K, InputT>>> inRddDuplicatedKeyPair =
        rdd.flatMapToPair(
            new PairFlatMapFunction<WindowedValue<KV<K, InputT>>, K,
                WindowedValue<KV<K, InputT>>>() {
              @Override
              public Iterable<Tuple2<K, WindowedValue<KV<K, InputT>>>>
              call(WindowedValue<KV<K, InputT>> wkv) {
                return Collections.singletonList(new Tuple2<>(wkv.getValue().getKey(), wkv));
              }
            });

    final SparkKeyedCombineFn<K, InputT, AccumT, OutputT> sparkCombineFn =
        new SparkKeyedCombineFn<>(combineFn, runtimeContext, sideInputs, windowingStrategy);
    final IterableCoder<WindowedValue<KV<K, AccumT>>> iterAccumCoder = IterableCoder.of(wkvaCoder);

    // Use coders to convert objects in the PCollection to byte arrays, so they
    // can be transferred over the network for the shuffle.
    JavaPairRDD<ByteArray, byte[]> inRddDuplicatedKeyPairBytes = inRddDuplicatedKeyPair
        .mapToPair(CoderHelpers.toByteFunction(keyCoder, wkviCoder));

    // The output of combineByKey will be "AccumT" (accumulator)
    // types rather than "OutputT" (final output types) since Combine.CombineFn
    // only provides ways to merge VAs, and no way to merge VOs.
    JavaPairRDD</*K*/ ByteArray, /*KV<K, AccumT>*/ byte[]> accumulatedBytes =
        inRddDuplicatedKeyPairBytes.combineByKey(
        new Function</*KV<K, InputT>*/ byte[], /*KV<K, AccumT>*/ byte[]>() {
          @Override
          public /*KV<K, AccumT>*/ byte[] call(/*KV<K, InputT>*/ byte[] input) {
            WindowedValue<KV<K, InputT>> wkvi = CoderHelpers.fromByteArray(input, wkviCoder);
            return CoderHelpers.toByteArray(sparkCombineFn.createCombiner(wkvi), iterAccumCoder);
          }
        },
        new Function2</*KV<K, AccumT>*/ byte[], /*KV<K, InputT>*/ byte[],
            /*KV<K, AccumT>*/ byte[]>() {
          @Override
          public /*KV<K, AccumT>*/ byte[] call(/*KV<K, AccumT>*/ byte[] acc,
              /*KV<K, InputT>*/ byte[] input) {
            Iterable<WindowedValue<KV<K, AccumT>>> wkvas =
                CoderHelpers.fromByteArray(acc, iterAccumCoder);
            WindowedValue<KV<K, InputT>> wkvi = CoderHelpers.fromByteArray(input, wkviCoder);
            return CoderHelpers.toByteArray(sparkCombineFn.mergeValue(wkvi, wkvas), iterAccumCoder);
          }
        },
        new Function2</*KV<K, AccumT>*/ byte[], /*KV<K, AccumT>*/ byte[],
            /*KV<K, AccumT>*/ byte[]>() {
          @Override
          public /*KV<K, AccumT>*/ byte[] call(/*KV<K, AccumT>*/ byte[] acc1,
              /*KV<K, AccumT>*/ byte[] acc2) {
            Iterable<WindowedValue<KV<K, AccumT>>> wkvas1 =
                CoderHelpers.fromByteArray(acc1, iterAccumCoder);
            Iterable<WindowedValue<KV<K, AccumT>>> wkvas2 =
                CoderHelpers.fromByteArray(acc2, iterAccumCoder);
            return CoderHelpers.toByteArray(sparkCombineFn.mergeCombiners(wkvas1, wkvas2),
                iterAccumCoder);
          }
        });

    JavaPairRDD<K, WindowedValue<OutputT>> extracted = accumulatedBytes
        .mapToPair(CoderHelpers.fromByteFunction(keyCoder, iterAccumCoder))
        .flatMapValues(new Function<Iterable<WindowedValue<KV<K, AccumT>>>,
            Iterable<WindowedValue<OutputT>>>() {
              @Override
              public Iterable<WindowedValue<OutputT>> call(
                  Iterable<WindowedValue<KV<K, AccumT>>> accums) {
                return sparkCombineFn.extractOutput(accums);
              }
            });
    return extracted.map(TranslationUtils.<K, WindowedValue<OutputT>>fromPairFunction()).map(
        new Function<KV<K, WindowedValue<OutputT>>, WindowedValue<KV<K, OutputT>>>() {
          @Override
          public WindowedValue<KV<K, OutputT>> call(KV<K, WindowedValue<OutputT>> kwvo)
              throws Exception {
            return kwvo.getValue().withValue(KV.of(kwvo.getKey(), kwvo.getValue().getValue()));
          }
        });
  }
}
