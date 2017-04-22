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

import com.google.common.base.Optional;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;


/**
 * A set of group/combine functions to apply to Spark {@link org.apache.spark.rdd.RDD}s.
 */
public class GroupCombineFunctions {

  /**
   * An implementation of
   * {@link org.apache.beam.runners.core.GroupByKeyViaGroupByKeyOnly.GroupByKeyOnly}
   * for the Spark runner.
   */
  public static <K, V> JavaRDD<WindowedValue<KV<K, Iterable<WindowedValue<V>>>>> groupByKeyOnly(
      JavaRDD<WindowedValue<KV<K, V>>> rdd,
      Coder<K> keyCoder,
      WindowedValueCoder<V> wvCoder) {
    // we use coders to convert objects in the PCollection to byte arrays, so they
    // can be transferred over the network for the shuffle.
    JavaPairRDD<ByteArray, byte[]> pairRDD =
        rdd
            .map(new ReifyTimestampsAndWindowsFunction<K, V>())
            .map(WindowingHelpers.<KV<K, WindowedValue<V>>>unwindowFunction())
            .mapToPair(TranslationUtils.<K, WindowedValue<V>>toPairFunction())
            .mapToPair(CoderHelpers.toByteFunction(keyCoder, wvCoder));
    // use a default parallelism HashPartitioner.
    Partitioner partitioner = new HashPartitioner(rdd.rdd().sparkContext().defaultParallelism());

    // using mapPartitions allows to preserve the partitioner
    // and avoid unnecessary shuffle downstream.
    return pairRDD
        .groupByKey(partitioner)
        .mapPartitionsToPair(
            TranslationUtils.pairFunctionToPairFlatMapFunction(
                CoderHelpers.fromByteFunctionIterable(keyCoder, wvCoder)),
            true)
        .mapPartitions(
            TranslationUtils.<K, Iterable<WindowedValue<V>>>fromPairFlatMapFunction(), true)
        .mapPartitions(
            TranslationUtils.functionToFlatMapFunction(
                WindowingHelpers.<KV<K, Iterable<WindowedValue<V>>>>windowFunction()),
            true);
  }

  /**
   * Apply a composite {@link org.apache.beam.sdk.transforms.Combine.Globally} transformation.
   */
  public static <InputT, AccumT> Optional<Iterable<WindowedValue<AccumT>>> combineGlobally(
      JavaRDD<WindowedValue<InputT>> rdd,
      final SparkGlobalCombineFn<InputT, AccumT, ?> sparkCombineFn,
      final Coder<InputT> iCoder,
      final Coder<AccumT> aCoder,
      final WindowingStrategy<?, ?> windowingStrategy) {
    // coders.
    final WindowedValue.FullWindowedValueCoder<InputT> wviCoder =
        WindowedValue.FullWindowedValueCoder.of(iCoder,
            windowingStrategy.getWindowFn().windowCoder());
    final WindowedValue.FullWindowedValueCoder<AccumT> wvaCoder =
        WindowedValue.FullWindowedValueCoder.of(aCoder,
            windowingStrategy.getWindowFn().windowCoder());
    final IterableCoder<WindowedValue<AccumT>> iterAccumCoder = IterableCoder.of(wvaCoder);

    // Use coders to convert objects in the PCollection to byte arrays, so they
    // can be transferred over the network for the shuffle.
    // for readability, we add comments with actual type next to byte[].
    // to shorten line length, we use:
    //---- WV: WindowedValue
    //---- Iterable: Itr
    //---- AccumT: A
    //---- InputT: I
    JavaRDD<byte[]> inputRDDBytes = rdd.map(CoderHelpers.toByteFunction(wviCoder));

    if (inputRDDBytes.isEmpty()) {
      return Optional.absent();
    }

    /*Itr<WV<A>>*/ byte[] accumulatedBytes = inputRDDBytes.aggregate(
        CoderHelpers.toByteArray(sparkCombineFn.zeroValue(), iterAccumCoder),
        new Function2</*A*/ byte[], /*I*/ byte[], /*A*/ byte[]>() {
          @Override
          public /*Itr<WV<A>>*/ byte[] call(/*Itr<WV<A>>*/ byte[] ab, /*WV<I>*/ byte[] ib)
              throws Exception {
            Iterable<WindowedValue<AccumT>> a = CoderHelpers.fromByteArray(ab, iterAccumCoder);
            WindowedValue<InputT> i = CoderHelpers.fromByteArray(ib, wviCoder);
            return CoderHelpers.toByteArray(sparkCombineFn.seqOp(a, i), iterAccumCoder);
          }
        },
        new Function2</*Itr<WV<A>>>*/ byte[], /*Itr<WV<A>>>*/ byte[], /*Itr<WV<A>>>*/ byte[]>() {
          @Override
          public /*Itr<WV<A>>>*/ byte[] call(/*Itr<WV<A>>>*/ byte[] a1b, /*Itr<WV<A>>>*/ byte[] a2b)
              throws Exception {
            Iterable<WindowedValue<AccumT>> a1 = CoderHelpers.fromByteArray(a1b, iterAccumCoder);
            Iterable<WindowedValue<AccumT>> a2 = CoderHelpers.fromByteArray(a2b, iterAccumCoder);
            Iterable<WindowedValue<AccumT>> merged = sparkCombineFn.combOp(a1, a2);
            return CoderHelpers.toByteArray(merged, iterAccumCoder);
          }
        }
    );

    return Optional.of(CoderHelpers.fromByteArray(accumulatedBytes, iterAccumCoder));
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
  public static <K, InputT, AccumT> JavaPairRDD<K, Iterable<WindowedValue<KV<K, AccumT>>>>
      combinePerKey(
          JavaRDD<WindowedValue<KV<K, InputT>>> rdd,
          final SparkKeyedCombineFn<K, InputT, AccumT, ?> sparkCombineFn,
          final Coder<K> keyCoder,
          final Coder<InputT> iCoder,
          final Coder<AccumT> aCoder,
          final WindowingStrategy<?, ?> windowingStrategy) {
    // coders.
    final WindowedValue.FullWindowedValueCoder<KV<K, InputT>> wkviCoder =
        WindowedValue.FullWindowedValueCoder.of(KvCoder.of(keyCoder, iCoder),
            windowingStrategy.getWindowFn().windowCoder());
    final WindowedValue.FullWindowedValueCoder<KV<K, AccumT>> wkvaCoder =
        WindowedValue.FullWindowedValueCoder.of(KvCoder.of(keyCoder, aCoder),
            windowingStrategy.getWindowFn().windowCoder());
    final IterableCoder<WindowedValue<KV<K, AccumT>>> iterAccumCoder = IterableCoder.of(wkvaCoder);

    // We need to duplicate K as both the key of the JavaPairRDD as well as inside the value,
    // since the functions passed to combineByKey don't receive the associated key of each
    // value, and we need to map back into methods in Combine.KeyedCombineFn, which each
    // require the key in addition to the InputT's and AccumT's being merged/accumulated.
    // Once Spark provides a way to include keys in the arguments of combine/merge functions,
    // we won't need to duplicate the keys anymore.
    // Key has to bw windowed in order to group by window as well.
    JavaPairRDD<K, WindowedValue<KV<K, InputT>>> inRddDuplicatedKeyPair =
        rdd.mapToPair(TranslationUtils.<K, InputT>toPairByKeyInWindowedValue());

    // Use coders to convert objects in the PCollection to byte arrays, so they
    // can be transferred over the network for the shuffle.
    // for readability, we add comments with actual type next to byte[].
    // to shorten line length, we use:
    //---- WV: WindowedValue
    //---- Iterable: Itr
    //---- AccumT: A
    //---- InputT: I
    JavaPairRDD<ByteArray, byte[]> inRddDuplicatedKeyPairBytes = inRddDuplicatedKeyPair
        .mapToPair(CoderHelpers.toByteFunction(keyCoder, wkviCoder));

    JavaPairRDD</*K*/ ByteArray, /*Itr<WV<KV<K, A>>>*/ byte[]> accumulatedBytes =
        inRddDuplicatedKeyPairBytes.combineByKey(
        new Function</*WV<KV<K, I>>*/ byte[], /*Itr<WV<KV<K, A>>>*/ byte[]>() {
          @Override
          public /*Itr<WV<KV<K, A>>>*/ byte[] call(/*WV<KV<K, I>>*/ byte[] input) {
            WindowedValue<KV<K, InputT>> wkvi = CoderHelpers.fromByteArray(input, wkviCoder);
            return CoderHelpers.toByteArray(sparkCombineFn.createCombiner(wkvi), iterAccumCoder);
          }
        },
        new Function2</*Itr<WV<KV<K, A>>>*/ byte[], /*WV<KV<K, I>>*/ byte[],
            /*Itr<WV<KV<K, A>>>*/ byte[]>() {
          @Override
          public /*Itr<WV<KV<K, A>>>*/ byte[] call(
              /*Itr<WV<KV<K, A>>>*/ byte[] acc,
              /*WV<KV<K, I>>*/ byte[] input) {
            Iterable<WindowedValue<KV<K, AccumT>>> wkvas =
                CoderHelpers.fromByteArray(acc, iterAccumCoder);
            WindowedValue<KV<K, InputT>> wkvi = CoderHelpers.fromByteArray(input, wkviCoder);
            return CoderHelpers.toByteArray(sparkCombineFn.mergeValue(wkvi, wkvas), iterAccumCoder);
          }
        },
        new Function2</*Itr<WV<KV<K, A>>>*/ byte[], /*Itr<WV<KV<K, A>>>*/ byte[],
            /*Itr<WV<KV<K, A>>>*/ byte[]>() {
          @Override
          public /*Itr<WV<KV<K, A>>>*/ byte[] call(
              /*Itr<WV<KV<K, A>>>*/ byte[] acc1,
              /*Itr<WV<KV<K, A>>>*/ byte[] acc2) {
            Iterable<WindowedValue<KV<K, AccumT>>> wkvas1 =
                CoderHelpers.fromByteArray(acc1, iterAccumCoder);
            Iterable<WindowedValue<KV<K, AccumT>>> wkvas2 =
                CoderHelpers.fromByteArray(acc2, iterAccumCoder);
            return CoderHelpers.toByteArray(sparkCombineFn.mergeCombiners(wkvas1, wkvas2),
                iterAccumCoder);
          }
        });

    return accumulatedBytes.mapToPair(CoderHelpers.fromByteFunction(keyCoder, iterAccumCoder));
  }

  /**
   * An implementation of
   * {@link org.apache.beam.sdk.util.Reshuffle} for the Spark runner.
   */
  public static <K, V> JavaRDD<WindowedValue<KV<K, V>>> reshuffle(
      JavaRDD<WindowedValue<KV<K, V>>> rdd,
      Coder<K> keyCoder,
      WindowedValueCoder<V> wvCoder) {

    // Use coders to convert objects in the PCollection to byte arrays, so they
    // can be transferred over the network for the shuffle.
    return rdd
        .map(new ReifyTimestampsAndWindowsFunction<K, V>())
        .map(WindowingHelpers.<KV<K, WindowedValue<V>>>unwindowFunction())
        .mapToPair(TranslationUtils.<K, WindowedValue<V>>toPairFunction())
        .mapToPair(CoderHelpers.toByteFunction(keyCoder, wvCoder))
        .repartition(rdd.getNumPartitions())
        .mapToPair(CoderHelpers.fromByteFunction(keyCoder, wvCoder))
        .map(TranslationUtils.<K, WindowedValue<V>>fromPairFunction())
        .map(TranslationUtils.<K, V>toKVByWindowInValue());
  }
}
