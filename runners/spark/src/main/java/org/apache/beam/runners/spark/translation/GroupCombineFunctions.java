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

import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.checkerframework.checker.nullness.qual.Nullable;
import scala.Tuple2;

/** A set of group/combine functions to apply to Spark {@link org.apache.spark.rdd.RDD}s. */
public class GroupCombineFunctions {

  /**
   * An implementation of {@link
   * org.apache.beam.runners.core.GroupByKeyViaGroupByKeyOnly.GroupByKeyOnly} for the Spark runner.
   */
  public static <K, V> JavaRDD<KV<K, Iterable<WindowedValue<V>>>> groupByKeyOnly(
      JavaRDD<WindowedValue<KV<K, V>>> rdd,
      Coder<K> keyCoder,
      WindowedValueCoder<V> wvCoder,
      @Nullable Partitioner partitioner) {
    // we use coders to convert objects in the PCollection to byte arrays, so they
    // can be transferred over the network for the shuffle.
    JavaPairRDD<ByteArray, byte[]> pairRDD =
        rdd.map(new ReifyTimestampsAndWindowsFunction<>())
            .mapToPair(TranslationUtils.toPairFunction())
            .mapToPair(CoderHelpers.toByteFunction(keyCoder, wvCoder));

    // If no partitioner is passed, the default group by key operation is called
    JavaPairRDD<ByteArray, Iterable<byte[]>> groupedRDD =
        (partitioner != null) ? pairRDD.groupByKey(partitioner) : pairRDD.groupByKey();

    return groupedRDD
        .mapToPair(CoderHelpers.fromByteFunctionIterable(keyCoder, wvCoder))
        .map(new TranslationUtils.FromPairFunction<>());
  }

  /**
   * Spark-level group by key operation that keeps original Beam {@link KV} pairs unchanged.
   *
   * @returns {@link JavaPairRDD} where the first value in the pair is the serialized key, and the
   *     second is an iterable of the {@link KV} pairs with that key.
   */
  static <K, V> JavaPairRDD<ByteArray, Iterable<WindowedValue<KV<K, V>>>> groupByKeyPair(
      JavaRDD<WindowedValue<KV<K, V>>> rdd, Coder<K> keyCoder, WindowedValueCoder<V> wvCoder) {
    // we use coders to convert objects in the PCollection to byte arrays, so they
    // can be transferred over the network for the shuffle.
    JavaPairRDD<ByteArray, byte[]> pairRDD =
        rdd.map(new ReifyTimestampsAndWindowsFunction<>())
            .mapToPair(TranslationUtils.toPairFunction())
            .mapToPair(CoderHelpers.toByteFunction(keyCoder, wvCoder));

    JavaPairRDD<ByteArray, Iterable<Tuple2<ByteArray, byte[]>>> groupedRDD =
        pairRDD.groupBy((value) -> value._1);

    return groupedRDD
        .mapValues(
            it -> Iterables.transform(it, new CoderHelpers.FromByteFunction<>(keyCoder, wvCoder)))
        .mapValues(it -> Iterables.transform(it, new TranslationUtils.FromPairFunction<>()))
        .mapValues(
            it -> Iterables.transform(it, new TranslationUtils.ToKVByWindowInValueFunction<>()));
  }

  /** Apply a composite {@link org.apache.beam.sdk.transforms.Combine.Globally} transformation. */
  public static <InputT, OutputT, AccumT>
      SparkCombineFn.WindowedAccumulator<InputT, InputT, AccumT, ?> combineGlobally(
          JavaRDD<WindowedValue<InputT>> rdd,
          final SparkCombineFn<InputT, InputT, AccumT, OutputT> sparkCombineFn,
          final Coder<AccumT> aCoder,
          final WindowingStrategy<?, ?> windowingStrategy) {

    @SuppressWarnings("unchecked")
    final Coder<BoundedWindow> windowCoder = (Coder) windowingStrategy.getWindowFn().windowCoder();
    final SparkCombineFn.WindowedAccumulatorCoder<InputT, InputT, AccumT> waCoder =
        sparkCombineFn.accumulatorCoder(windowCoder, aCoder, windowingStrategy);

    ValueAndCoderLazySerializable<SparkCombineFn.WindowedAccumulator<InputT, InputT, AccumT, ?>>
        accumulatedResult =
            rdd.aggregate(
                ValueAndCoderLazySerializable.of(sparkCombineFn.createCombiner(), waCoder),
                (ab, ib) -> {
                  SparkCombineFn.WindowedAccumulator<InputT, InputT, AccumT, ?> merged =
                      sparkCombineFn.mergeValue(ab.getOrDecode(waCoder), ib);
                  return ValueAndCoderLazySerializable.of(merged, waCoder);
                },
                (a1b, a2b) -> {
                  SparkCombineFn.WindowedAccumulator<InputT, InputT, AccumT, ?> merged =
                      sparkCombineFn.mergeCombiners(
                          a1b.getOrDecode(waCoder), a2b.getOrDecode(waCoder));
                  return ValueAndCoderLazySerializable.of(merged, waCoder);
                });

    return accumulatedResult.getOrDecode(waCoder);
  }

  /**
   * Apply a composite {@link org.apache.beam.sdk.transforms.Combine.PerKey} transformation.
   *
   * <p>This aggregation will apply Beam's {@link org.apache.beam.sdk.transforms.Combine.CombineFn}
   * via Spark's {@link JavaPairRDD#combineByKey(Function, Function2, Function2)} aggregation. For
   * streaming, this will be called from within a serialized context (DStream's transform callback),
   * so passed arguments need to be Serializable.
   */
  public static <K, V, AccumT>
      JavaPairRDD<K, SparkCombineFn.WindowedAccumulator<KV<K, V>, V, AccumT, ?>> combinePerKey(
          JavaRDD<WindowedValue<KV<K, V>>> rdd,
          final SparkCombineFn<KV<K, V>, V, AccumT, ?> sparkCombineFn,
          final Coder<K> keyCoder,
          final Coder<V> valueCoder,
          final Coder<AccumT> aCoder,
          final WindowingStrategy<?, ?> windowingStrategy) {

    boolean mustBringWindowToKey = sparkCombineFn.mustBringWindowToKey();
    @SuppressWarnings("unchecked")
    Coder<BoundedWindow> windowCoder = (Coder) windowingStrategy.getWindowFn().windowCoder();
    final SparkCombineFn.WindowedAccumulatorCoder<KV<K, V>, V, AccumT> waCoder =
        sparkCombineFn.accumulatorCoder(windowCoder, aCoder, windowingStrategy);

    // We need to duplicate K as both the key of the JavaPairRDD as well as inside the value,
    // since the functions passed to combineByKey don't receive the associated key of each
    // value, and we need to map back into methods in Combine.KeyedCombineFn, which each
    // require the key in addition to the InputT's and AccumT's being merged/accumulated.
    // Once Spark provides a way to include keys in the arguments of combine/merge functions,
    // we won't need to duplicate the keys anymore.
    // Key has to bw windowed in order to group by window as well.
    final JavaPairRDD<ByteArray, WindowedValue<KV<K, V>>> inRddDuplicatedKeyPair;
    if (!mustBringWindowToKey) {
      inRddDuplicatedKeyPair = rdd.mapToPair(TranslationUtils.toPairByKeyInWindowedValue(keyCoder));
    } else {
      inRddDuplicatedKeyPair =
          GroupNonMergingWindowsFunctions.bringWindowToKey(rdd, keyCoder, windowCoder);
    }

    JavaPairRDD<
            ByteArray,
            ValueAndCoderLazySerializable<
                SparkCombineFn.WindowedAccumulator<KV<K, V>, V, AccumT, ?>>>
        accumulatedResult =
            inRddDuplicatedKeyPair.combineByKey(
                input ->
                    ValueAndCoderLazySerializable.of(sparkCombineFn.createCombiner(input), waCoder),
                (acc, input) ->
                    ValueAndCoderLazySerializable.of(
                        sparkCombineFn.mergeValue(acc.getOrDecode(waCoder), input), waCoder),
                (acc1, acc2) ->
                    ValueAndCoderLazySerializable.of(
                        sparkCombineFn.mergeCombiners(
                            acc1.getOrDecode(waCoder), acc2.getOrDecode(waCoder)),
                        waCoder));

    return accumulatedResult.mapToPair(
        i ->
            new Tuple2<>(
                CoderHelpers.fromByteArray(i._1.getValue(), keyCoder), i._2.getOrDecode(waCoder)));
  }

  /** An implementation of {@link Reshuffle} for the Spark runner. */
  public static <T> JavaRDD<WindowedValue<T>> reshuffle(
      JavaRDD<WindowedValue<T>> rdd, WindowedValueCoder<T> wvCoder) {
    // Use coders to convert objects in the PCollection to byte arrays, so they
    // can be transferred over the network for the shuffle.
    return rdd.map(CoderHelpers.toByteFunction(wvCoder))
        .repartition(Math.max(rdd.context().defaultParallelism(), rdd.getNumPartitions()))
        .map(CoderHelpers.fromByteFunction(wvCoder));
  }
}
