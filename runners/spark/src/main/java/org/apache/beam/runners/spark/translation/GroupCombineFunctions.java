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
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.SignedBytes;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.runners.spark.util.CompositeKey;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.FullWindowedValueCoder;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.joda.time.Instant;
import scala.Tuple2;

/** A set of group/combine functions to apply to Spark {@link org.apache.spark.rdd.RDD}s. */
public class GroupCombineFunctions {

  /**
   * An implementation of {@link
   * org.apache.beam.runners.core.GroupByKeyViaGroupByKeyOnly.GroupByKeyOnly} for the Spark runner.
   */
  public static <K, V> JavaRDD<WindowedValue<KV<K, Iterable<WindowedValue<V>>>>> groupByKeyOnly(
      JavaRDD<WindowedValue<KV<K, V>>> rdd,
      Coder<K> keyCoder,
      WindowedValueCoder<V> wvCoder,
      boolean defaultParallelism) {
    // we use coders to convert objects in the PCollection to byte arrays, so they
    // can be transferred over the network for the shuffle.
    JavaPairRDD<ByteArray, byte[]> pairRDD =
        rdd.map(new ReifyTimestampsAndWindowsFunction<>())
            .map(WindowingHelpers.unwindowFunction())
            .mapToPair(TranslationUtils.toPairFunction())
            .mapToPair(CoderHelpers.toByteFunction(keyCoder, wvCoder));
    JavaPairRDD<ByteArray, Iterable<byte[]>> groupedRDD;
    if (defaultParallelism) {
      groupedRDD =
          pairRDD.groupByKey(new HashPartitioner(rdd.rdd().sparkContext().defaultParallelism()));
    } else {
      groupedRDD = pairRDD.groupByKey();
    }

    // using mapPartitions allows to preserve the partitioner
    // and avoid unnecessary shuffle downstream.
    return groupedRDD
        .mapPartitionsToPair(
            TranslationUtils.pairFunctionToPairFlatMapFunction(
                CoderHelpers.fromByteFunctionIterable(keyCoder, wvCoder)),
            true)
        .mapPartitions(TranslationUtils.fromPairFlatMapFunction(), true)
        .mapPartitions(
            TranslationUtils.functionToFlatMapFunction(WindowingHelpers.windowFunction()), true);
  }

  public static <K, V, W extends BoundedWindow> JavaRDD<WindowedValue<KV<K, Iterable<V>>>> groupByKeyNonMerging(
      JavaRDD<WindowedValue<KV<K, V>>> rdd,
      Coder<K> keyCoder,
      Coder<V> valueCoder,
      Coder<W> windowCoder) {

    JavaPairRDD<CompositeKey, byte[]> pairRDD = rdd
        .flatMapToPair((WindowedValue<KV<K, V>> kvWindowedValue) -> {
          Iterable<WindowedValue<KV<K, V>>> windows = kvWindowedValue.explodeWindows();

          return Iterators.transform(windows.iterator(),
              wid -> {
            // todo nahoru
                final V value = wid.getValue().getValue();
                final K key = wid.getValue().getKey();
                final byte[] keyBytes = CoderHelpers.toByteArray(key, keyCoder);
                @SuppressWarnings("unchecked")
                final W window = (W) Iterables.getOnlyElement(wid.getWindows());
                final byte[] windowBytes = CoderHelpers.toByteArray(window, windowCoder);

                System.out.println("key = " + key + " val " + value + " windowTs " + wid.getTimestamp() + " win "+ wid.getWindows().toArray()[0]);
                CompositeKey compositeKey = new CompositeKey(keyBytes, windowBytes, wid.getTimestamp().getMillis());
                return new Tuple2<>(compositeKey, CoderHelpers.toByteArray(value, valueCoder));
              }
          );
        });

    final Partitioner partitioner = new HashPartitioner(pairRDD.getNumPartitions());

    JavaPairRDD<CompositeKey, byte[]> byteArrayJavaPairRDD = pairRDD
        .repartitionAndSortWithinPartitions(partitioner, new Comprator());


    JavaPairRDD<WindowedValue<K>, Iterable<V>> windowedValueIterableJavaPairRDD = byteArrayJavaPairRDD
        .mapPartitionsToPair(
            it -> new ReduceByKeyIterator<>(it, keyCoder, valueCoder, windowCoder));

    return windowedValueIterableJavaPairRDD.mapPartitions(fromPairFlatMapFunction2());

  }
  /** A pair to {@link KV} flatmap function . */
  static <K, V> FlatMapFunction<Iterator<Tuple2<WindowedValue<K>, Iterable<V>>>, WindowedValue<KV<K, Iterable<V>>>> fromPairFlatMapFunction2() {
    return itr -> Iterators.transform(itr, t2 ->
    {
//      System.out.println("pred = " + t2);
      WindowedValue<KV<K, Iterable<V>>> newT = t2._1.withValue(KV.of(t2._1().getValue(), t2._2()));
//      System.out.println("po = " + newT.getValue().getValue());
      ImmutableList<V> vs = ImmutableList.copyOf(newT.getValue().getValue());
      System.out.println(newT.getValue().getKey()+" iter = " + vs);
      return newT;});
  }

  private static class Comprator implements Comparator<CompositeKey>, Serializable {

    @Override
    public int compare(CompositeKey o1, CompositeKey o2) {
      int compare = SignedBytes.lexicographicalComparator().compare(o1.getKey(), o2.getKey());
      if (compare == 0) {
        int compare1 =
            SignedBytes.lexicographicalComparator().compare(o1.getWindow(), o2.getWindow());
        if (compare1 == 0) {
          return Long.compare(o1.getTimestamp(), o2.getTimestamp());
        }
        return compare1;
      }
      return compare;
    }
  }


  /**
   * Transform stream of sorted key values into stream of value iterators for each key. This iterator
   * can be iterated only once!
   *
   * @param <K> type of key iterator emits
   * @param <V> type of value iterator emits
   */
  static class ReduceByKeyIterator<K, V, W extends BoundedWindow>
      extends AbstractIterator<Tuple2<WindowedValue<K>, Iterable<V>>> {

    private final PeekingIterator<Tuple2<CompositeKey, byte []>> inner;

    private CompositeKey previousKey;
//    private final PairFunction<Tuple2<CompositeKey, byte[]>, WindowedValue<K>, V> pairFunction;
    private final Coder<K> keyCoder;
    private final Coder<V> valueCoder;
    private final Coder<W> windowCoder;

    ReduceByKeyIterator(
        Iterator<Tuple2<CompositeKey, byte[]>> inner,
        Coder<K> keyCoder, Coder<V> valueCoder, Coder<W> windowCoder) {
      this.inner = Iterators.peekingIterator(inner);
      this.keyCoder = keyCoder;
      this.valueCoder = valueCoder;
      this.windowCoder = windowCoder;
    }

//    ReduceByKeyIterator(Iterator<Tuple2<CompositeKey, byte []>> inner,
//        PairFunction<Tuple2<CompositeKey, byte[]>, WindowedValue<K>, V> pairFunction) {
//      this.inner = Iterators.peekingIterator(inner);
//      this.pairFunction = pairFunction;
//    }

    @Override
    protected Tuple2<WindowedValue<K>, Iterable<V>> computeNext() {
      while (inner.hasNext()) {
        // just peek, so the value iterator can see the first value
        Tuple2<CompositeKey, byte[]> peek = inner.peek();

        final CompositeKey currentKey = peek._1 ;

        if (currentKey.isSameKey(previousKey)) {
          // inner iterator did not consume all values for a given key, we need to skip ahead until we
          // find value for the next key
          inner.next();
          continue;
        }
        previousKey = currentKey;
        Tuple2<WindowedValue<K>, V> decodedEl = getDecodedElement(peek);
        System.out.println("decodedEl = " + decodedEl._1 + " val="+ decodedEl._2);
        return new Tuple2<>(
            decodedEl._1,
            () ->
                new AbstractIterator<V>() {

                  @Override
                  protected V computeNext() {
                    // compute until we know, that next element contains a new key or there are no
                    // more elements to process
                    if (inner.hasNext() && currentKey.isSameKey(inner.peek()._1)) {
                      Tuple2<CompositeKey, byte[]> next = inner.next();
                      System.out.println("new value for key= " + decodedEl._1 + " val="+ getDecodedElement(next)._2);
                      return getDecodedElement(next)._2;
                    }
                    return endOfData();
                  }
                });
      }
      return endOfData();
    }

    Tuple2<WindowedValue<K>, V> getDecodedElement(Tuple2<CompositeKey, byte[]> item){
//      try {
      CompositeKey compositeKey = item._1;
      W w = CoderHelpers.fromByteArray(compositeKey.getWindow(), windowCoder);

      K key = CoderHelpers.fromByteArray(compositeKey.getKey(), keyCoder);
      WindowedValue<K> of = WindowedValue.of(key, new Instant(w.maxTimestamp()), w, PaneInfo.ON_TIME_AND_ONLY_FIRING);//todo predavat pane
      return new Tuple2<>(of, CoderHelpers.fromByteArray(item._2, valueCoder));
//      } catch (Exception e) {
//        throw new RuntimeException(e);
//      }

    }
  }

  /** Apply a composite {@link org.apache.beam.sdk.transforms.Combine.Globally} transformation. */
  public static <InputT, AccumT> Optional<Iterable<WindowedValue<AccumT>>> combineGlobally(
      JavaRDD<WindowedValue<InputT>> rdd,
      final SparkGlobalCombineFn<InputT, AccumT, ?> sparkCombineFn,
      final Coder<InputT> iCoder,
      final Coder<AccumT> aCoder,
      final WindowingStrategy<?, ?> windowingStrategy) {
    // coders.
    final WindowedValue.FullWindowedValueCoder<InputT> wviCoder =
        WindowedValue.FullWindowedValueCoder.of(
            iCoder, windowingStrategy.getWindowFn().windowCoder());
    final WindowedValue.FullWindowedValueCoder<AccumT> wvaCoder =
        WindowedValue.FullWindowedValueCoder.of(
            aCoder, windowingStrategy.getWindowFn().windowCoder());
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

//    if (inputRDDBytes.isEmpty()) {
//      return Optional.absent();
//    }

    /*Itr<WV<A>>*/
    /*Itr<WV<A>>>*/
    /*Itr<WV<A>>>*/
    /*Itr<WV<A>>>*/
    /*Itr<WV<A>>>*/
    /*Itr<WV<A>>>*/
    /*Itr<WV<A>>>*/
    /*A*/
    /*I*/
    /*A*/
    /*Itr<WV<A>>*/
    /*Itr<WV<A>>*/
    /*WV<I>*/
    byte[] accumulatedBytes =
        inputRDDBytes.aggregate(
            CoderHelpers.toByteArray(sparkCombineFn.zeroValue(), iterAccumCoder),
            (ab, ib) -> {
              Iterable<WindowedValue<AccumT>> a = CoderHelpers.fromByteArray(ab, iterAccumCoder);
              WindowedValue<InputT> i = CoderHelpers.fromByteArray(ib, wviCoder);
              return CoderHelpers.toByteArray(sparkCombineFn.seqOp(a, i), iterAccumCoder);
            },
            (a1b, a2b) -> {
              Iterable<WindowedValue<AccumT>> a1 = CoderHelpers.fromByteArray(a1b, iterAccumCoder);
              Iterable<WindowedValue<AccumT>> a2 = CoderHelpers.fromByteArray(a2b, iterAccumCoder);
              Iterable<WindowedValue<AccumT>> merged = sparkCombineFn.combOp(a1, a2);
              return CoderHelpers.toByteArray(merged, iterAccumCoder);
            });

    return Optional.of(CoderHelpers.fromByteArray(accumulatedBytes, iterAccumCoder));
  }

  /**
   * Apply a composite {@link org.apache.beam.sdk.transforms.Combine.PerKey} transformation.
   *
   * <p>This aggregation will apply Beam's {@link org.apache.beam.sdk.transforms.Combine.CombineFn}
   * via Spark's {@link JavaPairRDD#combineByKey(Function, Function2, Function2)} aggregation. For
   * streaming, this will be called from within a serialized context (DStream's transform callback),
   * so passed arguments need to be Serializable.
   */
  public static <K, InputT, AccumT>
      JavaPairRDD<K, Iterable<WindowedValue<KV<K, AccumT>>>> combinePerKey(
          JavaRDD<WindowedValue<KV<K, InputT>>> rdd,
          final SparkKeyedCombineFn<K, InputT, AccumT, ?> sparkCombineFn,
          final Coder<K> keyCoder,
          final Coder<InputT> iCoder,
          final Coder<AccumT> aCoder,
          final WindowingStrategy<?, ?> windowingStrategy) {
    // coders.
    final WindowedValue.FullWindowedValueCoder<KV<K, InputT>> wkviCoder =
        WindowedValue.FullWindowedValueCoder.of(
            KvCoder.of(keyCoder, iCoder), windowingStrategy.getWindowFn().windowCoder());
    final WindowedValue.FullWindowedValueCoder<KV<K, AccumT>> wkvaCoder =
        WindowedValue.FullWindowedValueCoder.of(
            KvCoder.of(keyCoder, aCoder), windowingStrategy.getWindowFn().windowCoder());
    final IterableCoder<WindowedValue<KV<K, AccumT>>> iterAccumCoder = IterableCoder.of(wkvaCoder);

    // We need to duplicate K as both the key of the JavaPairRDD as well as inside the value,
    // since the functions passed to combineByKey don't receive the associated key of each
    // value, and we need to map back into methods in Combine.KeyedCombineFn, which each
    // require the key in addition to the InputT's and AccumT's being merged/accumulated.
    // Once Spark provides a way to include keys in the arguments of combine/merge functions,
    // we won't need to duplicate the keys anymore.
    // Key has to bw windowed in order to group by window as well.
    JavaPairRDD<K, WindowedValue<KV<K, InputT>>> inRddDuplicatedKeyPair =
        rdd.mapToPair(TranslationUtils.toPairByKeyInWindowedValue());

    // Use coders to convert objects in the PCollection to byte arrays, so they
    // can be transferred over the network for the shuffle.
    // for readability, we add comments with actual type next to byte[].
    // to shorten line length, we use:
    //---- WV: WindowedValue
    //---- Iterable: Itr
    //---- AccumT: A
    //---- InputT: I
    JavaPairRDD<ByteArray, byte[]> inRddDuplicatedKeyPairBytes =
        inRddDuplicatedKeyPair.mapToPair(CoderHelpers.toByteFunction(keyCoder, wkviCoder));

    /*Itr<WV<KV<K, A>>>*/
    /*Itr<WV<KV<K, A>>>*/
    /*Itr<WV<KV<K, A>>>*/
    /*Itr<WV<KV<K, A>>>*/
    /*Itr<WV<KV<K, A>>>*/
    /*Itr<WV<KV<K, A>>>*/
    /*Itr<WV<KV<K, A>>>*/
    /*WV<KV<K, I>>*/
    /*Itr<WV<KV<K, A>>>*/
    /*Itr<WV<KV<K, A>>>*/
    /*Itr<WV<KV<K, A>>>*/
    /*WV<KV<K, I>>*/
    /*WV<KV<K, I>>*/
    /*Itr<WV<KV<K, A>>>*/
    /*Itr<WV<KV<K, A>>>*/
    /*WV<KV<K, I>>*/
    JavaPairRDD</*K*/ ByteArray, /*Itr<WV<KV<K, A>>>*/ byte[]> accumulatedBytes =
        inRddDuplicatedKeyPairBytes.combineByKey(
            input -> {
              WindowedValue<KV<K, InputT>> wkvi = CoderHelpers.fromByteArray(input, wkviCoder);
              return CoderHelpers.toByteArray(sparkCombineFn.createCombiner(wkvi), iterAccumCoder);
            },
            (acc, input) -> {
              Iterable<WindowedValue<KV<K, AccumT>>> wkvas =
                  CoderHelpers.fromByteArray(acc, iterAccumCoder);
              WindowedValue<KV<K, InputT>> wkvi = CoderHelpers.fromByteArray(input, wkviCoder);
              return CoderHelpers.toByteArray(
                  sparkCombineFn.mergeValue(wkvi, wkvas), iterAccumCoder);
            },
            (acc1, acc2) -> {
              Iterable<WindowedValue<KV<K, AccumT>>> wkvas1 =
                  CoderHelpers.fromByteArray(acc1, iterAccumCoder);
              Iterable<WindowedValue<KV<K, AccumT>>> wkvas2 =
                  CoderHelpers.fromByteArray(acc2, iterAccumCoder);
              return CoderHelpers.toByteArray(
                  sparkCombineFn.mergeCombiners(wkvas1, wkvas2), iterAccumCoder);
            });

    return accumulatedBytes.mapToPair(CoderHelpers.fromByteFunction(keyCoder, iterAccumCoder));
  }

  /** An implementation of {@link Reshuffle} for the Spark runner. */
  public static <K, V> JavaRDD<WindowedValue<KV<K, V>>> reshuffle(
      JavaRDD<WindowedValue<KV<K, V>>> rdd, Coder<K> keyCoder, WindowedValueCoder<V> wvCoder) {

    // Use coders to convert objects in the PCollection to byte arrays, so they
    // can be transferred over the network for the shuffle.
    return rdd.map(new ReifyTimestampsAndWindowsFunction<>())
        .map(WindowingHelpers.unwindowFunction())
        .mapToPair(TranslationUtils.toPairFunction())
        .mapToPair(CoderHelpers.toByteFunction(keyCoder, wvCoder))
        .repartition(rdd.getNumPartitions())
        .mapToPair(CoderHelpers.fromByteFunction(keyCoder, wvCoder))
        .map(TranslationUtils.fromPairFunction())
        .map(TranslationUtils.toKVByWindowInValue());
  }
}
