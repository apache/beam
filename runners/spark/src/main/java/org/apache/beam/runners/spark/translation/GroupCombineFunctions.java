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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.runners.spark.coders.CoderHelpers;
import org.apache.beam.runners.spark.util.ByteArray;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowedValue.WindowedValueCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
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
      @Nullable Partitioner partitioner) {
    // we use coders to convert objects in the PCollection to byte arrays, so they
    // can be transferred over the network for the shuffle.
    JavaPairRDD<ByteArray, byte[]> pairRDD =
        rdd.map(new ReifyTimestampsAndWindowsFunction<>())
            .map(WindowingHelpers.unwindowFunction())
            .mapToPair(TranslationUtils.toPairFunction())
            .mapToPair(CoderHelpers.toByteFunction(keyCoder, wvCoder));

    // If no partitioner is passed, the default group by key operation is called
    JavaPairRDD<ByteArray, Iterable<byte[]>> groupedRDD =
        (partitioner != null) ? pairRDD.groupByKey(partitioner) : pairRDD.groupByKey();

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

  /** Apply a composite {@link org.apache.beam.sdk.transforms.Combine.Globally} transformation. */
  public static <InputT, AccumT> Optional<Iterable<WindowedValue<AccumT>>> combineGlobally(
      JavaRDD<WindowedValue<InputT>> rdd,
      final SparkGlobalCombineFn<InputT, AccumT, ?> sparkCombineFn,
      final Coder<AccumT> aCoder,
      final WindowingStrategy<?, ?> windowingStrategy) {

    final WindowedValue.FullWindowedValueCoder<AccumT> wvaCoder =
        WindowedValue.FullWindowedValueCoder.of(
            aCoder, windowingStrategy.getWindowFn().windowCoder());
    final IterableCoder<WindowedValue<AccumT>> iterAccumCoder = IterableCoder.of(wvaCoder);

    SerializableAccumulator<AccumT> accumulatedResult =
        rdd.aggregate(
            SerializableAccumulator.empty(iterAccumCoder),
            (ab, ib) -> {
              Iterable<WindowedValue<AccumT>> merged =
                  sparkCombineFn.seqOp(ab.getOrDecode(iterAccumCoder), ib);
              return SerializableAccumulator.of(merged, iterAccumCoder);
            },
            (a1b, a2b) -> {
              Iterable<WindowedValue<AccumT>> merged =
                  sparkCombineFn.combOp(
                      a1b.getOrDecode(iterAccumCoder), a2b.getOrDecode(iterAccumCoder));
              return SerializableAccumulator.of(merged, iterAccumCoder);
            });

    final Iterable<WindowedValue<AccumT>> result = accumulatedResult.getOrDecode(iterAccumCoder);

    return Iterables.isEmpty(result) ? Optional.absent() : Optional.of(result);
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
          final Coder<AccumT> aCoder,
          final WindowingStrategy<?, ?> windowingStrategy) {

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

    JavaPairRDD<K, SerializableAccumulator<KV<K, AccumT>>> accumulatedResult =
        inRddDuplicatedKeyPair.combineByKey(
            input ->
                SerializableAccumulator.of(sparkCombineFn.createCombiner(input), iterAccumCoder),
            (acc, input) ->
                SerializableAccumulator.of(
                    sparkCombineFn.mergeValue(input, acc.getOrDecode(iterAccumCoder)),
                    iterAccumCoder),
            (acc1, acc2) ->
                SerializableAccumulator.of(
                    sparkCombineFn.mergeCombiners(
                        acc1.getOrDecode(iterAccumCoder), acc2.getOrDecode(iterAccumCoder)),
                    iterAccumCoder));

    return accumulatedResult.mapToPair(i -> new Tuple2<>(i._1, i._2.getOrDecode(iterAccumCoder)));
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

  /**
   * Wrapper around accumulated (combined) value with custom lazy serialization. Serialization is
   * done through given coder and it is performed within on-serialization callbacks {@link
   * #writeObject(ObjectOutputStream)} and {@link KryoAccumulatorSerializer#write(Kryo, Output,
   * SerializableAccumulator)}. Both Spark's serialization mechanisms (Java Serialization, Kryo) are
   * supported. Materialization of accumulated value is done when value is requested to avoid
   * serialization of the coder itself.
   *
   * @param <AccumT>
   */
  public static class SerializableAccumulator<AccumT> implements Serializable {
    private transient Iterable<WindowedValue<AccumT>> accumulated;
    private transient Coder<Iterable<WindowedValue<AccumT>>> coder;

    private byte[] serializedAcc;

    private SerializableAccumulator() {}

    private SerializableAccumulator(
        Iterable<WindowedValue<AccumT>> accumulated,
        Coder<Iterable<WindowedValue<AccumT>>> coder,
        byte[] serializedAcc) {
      this.accumulated = accumulated;
      this.coder = coder;
      this.serializedAcc = serializedAcc;
    }

    static <AccumT> SerializableAccumulator<AccumT> of(
        Iterable<WindowedValue<AccumT>> accumulated, Coder<Iterable<WindowedValue<AccumT>>> coder) {
      return new SerializableAccumulator<>(accumulated, coder, null);
    }

    static <AccumT> SerializableAccumulator<AccumT> ofBytes(byte[] serializedAcc) {
      Preconditions.checkNotNull(serializedAcc);
      return new SerializableAccumulator<>(null, null, serializedAcc);
    }

    static <AccumT> SerializableAccumulator<AccumT> empty(
        Coder<Iterable<WindowedValue<AccumT>>> coder) {
      return new SerializableAccumulator<>(Lists.newArrayList(), coder, null);
    }

    /**
     * Returns wrapped accumulated value when available as java object or deserialize them using
     * given {@code coder}.
     *
     * @param coder
     * @return
     */
    Iterable<WindowedValue<AccumT>> getOrDecode(Coder<Iterable<WindowedValue<AccumT>>> coder) {
      if (accumulated == null) {
        accumulated = CoderHelpers.fromByteArray(serializedAcc, coder);
        serializedAcc = null;
      }

      if (this.coder == null) {
        this.coder = coder;
      }

      return accumulated;
    }

    byte[] toBytes() {
      byte[] coded;
      if (coder != null) {
        coded = CoderHelpers.toByteArray(this.accumulated, coder);
      } else if (serializedAcc != null) {
        coded = serializedAcc;
      } else {
        throw new IllegalStateException(
            String.format(
                "Given '%s' cannot be serialized since it do not contain coder or already serialized data.",
                SerializableAccumulator.class.getSimpleName()));
      }
      return coded;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
      byte[] coded = toBytes();
      out.writeInt(coded.length);
      out.write(coded);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      int length = in.readInt();
      byte[] coded = new byte[length];
      in.readFully(coded);
      this.serializedAcc = coded;
    }
  }

  /**
   * Kryo serializer for {@link SerializableAccumulator}.
   *
   * @param <AccumT>
   */
  public static class KryoAccumulatorSerializer<AccumT>
      extends Serializer<SerializableAccumulator<AccumT>> {

    @Override
    public void write(Kryo kryo, Output output, SerializableAccumulator<AccumT> accumulator) {
      byte[] coded = accumulator.toBytes();

      output.writeInt(coded.length, true);
      output.write(coded);
    }

    @Override
    public SerializableAccumulator<AccumT> read(
        Kryo kryo, Input input, Class<SerializableAccumulator<AccumT>> type) {
      int length = input.readInt(true);
      byte[] coded = input.readBytes(length);
      return SerializableAccumulator.ofBytes(coded);
    }
  }
}
