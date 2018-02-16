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
package org.apache.beam.sdk.extensions.sketching;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@code PTransform}s for finding the k most frequent elements in a {@code PCollection}, or
 * the k most frequent values associated with each key in a {@code PCollection} of {@code KV}s.
 *
 * <h2>References</h2>
 *
 * <p>This class uses the Space-Saving algorithm, introduced in this paper :
 * <a>https://pdfs.semanticscholar.org/72f1/5aba2e67b1cc9cd1fb12c99e101c4c1aae4b.pdf</a>
 * <br>The implementation comes from Addthis' library Stream-lib :
 * <a>https://github.com/addthis/stream-lib</a>
 *
 * <h2>Parameters</h2>
 *
 * <p>Capacity :the maximum number of distinct elements that the Stream Summary can keep
 * track of at the same time
 */
@Experimental
public final class MostFrequent {

  /**
   * A {@code PTransform} that takes a {@code PCollection<T>} and returns a
   * {@code PCollection<StreamSummary<T>>} whose contents is a sketch which contains
   * the most frequent elements in the input {@code PCollection}.
   *
   * <p>The {@code capacity} parameter controls the maximum number of elements the sketch
   * can contain. Once this capacity is reached the least frequent element is dropped each
   * time an incoming element is not already present in the sketch.
   * Each element in the sketch is associated to a counter, that keeps track of the estimated
   * frequency as well as the maximal potential error.
   * <br>See {@link MostFrequentFn} for more details.
   *
   * <p>Example of use
   * <pre>{@code PCollection<String> input = ...;
   * PCollection<StreamSummary<String>> ssSketch = input
   *        .apply(MostFrequent.<String>perKey(10000));
   * }</pre>
   *
   * @param <InputT>         the type of the elements in the input {@code PCollection}
   */
  public static <InputT> GlobalSummary<InputT> globally() {
    return GlobalSummary.<InputT>builder().build();
  }

  /**
   * A {@code PTransform} that takes an input {@code PCollection<KV<K, T>>} and returns a
   * {@code PCollection<KV<K, StreamSummary<T>>} that contains an output element mapping each
   * distinct key in the input {@code PCollection} to a sketch which contains the most frequent
   * values associated with that key in the input {@code PCollection}.
   *
   * <p>The {@code capacity} parameter controls the maximum number of elements the sketch
   * can contain. Once this capacity is reached the least frequent element is dropped each
   * time an incoming element is not already present in the sketch.
   * Each element in the sketch is associated to a counter, that keeps track of the estimated
   * frequency as well as the maximal potential error.
   * <br>See {@link MostFrequentFn} for more details.
   *
   * <p>Example of use
   * <pre>{@code PCollection<KV<Integer, String>> input = ...;
   * PCollection<KV<Integer, StreamSummary<String>>> ssSketch = input
   *        .apply(MostFrequent.<Integer, String>globally(10000));
   * }</pre>
   *
   * @param <K>         the type of the keys in the input and output {@code PCollection}s
   * @param <V>         the type of values in the input {@code PCollection}
   */
  public static <K, V> PerKeySummary<K, V> perKey() {
    return PerKeySummary.<K, V>builder().build();
  }

  /**
   * Implementation of {@link #globally()}.
   *
   * @param <InputT> the type of the elements in the input {@link PCollection}
   */
  @AutoValue
  public abstract static class GlobalSummary<InputT>
          extends PTransform<PCollection<InputT>, PCollection<KV<InputT, Long>>> {

    abstract int capacity();
    abstract int topK();
    abstract Builder<InputT> toBuilder();

    static <InputT> Builder<InputT> builder() {
      return new AutoValue_MostFrequent_GlobalSummary.Builder<InputT>()
              .setCapacity(1000)
              .setTopK(100);
    }

    @AutoValue.Builder
    abstract static class Builder<InputT> {
      abstract Builder<InputT> setCapacity(int c);
      abstract Builder<InputT> setTopK(int k);

      abstract GlobalSummary<InputT> build();
    }

    /**
     * Sets the capacity {@code c}.
     *
     * <p>Keep in mind that {@code c}
     *
     * @param k the top k most frequent elements to return.
     */
    public GlobalSummary<InputT> topKElements(int k) {
      return toBuilder().setTopK(k).build();
    }

    /**
     * Sets the capacity {@code c}.
     *
     * <p>Keep in mind that {@code c}
     *
     * @param c the capacity of the summary.
     */
    public GlobalSummary<InputT> withCapacity(int c) {
      return toBuilder().setCapacity(c).build();
    }

    @Override
    public PCollection<KV<InputT, Long>> expand(PCollection<InputT> input) {
      return input
              .apply("Compute Stream Summary",
                      Combine.<InputT, StreamSummary<InputT>>globally(MostFrequentFn.
                              <InputT>create(this.capacity())))
              .apply("Retrieve k most frequent elements",
                      ParDo.<StreamSummary<InputT>, KV<InputT, Long>>of(RetrieveTopK.
                              <InputT>globally(this.topK())));
    }
  }

  /**
   * Implementation of {@link #perKey()}.
   *
   * @param <K> type of the keys mapping the elements
   * @param <V> type of the values being combined per key
   */
  @AutoValue
  public abstract static class PerKeySummary<K, V>
          extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, List<KV<V, Long>>>>> {

    abstract int capacity();
    abstract int topK();
    abstract Builder<K, V> toBuilder();

    static <K, V> Builder<K, V> builder() {
      return new AutoValue_MostFrequent_PerKeySummary.Builder<K, V>()
              .setCapacity(1000)
              .setTopK(100);
    }

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setCapacity(int c);
      abstract Builder<K, V> setTopK(int k);

      abstract PerKeySummary<K, V> build();
    }

    /**
     * Sets the capacity {@code c}.
     *
     * <p>Keep in mind that {@code c}
     *
     * @param k the top k most frequent elements to return.
     */
    public PerKeySummary<K, V> topKElements(int k) {
      return toBuilder().setTopK(k).build();
    }

    /**
     * Sets the capacity {@code c}.
     *
     * <p>Keep in mind that {@code c}
     *
     * @param c the capacity of the summary.
     */
    public PerKeySummary<K, V> withCapacity(int c) {
      return toBuilder().setCapacity(c).build();
    }

    @Override
    public PCollection<KV<K, List<KV<V, Long>>>> expand(PCollection<KV<K, V>> input) {
      return input
              .apply("Compute Stream Summary",
                      Combine.<K, V, StreamSummary<V>>perKey(MostFrequentFn.
                              <V>create(this.capacity())))
              .apply("Retrieve k most frequent elements",
                      ParDo.<KV<K, StreamSummary<V>>, KV<K, List<KV<V, Long>>>>
                              of(RetrieveTopK.<K, V>perKey(this.topK())));
    }
  }

  /**
   * A {@code Combine.CombineFn} that computes the stream into a {@link StreamSummary}
   * sketch, useful as an argument to {@link Combine#globally} or {@link Combine#perKey}.
   *
   * <p>The Space-Saving algorithm summarizes the stream by using a doubly linked-list of buckets
   * ordered by the frequency value they represent. Each of these buckets contains a linked-list
   * of counters which estimate the {@code count} for an element as well as the maximum
   * overestimation {@code e} associated to it. The frequency cannot be overestimated.
   *
   * <p>An element is guaranteed to be in the top K most frequent if its guaranteed number of hits,
   * i.e. {@code count - e}, is greater than the count of the element at the position k+1.
   *
   * @param <InputT>         the type of the elements being combined
   */
  public static class MostFrequentFn<InputT>
          extends Combine.CombineFn<InputT, StreamSummary<InputT>, StreamSummary<InputT>> {

    private int capacity;

    private MostFrequentFn(int capacity) {
      this.capacity = capacity;
    }

    public static <T> MostFrequentFn<T> create(int capacity) {
      if (capacity <= 0) {
        throw new IllegalArgumentException("Capacity must be greater than 0.");
      }
      return new MostFrequentFn<>(capacity);
    }

    @Override
    public StreamSummary<InputT> createAccumulator() {
      return new StreamSummary<>(this.capacity);
    }

    @Override
    public StreamSummary<InputT> addInput(StreamSummary<InputT> accumulator, InputT element) {
      accumulator.offer(element, 1);
      return accumulator;
    }

    @Override
    public StreamSummary<InputT> mergeAccumulators(
            Iterable<StreamSummary<InputT>> accumulators) {
      Iterator<StreamSummary<InputT>> it = accumulators.iterator();
      if (it.hasNext()) {
        StreamSummary<InputT> mergedAccum = it.next();
        while (it.hasNext()) {
          StreamSummary<InputT> other = it.next();
          List<Counter<InputT>> top = other.topK(capacity);
          for (Counter<InputT> counter : top) {
            mergedAccum.offer(counter.getItem(), (int) counter.getCount());
          }
        }
        return mergedAccum;
      }
      return null;
    }

    @Override
    public StreamSummary<InputT> extractOutput(StreamSummary<InputT> accumulator) {
      return accumulator;
    }

    @Override
    public Coder<StreamSummary<InputT>> getAccumulatorCoder(CoderRegistry registry,
                                                       Coder inputCoder) {
      return new StreamSummaryCoder<>();
    }

    @Override
    public Coder<StreamSummary<InputT>> getDefaultOutputCoder(CoderRegistry registry,
                                                         Coder inputCoder) {
      return new StreamSummaryCoder<>();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
              .add(DisplayData.item("capacity", capacity)
                      .withLabel("Maximum number of elements kept tracked by the Summary"));
    }
  }

  /**
   * @param <T>
   */
  public static class StreamSummaryCoder<T> extends CustomCoder<StreamSummary<T>> {

    private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();

    @Override
    public void encode(StreamSummary<T> value, OutputStream outStream) throws IOException {
      BYTE_ARRAY_CODER.encode(value.toBytes(), outStream);
    }

    @Override
    public StreamSummary<T> decode(InputStream inStream) throws IOException {
      try {
        return new StreamSummary<>(BYTE_ARRAY_CODER.decode(inStream));
      } catch (ClassNotFoundException e) {
        throw new CoderException(e.getMessage()
                + " The stream summary can't be decoded from the input stream", e);
      }
    }
  }

  /**
   *
   */
  public static class RetrieveTopK {

    /**
     *
     * @param topK
     * @param <T>
     * @return
     */
    public static <T> DoFn<StreamSummary<T>, KV<T, Long>> globally(int topK) {
      return new DoFn<StreamSummary<T>, KV<T, Long>>() {

        final int topk = topK;

        @ProcessElement
        public void apply(ProcessContext c) {
          for (KV<T, Long> pair : getTopK(c.element(), topK)) {
            c.output(pair);
          }
        }
      };
    }

    /**
     *
     * @param topK
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> DoFn<KV<K, StreamSummary<V>>, KV<K, List<KV<V, Long>>>> perKey(int topK) {
      return new DoFn<KV<K, StreamSummary<V>>, KV<K, List<KV<V, Long>>>>() {

        final int topk = topK;

        @ProcessElement
        public void processElement(ProcessContext c) {
          KV<K, StreamSummary<V>> kv = c.element();
          c.output(KV.of(kv.getKey(), getTopK(kv.getValue(), topK)));
        }
      };
    }

    /**
     *
     * @param ss
     * @param k
     * @param <T>
     * @return
     */
    public static <T> List<KV<T, Long>> getTopK(StreamSummary<T> ss, int k) {
      List<KV<T, Long>> mostFrequent = new ArrayList<>(k);
      for (Counter<T> counter : ss.topK(k)) {
        mostFrequent.add(KV.of(counter.getItem(), counter.getCount()));
      }
      return mostFrequent;
    }
  }
}
