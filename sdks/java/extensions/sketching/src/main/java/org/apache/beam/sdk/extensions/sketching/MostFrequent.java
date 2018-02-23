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

import static com.google.common.base.Preconditions.checkArgument;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.CoderUtils;
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
      final Coder<InputT> inputCoder = input.getCoder();
      Class clazz = inputCoder.getEncodedTypeDescriptor().getRawType();
      PCollection<KV<InputT, Long>> result;

      if (Serializable.class.isAssignableFrom(clazz)) {
        MostFrequentFn.SerializableElements<InputT> fn = MostFrequentFn.SerializableElements
            .create(this.capacity());

        result = input
            .apply("Compute Stream Summary", Combine.globally(fn))
            .apply("Retrieve k most frequent elements", ParDo
                .of(RetrieveTopK.globallyUnWrapped(this.topK())))
            .setCoder(KvCoder.of(input.getCoder(), VarLongCoder.of()));
      } else {
        MostFrequentFn.NonSerializableElements<InputT> fn = MostFrequentFn.NonSerializableElements
            .create(inputCoder).withCapacity(this.capacity());

        result = input
            .apply("Compute Stream Summary", Combine.globally(fn))
            .apply("Retrieve k most frequent elements", ParDo
                .of(RetrieveTopK.globallyWrapped(this.topK())))
            .setCoder(KvCoder.of(input.getCoder(), VarLongCoder.of()));
      }
      return result;
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
          extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, KV<V, Long>>>> {

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
    public PCollection<KV<K, KV<V, Long>>> expand(PCollection<KV<K, V>> input) {
      final KvCoder<K, V> inputCoder = (KvCoder<K, V>) input.getCoder();
      final Coder<V> valueCoder = inputCoder.getValueCoder();
      Class clazz = inputCoder.getValueCoder().getEncodedTypeDescriptor().getRawType();
      PCollection<KV<K, KV<V, Long>>> result;

      if (Serializable.class.isAssignableFrom(clazz)) {
        MostFrequentFn.SerializableElements<V> fn = MostFrequentFn.SerializableElements
            .create(this.capacity());

        result = input
            .apply("Compute Stream Summary", Combine.perKey(fn))
            .apply("Retrieve k most frequent elements", ParDo
                .of(RetrieveTopK.perKeyUnWrapped(this.topK())))
            .setCoder(KvCoder.of(
                inputCoder.getKeyCoder(),
                KvCoder.of(
                    inputCoder.getValueCoder(),
                    VarLongCoder.of())));
      } else {
        MostFrequentFn.NonSerializableElements<V> fn = MostFrequentFn.NonSerializableElements
            .create(valueCoder).withCapacity(this.capacity());

        result = input
            .apply("Compute Stream Summary", Combine.perKey(fn))
            .apply("Retrieve k most frequent elements", ParDo
                .of(RetrieveTopK.perKeyWrapped(this.topK())))
            .setCoder(KvCoder.of(
                inputCoder.getKeyCoder(),
                KvCoder.of(
                    inputCoder.getValueCoder(),
                    VarLongCoder.of())));
      }
      return result;
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


  public abstract static class MostFrequentFn<InputT, ElementT>
      extends CombineFn<InputT, StreamSummary<ElementT>, StreamSummary<ElementT>> {

    int capacity;

    private MostFrequentFn(int capacity) {
      this.capacity = capacity;
    }

    @Override public StreamSummary<ElementT> mergeAccumulators(
        Iterable<StreamSummary<ElementT>> accumulators) {
      Iterator<StreamSummary<ElementT>> it = accumulators.iterator();
      StreamSummary<ElementT> mergedAccum = it.next();
      while (it.hasNext()) {
        StreamSummary<ElementT> other = it.next();
        List<Counter<ElementT>> top = other.topK(other.size());
        for (Counter<ElementT> counter : top) {
          mergedAccum.offer(counter.getItem(), (int) counter.getCount());
        }
      }
      return mergedAccum;
    }

    @Override public StreamSummary<ElementT> extractOutput(StreamSummary<ElementT> ss) {
      return ss;
    }

    @Override public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(DisplayData
              .item("capacity", capacity)
              .withLabel("Capacity of the Stream Summary sketch"));
    }

    /**
     *
     * @param <InputT>
     */
    public static class SerializableElements<InputT> extends MostFrequentFn<InputT, InputT> {

      SerializableElements(int capacity) {
        super(capacity);
      }

      public static <InputT> SerializableElements<InputT> create(int capacity) {
        return new SerializableElements<>(capacity);
      }

      @Override public StreamSummary<InputT> createAccumulator() {
        return new StreamSummary<>(capacity);
      }

      @Override public StreamSummary<InputT> addInput(
          StreamSummary<InputT> accumulator, InputT input) {
        accumulator.offer(input);
        return accumulator;
      }
    }

    /**
     *
     * @param <InputT>
     */
    public static class NonSerializableElements<InputT>
        extends MostFrequentFn<InputT, ElementWrapper<InputT>> {

      Coder<InputT> coder;

      NonSerializableElements(int capacity, Coder<InputT> coder) {
        super(capacity);
        this.coder = coder;
      }

      public static <InputT> NonSerializableElements<InputT> create(Coder<InputT> coder) {
        return new NonSerializableElements<>(1000, coder);
      }

      public NonSerializableElements<InputT> withCapacity(int capacity) {
        checkArgument(capacity > 0, "Capacity must be greater than 0 ! Actual: " + capacity);
        return new NonSerializableElements<>(capacity, coder);
      }

      @Override public StreamSummary<ElementWrapper<InputT>> createAccumulator() {
        return new StreamSummary<>(capacity);
      }

      @Override public StreamSummary<ElementWrapper<InputT>> addInput(
          StreamSummary<ElementWrapper<InputT>> accumulator, InputT input) {
        accumulator.offer(ElementWrapper.of(input, coder));
        return accumulator;
      }
    }
  }

  /**
   *
   * @param <T>
   */
  public static class ElementWrapper<T> implements Serializable {
    public static <T> ElementWrapper<T> of(T element, Coder<T> coder) {
      return new ElementWrapper<>(element, coder);
    }

    private T element;
    private Coder<T> elemCoder;

    public ElementWrapper() {
    }

    public ElementWrapper(T element, Coder<T> coder) {
      this.element = element;
      this.elemCoder = coder;
    }

    public T getElement() {
      return element;
    }

    public Coder<T> getCoder() {
      return elemCoder;
    }

    @Override
    public int hashCode() {
      return element.hashCode();
    }

    @SuppressWarnings("unchecked")
    protected void readObject(ObjectInputStream in)
            throws IOException, ClassNotFoundException {
      elemCoder = (Coder<T>) in.readObject();
      int length = in.readInt();
      byte[] elemBytes = new byte[length];
      for (int i = 0; i < length; i++) {
       elemBytes[i] = in.readByte();
      }
      this.element = CoderUtils.decodeFromByteArray(elemCoder, elemBytes);
      in.close();
    }

    protected void writeObject(ObjectOutputStream out)
            throws IOException {
      out.writeObject(elemCoder);
      byte[] elemBytes = CoderUtils.encodeToByteArray(elemCoder, element);
      int length = elemBytes.length;
      out.writeInt(length);
      out.write(elemBytes);
      out.close();
    }
  }

  /**
   *
   */
  public static class StreamSummaryCoder<T>
          extends CustomCoder<StreamSummary<T>> {
    private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();

    @Override
    public void encode(StreamSummary<T> value, OutputStream outStream)
            throws IOException {
      BYTE_ARRAY_CODER.encode(value.toBytes(), outStream);
    }

    @Override
    public StreamSummary<T> decode(InputStream inStream)
            throws IOException {
      try {
        return new StreamSummary<>(BYTE_ARRAY_CODER.decode(inStream));
      } catch (ClassNotFoundException e) {
        throw new CoderException("The stream cannot be decoded !");
      }
    }
  }

  /**
   *
   */
  static class RetrieveTopK {
    /**
     *
     * @param topK
     * @param <T>
     */
    public static <T> DoFn<
        StreamSummary<ElementWrapper<T>>, KV<T, Long>> globallyWrapped(int topK) {
      return new DoFn<StreamSummary<ElementWrapper<T>>, KV<T, Long>>() {
        @ProcessElement
        public void apply(ProcessContext c) {
          for (KV<ElementWrapper<T>, Long> pair : getTopK(c.element(), topK)) {
            c.output(KV.of(pair.getKey().getElement(), pair.getValue()));
          }
        }
      };
    }

    /**
     *
     * @param topK
     * @param <T>
     */
    public static <T> DoFn<StreamSummary<T>, KV<T, Long>> globallyUnWrapped(int topK) {
      return new DoFn<StreamSummary<T>, KV<T, Long>>() {
        @ProcessElement
        public void apply(ProcessContext c) {
          for (KV<T, Long> pair : getTopK(c.element(), topK)) {
            c.output(KV.of(pair.getKey(), pair.getValue()));
          }
        }
      };
    }

    /**
     *
     * @param topK
     * @param <K>
     * @param <V>
     */
    public static <K, V> DoFn<
        KV<K, StreamSummary<V>>, KV<K, KV<V, Long>>> perKeyUnWrapped(int topK) {
      return new DoFn<KV<K, StreamSummary<V>>, KV<K, KV<V, Long>>>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          KV<K, StreamSummary<V>> kv = c.element();
          for (KV<V, Long> pair : getTopK(kv.getValue(), topK)) {
            c.output(KV.of(
                kv.getKey(),
                KV.of(pair.getKey(), pair.getValue())));
          }
        }
      };
    }

    /**
     *
     * @param topK
     * @param <K>
     * @param <V>
     */
    public static <K, V> DoFn<
        KV<K, StreamSummary<ElementWrapper<V>>>, KV<K, KV<V, Long>>> perKeyWrapped(int topK) {
      return new DoFn<KV<K, StreamSummary<ElementWrapper<V>>>, KV<K, KV<V, Long>>>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          KV<K, StreamSummary<ElementWrapper<V>>> kv = c.element();
          for (KV<ElementWrapper<V>, Long> pair : getTopK(kv.getValue(), topK)) {
            c.output(KV.of(
                kv.getKey(),
                KV.of(pair.getKey().getElement(), pair.getValue())));
          }
        }
      };
    }

    /**
     *
     * @param ss
     * @param k
     * @param <T>
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
