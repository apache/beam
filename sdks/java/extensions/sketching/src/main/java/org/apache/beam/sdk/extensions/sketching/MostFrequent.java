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
 * {@code PTransform}s for computing the most frequent elements of a stream.
 *
 * <p>This class uses the Stream Summary sketch which uses a HashMap to keep track of
 * a certain number of elements considered as the most frequent elements in a stream.
 *
 * <h2>References</h2>
 *
 * <p>This class uses the Space-Saving algorithm, introduced in the paper available <a href=
 * "https://pdfs.semanticscholar.org/72f1/5aba2e67b1cc9cd1fb12c99e101c4c1aae4b.pdf">here</a>.
 * <br>The implementation of the Stream Summary sketch comes from
 * <a href="https://github.com/addthis/stream-lib">Addthis' library Stream-lib</a>:
 *
 * <h2>Parameters</h2>
 *
 * <p>One unique parameter can be tuned in order to maximize the chances to have acurate results.
 *
 * <p>The Stream Summary uses a doubly linked-list of buckets in order to order elements
 * by frequency. Each bucket is associated with one or more Counters, which store the value
 * of an element as well as its frequency count and maximum potential absolute error {@code e}.
 * <br>An element is guaranteed to be in the top K most frequent if its guaranteed number
 * of hits, i.e. {@code count - e}, is greater than the count of the element at the position k+1.
 * <b>WARNING: Maximum potential error will always be underestimated as information is
 * lost during merging, so be careful when interpreting this value.</b>
 *
 * <p>The {@code capacity} of the Stream Summary sketch corresponds to the maximum number
 * of elements that can be tracked at once. They are stored in a HashMap. When the maximum
 * {@code capacity} is reached, the least frequent element will be replaced by the next
 * untracked element, and the value of its frequency will be added as the maximum potential
 * error of the new element.
 * <br> By default, the {@code capacity} is set to 10000 but this value depends highly on
 * the use case.
 *
 * <p><b>This sketch depends highly on the order of the incoming elements,
 * so please keep in mind the two following points:</b>
 * <li>
 *   <ul> This sketch should always be used in relatively short time windows because some
 *   highly frequent elements could appear late in the process, making them disregarded.</ul>
 *   <ul> If one wants the top k most frequent elements, then the {@code capacity}
 *   of the sketch should be significantly larger than k. It is based on the idea that most
 *   frequent elements will appear soon in the process, putting them on top of the linked-list
 *   so they will never be discarded when maximum {@code capacity} is reached.</ul>
 * </li>
 *
 * @TODO
 * <h2>Examples:</h2>
 *
 * <h4>Default globally use</h4>
 *
 * <p>Example of use
 * <pre>{@code PCollection<String> input = ...;
 * PCollection<StreamSummary<String>> ssSketch = input
 *        .apply(MostFrequent.<String>perKey(10000));
 * }</pre>
 *
 * <h4>Default perkey use with tuned paramters</h4>
 * <p>Example of use
 * <pre>{@code PCollection<KV<Integer, String>> input = ...;
 * PCollection<KV<Integer, StreamSummary<String>>> ssSketch = input
 *        .apply(MostFrequent.<Integer, String>globally(10000));
 * }</pre>
 */
@Experimental
public final class MostFrequent {

  /**
   * Computes and returns the top K most frequent elements in the input {@link PCollection}.
   *
   * @param <InputT>    the type of the elements in the input {@link PCollection}
   */
  public static <InputT> GlobalSummary<InputT> globally() {
    return GlobalSummary.<InputT>builder().build();
  }

  /**
   * Computes and returns the top K most frequent elements in the input
   * {@link PCollection} of {@link KV}s.
   *
   * @param <K>         the type of the keys mapping the elements
   * @param <V>         the type of the elements being combined per key
   */
  public static <K, V> PerKeySummary<K, V> perKey() {
    return PerKeySummary.<K, V>builder().build();
  }

  /** Implementation of {@link #globally()} */
  @AutoValue
  public abstract static class GlobalSummary<InputT>
          extends PTransform<PCollection<InputT>, PCollection<KV<InputT, Long>>> {

    abstract int capacity();
    abstract int topK();
    abstract Builder<InputT> toBuilder();

    static <InputT> Builder<InputT> builder() {
      return new AutoValue_MostFrequent_GlobalSummary.Builder<InputT>()
              .setCapacity(10000)
              .setTopK(100);
    }

    @AutoValue.Builder
    abstract static class Builder<InputT> {
      abstract Builder<InputT> setCapacity(int c);
      abstract Builder<InputT> setTopK(int k);

      abstract GlobalSummary<InputT> build();
    }

    /**
     * Sets the number {@code k} of most frequent element to retrieve.
     *
     * @param k the top k most frequent elements to return
     */
    public GlobalSummary<InputT> topKElements(int k) {
      return toBuilder().setTopK(k).build();
    }

    /**
     * Sets the capacity {@code c}.
     *
     * @param c the capacity of the summary
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

  /** Implementation of {@link #perKey()}. */
  @AutoValue
  public abstract static class PerKeySummary<K, V>
          extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, KV<V, Long>>>> {

    abstract int capacity();
    abstract int topK();
    abstract Builder<K, V> toBuilder();

    static <K, V> Builder<K, V> builder() {
      return new AutoValue_MostFrequent_PerKeySummary.Builder<K, V>()
              .setCapacity(10000)
              .setTopK(100);
    }

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setCapacity(int c);
      abstract Builder<K, V> setTopK(int k);

      abstract PerKeySummary<K, V> build();
    }

    /**
     * Sets the number {@code k} of most frequent elements to retrieve.
     *
     * @param k the top k most frequent elements to return
     */
    public PerKeySummary<K, V> topKElements(int k) {
      return toBuilder().setTopK(k).build();
    }

    /**
     * Sets the capacity {@code c}.
     *
     * @param c the capacity of the summary
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
   * Implements the {@link CombineFn} of {@link MostFrequent} transforms.
   *
   * <p> This is an abstract class. In practice you should use either {@link SerializableElements}
   * or {@link NonSerializableElements} because in the latter case elements will be wrapped in
   * {@link ElementWrapper} so they can be serialized thanks to a {@link Coder}.
   *
   * @param <InputT>         the type of the elements being combined
   * @param <ElementT>       the type of the elements actually taken by the accumulator
   */
  public abstract static class MostFrequentFn<InputT, ElementT>
      extends CombineFn<InputT, StreamSummary<ElementT>, StreamSummary<ElementT>> {

    protected int capacity;

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

    /** {@link MostFrequentFn} version to use when elements are serializable. */
    public static class SerializableElements<InputT> extends MostFrequentFn<InputT, InputT> {

      private SerializableElements(int capacity) {
        super(capacity);
      }

      /**
       * Creates a {@link MostFrequentFn} for serializable elements
       * with the given {@code capacity}.
       *
       * @param capacity the capacity of the summary
       */
      public static <InputT> SerializableElements<InputT> create(int capacity) {
        checkArgument(capacity > 0,
            "Capacity must be greater than 0 ! Actual: " + capacity);
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

    /** {@link MostFrequentFn} version to use when elements are not serializable. */
    public static class NonSerializableElements<InputT>
        extends MostFrequentFn<InputT, ElementWrapper<InputT>> {

      protected Coder<InputT> coder;

      private NonSerializableElements(int capacity, Coder<InputT> coder) {
        super(capacity);
        this.coder = coder;
      }

      /**
       * Creates a {@link MostFrequentFn} for non serializable elements
       * with the given {@link Coder}.
       *
       * @param coder     the coder of the elements to combine
       */
      public static <InputT> NonSerializableElements<InputT> create(Coder<InputT> coder) {
        try {
          coder.verifyDeterministic();
        } catch (Coder.NonDeterministicException e) {
          throw new IllegalArgumentException("Coder must be deterministic to perform this sketch."
              + e.getMessage(), e);
        }
        return new NonSerializableElements<>(10000, coder);
      }

      /**
       * Creates a {@link MostFrequentFn} for non serializable elements
       * with the given {@code capacity}.
       *
       * @param capacity  the capacity of the summary
       */
      public NonSerializableElements<InputT> withCapacity(int capacity) {
        checkArgument(capacity > 0,
            "Capacity must be greater than 0 ! Actual: " + capacity);
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
   * This class is used to wrap a non serializable type into a serializable wrapper which
   * uses the {@link Coder} as serializer. This wrapper is automatically used when calling
   * {@link MostFrequentFn.NonSerializableElements} Combine.
   */
  public static class ElementWrapper<T> implements Serializable {
    /** Returns a {@link ElementWrapper} with the given element to wrap and its coder. */
    public static <T> ElementWrapper<T> of(T element, Coder<T> coder) {
      return new ElementWrapper<>(element, coder);
    }

    private T element;
    private Coder<T> elemCoder;

    public ElementWrapper() {
    }

    private ElementWrapper(T element, Coder<T> coder) {
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

  /** Coder for {@link StreamSummary} class. */
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
        throw new CoderException("The stream cannot be decoded !");
      }
    }
  }

  /**
   * Utility class to retrieve the top K elements from a {@link PCollection}
   * or directly from a {@link StreamSummary} sketch.
   *
   * <p>This contains methods that retrieve the top k most frequent elements in
   * a {@link StreamSummary} sketch. Elements are returned in {@link KV}s with the
   * element value as key and its estimate frequency as value.
   */
  public static class RetrieveTopK {

    /**
     * {@link DoFn} that retrieves the top k most frequent elements in a {@link PCollection} of
     * {@link StreamSummary} containing non serializable objects wrapped in {@link ElementWrapper}.
     *
     * @param k     the top k most frequent elements to return
     */
    public static <T> DoFn<
        StreamSummary<ElementWrapper<T>>, KV<T, Long>> globallyWrapped(int k) {
      return new DoFn<StreamSummary<ElementWrapper<T>>, KV<T, Long>>() {
        @ProcessElement
        public void apply(ProcessContext c) {
          for (KV<ElementWrapper<T>, Long> pair : getTopK(c.element(), k)) {
            c.output(KV.of(pair.getKey().getElement(), pair.getValue()));
          }
        }
      };
    }

    /**
     * {@link DoFn} that retrieves the top k most frequent elements in a
     * {@link PCollection} of {@link StreamSummary} containing serializable objects.
     *
     * @param k     the top k most frequent elements to return
     */
    public static <T> DoFn<StreamSummary<T>, KV<T, Long>> globallyUnWrapped(int k) {
      return new DoFn<StreamSummary<T>, KV<T, Long>>() {
        @ProcessElement
        public void apply(ProcessContext c) {
          for (KV<T, Long> pair : getTopK(c.element(), k)) {
            c.output(KV.of(pair.getKey(), pair.getValue()));
          }
        }
      };
    }

    /**
     * {@link DoFn} that retrieves the top k most frequent elements per key in a
     * {@link PCollection} of {@link KV}s with {@link StreamSummary} containing
     * non serializable objects wrapped in {@link ElementWrapper}.
     *
     * @param k     the top k most frequent elements to return
     */
    public static <K, V> DoFn<
        KV<K, StreamSummary<ElementWrapper<V>>>, KV<K, KV<V, Long>>> perKeyWrapped(int k) {
      return new DoFn<KV<K, StreamSummary<ElementWrapper<V>>>, KV<K, KV<V, Long>>>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          KV<K, StreamSummary<ElementWrapper<V>>> kv = c.element();
          for (KV<ElementWrapper<V>, Long> pair : getTopK(kv.getValue(), k)) {
            c.output(KV.of(
                kv.getKey(),
                KV.of(pair.getKey().getElement(), pair.getValue())));
          }
        }
      };
    }

    /**
     * {@link DoFn} that retrieves the top k most frequent elements per key in a
     * {@link PCollection} of {@link KV}s with {@link StreamSummary} containing
     * serializable objects.
     *
     * @param k     the top k most frequent elements to return
     */
    public static <K, V> DoFn<
        KV<K, StreamSummary<V>>, KV<K, KV<V, Long>>> perKeyUnWrapped(int k) {
      return new DoFn<KV<K, StreamSummary<V>>, KV<K, KV<V, Long>>>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          KV<K, StreamSummary<V>> kv = c.element();
          for (KV<V, Long> pair : getTopK(kv.getValue(), k)) {
            c.output(KV.of(
                kv.getKey(),
                KV.of(pair.getKey(), pair.getValue())));
          }
        }
      };
    }

    /**
     * Retrieve the {@code k} most frequent elements in a {@link StreamSummary} sketch
     * and returns a list of {@link KV}s where the pair at index i represents the ith
     * most frequent element.
     * <br><b>If one also wants to retrieve the estimate maximum potential error,
     * a customized method should be used.</b>
     *
     * @param k     the top k most frequent elements to return
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
