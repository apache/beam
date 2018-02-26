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
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
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
 * <h2>Brief overview of the sketch</h2>
 *
 * <p>The Stream Summary uses a doubly linked-list of buckets in order to order elements
 * by frequency. Each bucket is associated with a list of Counters, each of them storing
 * information about one of the tracked elements : value, count and maximum potential error.
 * <br>
 *   An element is guaranteed to be in the top K most frequent if its guaranteed number
 *   of hits, i.e. the {@code count minus error}, is greater than the count of the element at
 *   the position k+1.
 * <br>
 *   <b>WARNING</b>: Information about the error is lost during the parallelization,
 *   so this value may be underestimated in most cases, that is why the error is
 *   not output by default. This issue will be solved in the next version of the extension.
 *
 * <h2>References</h2>
 *
 * <p>This class uses the Space-Saving algorithm, introduced in the paper available <a href=
 * "https://pdfs.semanticscholar.org/72f1/5aba2e67b1cc9cd1fb12c99e101c3c1aae3b.pdf">here</a>.
 * <br>The implementation of the Stream Summary sketch comes from
 * <a href="https://github.com/addthis/stream-lib">Addthis' library Stream-lib</a>:
 *
 * <h2>Parameters</h2>
 *
 * <p>Two parameters can be tuned:
 *
 * <ul>
 *   <li>The top {@code k} most frequent elements you want to output from the stream.<br>
 *     By default, {@code k} is set to {@code 100}.
 *   <li>The {@code capacity} of the Stream Summary sketch. It corresponds to
 *   the maximum number of elements that can be tracked at once.
 *   When the maximum {@code capacity} is reached, the least frequent element
 *   will be replaced by the next untracked element.<br>
 *     By default, the {@code capacity} is set to {@code 10000} but this value depends
 *     highly on the use case and more specifically on the order of incoming elements.
 * </ul>
 *
 *  <p><b>WARNING</b>: Keep in mind that he {@code capacity} should always be
 *  significantly higher than {@code k}. One must ask himself the following question:<br>
 *    What is the lowest rank {@code x} such that any element arriving at a rank {@code y > x}
 *    has a relatively small probability (you decide) to actually become one the {@code k}
 *    most frequent elements by the end of the stream ?<br>
 *      Then the {@code capacity} should at least be equal to {@code x}
 *
 * <h2>Examples:</h2>
 *
 * <p>There are 2 ways of using this class:
 *
 * <ul>
 *   <li>Use the {@link PTransform}s that return a {@link PCollection} of {@link KV}s,
 *   each of them associating one of the estimate top {@code k} most frequent elements
 *   to its frequency count.
 *   <li>Use one of the two {@link MostFrequentFn} combines that is exposed in order
 *   to make advanced processing using directly the {@link StreamSummary} sketch. One
 *   of the combines is used for {@link Serializable} elements while the second one is
 *   designed to handle non {@link Serializable} elements by wrapping them in an
 *   {@link ElementWrapper}.
 * </ul>
 *
 * <h3>Using the {@link PTransform}s</h3>
 *
 * <h4>Default globally use</h4>
 *
 * <pre><code>
 * {@literal PCollection<MyClass>} input = ...;
 * {@literal PCollection<KV<MyClass, Long>>} output = input
 *     .apply(MostFrequent.globally());
 * </code></pre>
 *
 * <h4>Per key use with tuned parameters</h4>
 *
 * <pre><code>
 * int k = 1000;
 * int capacity = 1000000;
 * {@literal PCollection<KV<Integer, MyClass>>} input = ...;
 * {@literal PCollection<KV<Integer, KV<MyClass, Long>>>} output = input
 *     .apply(MostFrequent.perKey()
 *        .withCapacity(capacity)
 *        .topKElements(k));
 * </code></pre>
 *
 * <h3>Using the {@link CombineFn}s</h3>
 *
 * <h4>Case with {@link Serializable} elements</h4>
 *
 * <pre><code>
 * int capacity = 1000000;
 * {@literal PCollection<SerializableClass>} input = ...;
 * {@literal PCollection<StreamSummarySketch<SerializableClass>>} output = input
 *     .apply(Combine.globally(MostFrequent.MostFrequentFn.SerializableElements
 *        .create(capacity)));
 * </code></pre>
 *
 * <h4>Case with non {@link Serializable} elements</h4>
 *
 * <pre><code>
 * int capacity = 1000000;
 * {@literal PCollection<NonSerializableClass>} input = ...;
 * {@literal PCollection<StreamSummarySketch<NonSerializableClass>>} output = input
 *     .apply(Combine.globally(MostFrequent.MostFrequentFn.NonSerializableElements
 *        .create(new NonSerializableClassCoder())
 *        .withCapacity(capacity)));
 * </code></pre>
 *
 * <h3>Query the sketch</h3>
 *
 * <p>There are two ways of getting top k elements from the sketch:
 *
 * <ul>
 *   <li>Using directly the {@link StreamSummarySketch#estimateTopK(int)} method that
 *   will return a {@link List} of {@link KV}s pairing elements to their frequency count.
 *   <li>Using {@link RetrieveTopK} utility class which contains {@link DoFn}s to process
 *   {@link PCollection} of {@link StreamSummarySketch} globally or per key, with serializable
 *   or non serializable elements. This {@link DoFn}s are used by default {@link PTransform}s.
 * </ul>
 *
 * <h4>Query the resulting {@link PCollection} containing the sketch.</h4>
 *
 * <pre><code>
 * int k = 100;
 * int capacity = 100000
 * {@literal PCollection<StreamSummarySketch<MyClass>>} input = ...;
 * {@literal PCollection<KV<MyClass, Long>>} output = input
 *     .apply(MostFrequent.MostFrequentFn.NonSerializableElements
 *        .create(new MyClassCoder())
 *        .withCapacity(capacity))
 *     .apply(ParDo.of(RetrieveTopK.globallyNonSerializable()));
 * </code></pre>
 *
 * <b>Warning: this class is experimental.</b> <br>
 * Its API is subject to change in future versions of Beam.
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

  /** Implementation of {@link #globally()}. */
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
            .apply("Compute Stream Summary globally serializable", Combine.globally(fn))
            .apply("Retrieve k most frequent elements", ParDo
                .of(RetrieveTopK.globallySerializable(this.topK())))
            .setCoder(KvCoder.of(input.getCoder(), VarLongCoder.of()));
      } else {
        MostFrequentFn.NonSerializableElements<InputT> fn = MostFrequentFn.NonSerializableElements
            .create(inputCoder).withCapacity(this.capacity());

        result = input
            .apply("Compute Stream Summary globally non serializable", Combine.globally(fn))
            .apply("Retrieve k most frequent elements", ParDo
                .of(RetrieveTopK.globallyNonSerializable(this.topK())))
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
            .apply("Compute Stream Summary per key serializable", Combine.perKey(fn))
            .apply("Retrieve k most frequent elements", ParDo
                .of(RetrieveTopK.perKeySerializable(this.topK())))
            .setCoder(KvCoder.of(
                inputCoder.getKeyCoder(),
                KvCoder.of(
                    inputCoder.getValueCoder(),
                    VarLongCoder.of())));
      } else {
        MostFrequentFn.NonSerializableElements<V> fn = MostFrequentFn.NonSerializableElements
            .create(valueCoder).withCapacity(this.capacity());

        result = input
            .apply("Compute Stream Summary per key non serializable", Combine.perKey(fn))
            .apply("Retrieve k most frequent elements", ParDo
                .of(RetrieveTopK.perKeyNonSerializable(this.topK())))
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
   * <p>This is an abstract class. In practice you should use either {@link SerializableElements}
   * or {@link NonSerializableElements}. In the latter case elements will be wrapped into an
   * {@link ElementWrapper} so they can be serialized via a {@link Coder}.
   *
   * @param <InputT>         the type of the elements being combined
   * @param <ElementT>       the type of the elements actually taken by the accumulator
   */
  public abstract static class MostFrequentFn<InputT, ElementT>
      extends CombineFn<InputT, StreamSummarySketch<ElementT>, StreamSummarySketch<ElementT>> {

    protected int capacity;

    private MostFrequentFn(int capacity) {
      this.capacity = capacity;
    }

    @Override public StreamSummarySketch<ElementT> mergeAccumulators(
        Iterable<StreamSummarySketch<ElementT>> accumulators) {
      Iterator<StreamSummarySketch<ElementT>> it = accumulators.iterator();
      StreamSummarySketch<ElementT> mergedAccum = it.next();
      it.forEachRemaining((StreamSummarySketch<ElementT> accum) -> mergedAccum.merge(accum));
      return mergedAccum;
    }

    @Override public StreamSummarySketch<ElementT> extractOutput(StreamSummarySketch<ElementT> ss) {
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

      @Override public StreamSummarySketch<InputT> createAccumulator() {
        return new StreamSummarySketch<>(capacity);
      }

      @Override public StreamSummarySketch<InputT> addInput(
          StreamSummarySketch<InputT> accumulator, InputT input) {
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

      @Override public StreamSummarySketch<ElementWrapper<InputT>> createAccumulator() {
        return new StreamSummarySketch<>(capacity);
      }

      @Override public StreamSummarySketch<ElementWrapper<InputT>> addInput(
          StreamSummarySketch<ElementWrapper<InputT>> accumulator, InputT input) {
        accumulator.offer(ElementWrapper.of(input, coder));
        return accumulator;
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
     * {@link DoFn} that retrieves the top k most frequent elements in
     * a {@link PCollection} of {@link StreamSummarySketch} containing
     * non serializable objects wrapped in {@link ElementWrapper}.
     *
     * @param k     the top k most frequent elements to return
     */
    public static <T> DoFn<StreamSummarySketch<ElementWrapper<T>>,
        KV<T, Long>> globallyNonSerializable(int k) {
      return new DoFn<StreamSummarySketch<ElementWrapper<T>>, KV<T, Long>>() {
        @ProcessElement
        public void apply(ProcessContext c) {
          for (KV<ElementWrapper<T>, Long> pair : c.element().estimateTopK(k)) {
            c.output(KV.of(pair.getKey().getElement(), pair.getValue()));
          }
        }
      };
    }

    /**
     * {@link DoFn} that retrieves the top k most frequent elements in a
     * {@link PCollection} of {@link StreamSummarySketch} containing serializable objects.
     *
     * @param k     the top k most frequent elements to return
     */
    public static <T> DoFn<StreamSummarySketch<T>, KV<T, Long>> globallySerializable(int k) {
      return new DoFn<StreamSummarySketch<T>, KV<T, Long>>() {
        @ProcessElement
        public void apply(ProcessContext c) {
          List<KV<T, Long>> top = c.element().estimateTopK(k);
          for (KV<T, Long> pair : top) {
            c.output(KV.of(pair.getKey(), pair.getValue()));
          }
        }
      };
    }

    /**
     * {@link DoFn} that retrieves the top k most frequent elements per key in a
     * {@link PCollection} of {@link KV}s with {@link StreamSummarySketch} containing
     * non serializable objects wrapped in {@link ElementWrapper}.
     *
     * @param k     the top k most frequent elements to return
     */
    public static <K, V> DoFn<KV<K, StreamSummarySketch<ElementWrapper<V>>>,
        KV<K, KV<V, Long>>> perKeyNonSerializable(int k) {
      return new DoFn<KV<K, StreamSummarySketch<ElementWrapper<V>>>, KV<K, KV<V, Long>>>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          KV<K, StreamSummarySketch<ElementWrapper<V>>> kv = c.element();
          for (KV<ElementWrapper<V>, Long> pair : kv.getValue().estimateTopK(k)) {
            c.output(KV.of(
                kv.getKey(),
                KV.of(pair.getKey().getElement(), pair.getValue())));
          }
        }
      };
    }

    /**
     * {@link DoFn} that retrieves the top k most frequent elements per key in a
     * {@link PCollection} of {@link KV}s with {@link StreamSummarySketch} containing
     * serializable objects.
     *
     * @param k     the top k most frequent elements to return
     */
    public static <K, V> DoFn<KV<K, StreamSummarySketch<V>>,
        KV<K, KV<V, Long>>> perKeySerializable(int k) {
      return new DoFn<KV<K, StreamSummarySketch<V>>, KV<K, KV<V, Long>>>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          KV<K, StreamSummarySketch<V>> kv = c.element();
          for (KV<V, Long> pair : kv.getValue().estimateTopK(k)) {
            c.output(KV.of(
                kv.getKey(),
                KV.of(pair.getKey(), pair.getValue())));
          }
        }
      };
    }
  }

  /** Wrap StreamLib's Stream Summary sketch. */
  public static class StreamSummarySketch<T> extends StreamSummary<T> {

    public StreamSummarySketch() {
      super();
    }

    public StreamSummarySketch(int capacity) {
      super(capacity);
    }

    /** Merge several {@link StreamSummarySketch} into this one. */
    public void merge(StreamSummarySketch<T>... sketches) {
      for (StreamSummarySketch<T> sketch : sketches) {
        List<Counter<T>> top = sketch.topK(sketch.size());
        for (Counter<T> counter : top) {
          offer(counter.getItem(), (int) counter.getCount());
        }
      }
    }

    /**
     * Retrieves the {@code k} most frequent elements in a {@link StreamSummarySketch}
     * sketch and returns a list of {@link KV}s where the pair at index i represents
     * the ith most frequent element associated with its estimate count.
     *
     * @param k     the top k most frequent elements to return
     */
    public List<KV<T, Long>> estimateTopK(int k) {
      List<Counter<T>> top = super.topK(k);
      List<KV<T, Long>> output = new ArrayList<>(k);
      for (Counter<T> c : top) {
        output.add(KV.of(c.getItem(), c.getCount()));
      }
      return output;
    }

    @Override
    public int hashCode() {
      return this.toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      } else if (!(o instanceof StreamSummarySketch)) {
        return false;
      }
      StreamSummarySketch that = (StreamSummarySketch) o;
      return this.toString().equals(that.toString());
    }
  }

  /**
   * This class is used to wrap a non serializable type into a serializable wrapper which
   * uses the {@link Coder} as serializer. This wrapper is automatically used when calling
   * {@link MostFrequentFn.NonSerializableElements} Combine.
   */
  public static class ElementWrapper<T> implements Externalizable {
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

    @Override
    public String toString() {
      return element.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      } else if (!(o instanceof ElementWrapper)) {
        return false;
      }
      ElementWrapper that = (ElementWrapper) o;
      return this.element.equals(that.element);
    }

    @Override public void writeExternal(ObjectOutput out) throws IOException {
      byte[] elemBytes = CoderUtils.encodeToByteArray(elemCoder, element);
      int length = elemBytes.length;
      out.writeInt(length);
      out.write(elemBytes);
      out.writeObject(elemCoder);
    }

    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException {
      int length = in.readInt();
      byte[] elemBytes = new byte[length];
      for (int i = 0; i < length; i++) {
        elemBytes[i] = in.readByte();
      }
      elemCoder = (Coder<T>) in.readObject();
      this.element = CoderUtils.decodeFromByteArray(elemCoder, elemBytes);
    }
  }

  /** Coder for {@link StreamSummarySketch} class. */
  public static class StreamSummarySketchCoder<T> extends CustomCoder<StreamSummarySketch<T>> {

    @Override
    public void encode(StreamSummarySketch<T> value, OutputStream outStream) throws IOException {
      ObjectOutputStream oos = new ObjectOutputStream(outStream);
      value.writeExternal(oos);
    }

    @Override
    public StreamSummarySketch<T> decode(InputStream inStream) throws IOException {
      ObjectInputStream ois = new ObjectInputStream(inStream);
      try {
        StreamSummarySketch<T> ss = new StreamSummarySketch<>();
        ss.readExternal(ois);
        return ss;
      } catch (ClassNotFoundException e) {
        throw new CoderException("The stream cannot be decoded !");
      }
    }

    @Override public boolean consistentWithEquals() {
      return true;
    }
  }

  /** Coder for {@link ElementWrapper} class. */
  public static class ElementWrapperCoder<T> extends CustomCoder<ElementWrapper<T>> {

    @Override
    public void encode(ElementWrapper<T> value, OutputStream outStream) throws IOException {
      ObjectOutputStream oos = new ObjectOutputStream(outStream);
      value.writeExternal(oos);
    }

    @Override
    public ElementWrapper<T> decode(InputStream inStream) throws IOException {
      ObjectInputStream ois = new ObjectInputStream(inStream);
      ElementWrapper<T> decoded = new ElementWrapper<>();
      try {
        decoded.readExternal(ois);
        return decoded;
      } catch (ClassNotFoundException e) {
        throw new CoderException("The stream cannot be decoded !");
      }
    }
  }
}
