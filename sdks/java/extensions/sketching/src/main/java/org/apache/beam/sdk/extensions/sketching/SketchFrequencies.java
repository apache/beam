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

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.clearspring.analytics.stream.frequency.FrequencyMergeException;
import com.google.auto.value.AutoValue;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Iterator;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@code PTransform}s to compute the estimate frequency of each element in a stream.
 *
 * <p>This class uses the Count-min Sketch structure which allows very efficient queries
 * on the data stream summarization.
 *
 * <h2>References</h2>
 *
 * <p>The implementation comes from <a href="https://github.com/addthis/stream-lib">
 * Addthis' Stream-lib library</a>. <br>
 * The papers and other useful information about Count-Min Sketch are available on <a
 * href="https://sites.google.com/site/countminsketch/">this website</a>. <br>
 *
 * <h2>Parameters</h2>
 *
 * <p>Two parameters can be tuned in order to control the accuracy of the computation:
 *
 * <ul>
 *   <li><b>Relative Error:</b> <br>
 *       The relative error "{@code epsilon}" controls the accuracy of the estimation.
 *       By default, the relative is around {@code 1%} of the total count.
 *   <li><b>Confidence</b> <br>
 *       The relative error can be guaranteed only with a certain "{@code confidence}",
 *       between 0 and 1 (1 being of course impossible). <br>
 *       The default value is set to 0.999 meaning that we can guarantee
 *       that the relative error will not exceed 1% of the total count in 99.9% of cases.
 * </ul>
 *
 * <p>These two parameters will determine the size of the Count-min sketch, which is
 * a two-dimensional array with depth and width defined as follows :
 * <ul>
 *   <li>{@code width = ceil(2 / epsilon)}</li>
 *   <li>{@code depth = ceil(-log(1 - confidence) / log(2))}</li>
 * </ul>
 *
 * <p>With the default values, this gives a depth of 200 and a width of 10.
 *
 * <p><b>WARNING:</b> The relative error concerns the total number of distinct elements
 * in a stream. Thus, an element having 1000 occurrences in a stream of 1 million distinct
 * elements will have 1% of 1 million as relative error, i.e. 10 000. This means the frequency
 * is 1000 +/- 10 000 for this element. Therefore this is obvious that the relative error must
 * be really low in very large streams. <br>
 * Also keep in mind that this algorithm works well on highly skewed data but gives poor
 * results if the elements are evenly distributed.
 *
 * <h2>Examples</h2>
 *
 * <p>There are 2 ways of using this class:
 *
 * <ul>
 *   <li>Use the {@link PTransform}s that return a {@link PCollection} singleton that contains
 *       a Count-min sketch for querying the estimate number of hits of the elements.
 *   <li>Use the {@link CountMinSketchFn} {@code CombineFn} that is exposed in order to make
 *       advanced processing involving the Count-Min sketch.
 * </ul>
 *
 * <h3>Example 1: default use</h3>
 *
 * <p>The simplest use is to call the {@link #globally()} or {@link #perKey()} method in
 * order to retrieve the sketch with an estimate number of hits for each element in the stream.
 *
 * <pre><code>
 * {@literal PCollection<MyObject>} pc = ...;
 * {@literal PCollection<CountMinSketch>} countMinSketch = pc.apply(SketchFrequencies
 * {@literal        .<MyObject>}globally()); //{@literal .<MyObject>}perKey();
 * </code></pre>
 *
 * <h3>Example 2: tune accuracy parameters</h3>
 *
 * <p>One can tune the {@code epsilon} and {@code confidence} parameters in order to
 * control accuracy and memory. <br>
 * The tuning works exactly the same for {@link #globally()} and {@link #perKey()}.
 *
 * <pre><code>
 *  double eps = 0.001;
 *  double conf = 0.9999;
 * {@literal PCollection<MyObject>} pc = ...;
 * {@literal PCollection<CountMinSketch>} countMinSketch = pc.apply(SketchFrequencies
 * {@literal  .<MyObject>}globally() //{@literal .<MyObject>}perKey()
 *            .withRelativeError(eps)
 *            .withConfidence(conf));
 * </code></pre>
 *
 * <h3>Example 3: query the resulting sketch</h3>
 *
 * <p>This example shows how to query the resulting {@link Sketch}.
 * To estimate the number of hits of an element, one has to use
 * {@link Sketch#estimateCount(Object, Coder)} method and to provide
 * the coder for the element type. <br>
 * For instance, one can build a KV Pair linking each element to an estimation
 * of its frequency, using the sketch as side input of a {@link ParDo}. <br>
 *
 * <pre><code>
 * {@literal PCollection<MyObject>} pc = ...;
 * {@literal PCollection<CountMinSketch>} countMinSketch = pc.apply(SketchFrequencies
 * {@literal       .<MyObject>}globally());
 *
 * // Retrieve the coder for MyObject
 * final{@literal Coder<MyObject>} = pc.getCoder();
 * // build a View of the sketch so it can be passed a sideInput
 * final{@literal PCollectionView<CountMinSketch>} sketchView = sketch.apply(View
 * {@literal       .<CountMinSketch>}asSingleton());
 *
 * {@literal PCollection<KV<MyObject, Long>>} pairs = pc.apply(ParDo.of(
 *        {@literal new DoFn<Long, KV<MyObject, Long>>()} {
 *          {@literal @ProcessElement}
 *           public void procesElement(ProcessContext c) {
 *             Long elem = c.element();
 *             CountMinSketch sketch = c.sideInput(sketchView);
 *             c.output(sketch.estimateCount(elem, coder));
 *            }}).withSideInputs(sketchView));
 * </code></pre>
 *
 * <h3>Example 4: Using the CombineFn</h3>
 *
 * <p>The {@code CombineFn} does the same thing as the {@code PTransform}s but
 * it can be used for doing stateful processing or in
 * {@link org.apache.beam.sdk.transforms.CombineFns.ComposedCombineFn}.
 *
 * <p>This example is not really interesting but it shows how you can properly create
 * a {@link CountMinSketchFn}. One must always specify a coder using the {@link
 * CountMinSketchFn#create(Coder)} method.
 *
 * <pre><code>
 *  double eps = 0.0001;
 *  double conf = 0.9999;
 * {@literal PCollection<MyObject>} input = ...;
 * {@literal PCollection<CountMinSketch>} output = input.apply(Combine.globally(CountMinSketchFn
 * {@literal    .<MyObject>}create(new MyObjectCoder())
 *              .withAccuracy(eps, conf)));
 * </code></pre>
 *
 * <p><b>Warning: this class is experimental.</b> <br>
 * Its API is subject to change in future versions of Beam.
 */
@Experimental
public final class SketchFrequencies {

  /**
   * Create the {@link PTransform} that will build a Count-min sketch for keeping track
   * of the frequency of the elements in the whole stream.
   *
   * <p>It returns a {@code PCollection<{@link CountMinSketch}>}  that can be queried in order to
   * obtain estimations of the elements' frequencies.
   *
   * @param <InputT> the type of the elements in the input {@link PCollection}
   */
  public static <InputT> GlobalSketch<InputT> globally() {
    return GlobalSketch.<InputT>builder().build();
  }

  /**
   * Like {@link #globally()} but per key, i.e a Count-min sketch per key
   * in {@code  PCollection<KV<K, V>>} and returns a
   * {@code PCollection<KV<K, {@link CountMinSketch}>>}.
   *
   * @param <K> type of the keys mapping the elements
   * @param <V> type of the values being combined per key
   */
  public static <K, V> PerKeySketch<K, V> perKey() {
    return PerKeySketch.<K, V>builder().build();
  }

  /**
   * Implementation of {@link #globally()}.
   *
   * @param <InputT> the type of the elements in the input {@link PCollection}
   */
  @AutoValue
  public abstract static class GlobalSketch<InputT>
          extends PTransform<PCollection<InputT>, PCollection<Sketch<InputT>>> {

    abstract double relativeError();

    abstract double confidence();

    abstract Builder<InputT> toBuilder();

    static <InputT> Builder<InputT> builder() {
      return new AutoValue_SketchFrequencies_GlobalSketch.Builder<InputT>()
              .setRelativeError(0.01)
              .setConfidence(0.999);
    }

    @AutoValue.Builder
    abstract static class Builder<InputT> {
      abstract Builder<InputT> setRelativeError(double eps);

      abstract Builder<InputT> setConfidence(double conf);

      abstract GlobalSketch<InputT> build();
    }

    /**
     * Returns a new {@link PTransform} with a new relative error {@code epsilon}.
     *
     * <p>Keep in mind that the lower the {@code epsilon} value, the greater the width.
     *
     * @param eps the error relative to the total number of distinct elements
     */
    public GlobalSketch<InputT> withRelativeError(double eps) {
      return toBuilder().setRelativeError(eps).build();
    }

    /**
     * Returns a new {@link PTransform} with a new {@code confidence} value, i.e.
     * the probability that the relative error is lower or equal to {@code epsilon}.
     *
     * <p>Keep in mind that the greater the confidence, the greater the depth.
     *
     * @param conf the confidence in the result to not exceed the relative error
     */
    public GlobalSketch<InputT> withConfidence(double conf) {
      return toBuilder().setConfidence(conf).build();
    }

    @Override
    public PCollection<Sketch<InputT>> expand(PCollection<InputT> input) {
      return input.apply("Compute Count-Min Sketch",
                      Combine.<InputT, Sketch<InputT>>globally(CountMinSketchFn
                              .<InputT>create(input.getCoder())
                              .withAccuracy(relativeError(), confidence())));
    }
  }

  /**
   * Implementation of {@link #perKey()}.
   *
   * @param <K> type of the keys mapping the elements
   * @param <V> type of the values being combined per key
   */
  @AutoValue
  public abstract static class PerKeySketch<K, V>
          extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Sketch<V>>>> {

    abstract double relativeError();

    abstract double confidence();

    abstract Builder<K, V> toBuilder();

    static <K, V> Builder<K, V> builder() {
      return new AutoValue_SketchFrequencies_PerKeySketch.Builder<K, V>()
              .setRelativeError(0.01)
              .setConfidence(0.999);
    }

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setRelativeError(double eps);

      abstract Builder<K, V> setConfidence(double conf);

      abstract PerKeySketch<K, V> build();
    }

    /**
     * Returns a new {@link PTransform} with a new relative error {@code epsilon}.
     *
     * <p>Keep in mind that the lower the {@code epsilon} value, the greater the width.
     *
     * @param eps the error relative to the total number of distinct elements
     */
    public PerKeySketch<K, V> withRelativeError(double eps) {
      return toBuilder().setRelativeError(eps).build();
    }

    /**
     * Returns a new {@link PTransform} with a new {@code confidence} value, i.e.
     * the probability that the relative error is lower or equal to {@code epsilon}.
     *
     * <p>Keep in mind that the greater the confidence, the greater the depth.
     *
     * @param conf the confidence in the result to not exceed the relative error
     */
    public PerKeySketch<K, V> withConfidence(double conf) {
      return toBuilder().setConfidence(conf).build();
    }

    @Override
    public PCollection<KV<K, Sketch<V>>> expand(PCollection<KV<K, V>> input) {
      KvCoder<K, V> inputCoder = (KvCoder<K, V>) input.getCoder();
      return input.apply("Compute Count-Min Sketch perKey",
              Combine.<K, V, Sketch<V>>perKey(CountMinSketchFn
                      .<V>create(inputCoder.getValueCoder())
                      .withAccuracy(relativeError(), confidence())));
    }
  }

  /**
   * Implements the {@link CombineFn} of {@link SketchFrequencies} transforms.
   *
   * @param <InputT> the type of the elements in the input {@link PCollection}
   */
  public static class CountMinSketchFn<InputT>
          extends CombineFn<InputT, Sketch<InputT>, Sketch<InputT>> {

    private final Coder<InputT> inputCoder;
    private final int depth;
    private final int width;
    private final double epsilon;
    private final double confidence;

    private CountMinSketchFn(final Coder<InputT> coder, double eps, double confidence) {
      this.epsilon = eps;
      this.confidence = confidence;
      this.width = (int) Math.ceil(2 / eps);
      this.depth = (int) Math.ceil(-Math.log(1 - confidence) / Math.log(2));
      this.inputCoder = coder;
    }

    /**
     * Returns a {@link CountMinSketchFn} combiner with the given input coder. <br>
     * <b>Warning :</b> the coder must be deterministic.
     *
     * @param coder the coder that encodes the elements' type
     */
    public static <InputT> CountMinSketchFn<InputT> create(Coder<InputT> coder) {
      try {
        coder.verifyDeterministic();
      } catch (Coder.NonDeterministicException e) {
        throw new IllegalArgumentException("Coder must be deterministic to perform this sketch."
                + e.getMessage(), e);
      }
      return new CountMinSketchFn<>(coder, 0.01, 0.999);
    }

    /**
     * Returns a new {@link CountMinSketchFn} combiner with new precision accuracy parameters
     * {@code epsilon} and {@code confidence}.
     *
     * <p>Keep in mind that the lower the {@code epsilon} value, the greater the width,
     * and the greater the confidence, the greater the depth.
     *
     * @param epsilon the error relative to the total number of distinct elements
     * @param confidence the confidence in the result to not exceed the relative error
     */
    public CountMinSketchFn<InputT> withAccuracy(double epsilon, double confidence) {
      if (epsilon <= 0D) {
        throw new IllegalArgumentException("The relative error must be positive");
      }

      if (confidence <= 0D || confidence >= 1D) {
        throw new IllegalArgumentException("The confidence must be between 0 and 1");
      }
      return new CountMinSketchFn<>(inputCoder, epsilon, confidence);
    }

    @Override public Sketch<InputT> createAccumulator() {
      return Sketch.<InputT>create(epsilon, confidence);
    }

    @Override public Sketch<InputT> addInput(Sketch<InputT> accumulator, InputT element) {
      accumulator.add(element, inputCoder);

      return accumulator;
    }

    @Override public Sketch<InputT> mergeAccumulators(Iterable<Sketch<InputT>> accumulators) {
      Iterator<Sketch<InputT>> it = accumulators.iterator();
      Sketch<InputT> first = it.next();
      CountMinSketch mergedSketches = first.sketch();
      try {
        while (it.hasNext()) {
          mergedSketches = CountMinSketch.merge(mergedSketches, it.next().sketch());
        }
      } catch (FrequencyMergeException e) {
        // Should never happen because every instantiated accumulator are of the same type.
        throw new IllegalStateException("The accumulators cannot be merged:" + e.getMessage());
      }

      return Sketch.<InputT>create(mergedSketches);
    }

    /** Output the whole structure so it can be queried, reused or stored easily. */
    @Override public Sketch<InputT> extractOutput(Sketch<InputT> accumulator) {
      return accumulator;
    }


    @Override public Coder<Sketch<InputT>> getAccumulatorCoder(CoderRegistry registry,
                                                               Coder inputCoder) {
      return new CountMinSketchCoder<>();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
              .add(DisplayData.item("width", width)
                      .withLabel("width of the Count-Min sketch array"))
              .add(DisplayData.item("depth", depth)
                      .withLabel("depth of the Count-Min sketch array"))
              .add(DisplayData.item("eps", epsilon)
                      .withLabel("relative error to the total number of elements"))
              .add(DisplayData.item("conf", confidence)
                      .withLabel("confidence in the relative error"));
    }
  }

  /**
   * Wrap StreamLib's Count-Min Sketch to support counting all user types by hashing
   * the encoded user type using the supplied deterministic coder. This is required
   * since objects in Apache Beam are considered equal if their encodings are equal.
   */
  @AutoValue
  public abstract static class Sketch<T> implements Serializable {

    static final int SEED = 123456;

    static <T> Sketch<T> create(double eps, double conf) {
      int width = (int) Math.ceil(2 / eps);
      int depth = (int) Math.ceil(-Math.log(1 - conf) / Math.log(2));
      return new AutoValue_SketchFrequencies_Sketch<>(
              depth,
              width,
              new CountMinSketch(depth, width, SEED));
    }

    static <T> Sketch<T> create(CountMinSketch sketch) {
      int width = (int) Math.ceil(2 / sketch.getRelativeError());
      int depth = (int) Math.ceil(-Math.log(1 - sketch.getConfidence()) / Math.log(2));
      return new AutoValue_SketchFrequencies_Sketch<>(
              depth,
              width,
              sketch);
    }

    abstract int depth();
    abstract int width();
    abstract CountMinSketch sketch();

    public void add(T element, long count, Coder<T> coder) {
      sketch().add(hashElement(element, coder), count);
    }

    public void add(T element, Coder<T> coder) {
      add(element, 1L, coder);
    }

    private long hashElement(T element, Coder<T> coder) {
      try {
        byte[] elemBytes = CoderUtils.encodeToByteArray(coder, element);
        return Hashing.murmur3_128().hashBytes(elemBytes).asLong();
      } catch (CoderException e) {
        throw new IllegalStateException("The input value cannot be encoded: " + e.getMessage(), e);
      }
    }

    /**
     * Utility class to retrieve the estimate frequency of an element from a {@link
     * CountMinSketch}.
     */
    public long estimateCount(T element, Coder<T> coder) {
      return sketch().estimateCount(hashElement(element, coder));
    }

  }

  /**
   * Coder for {@link CountMinSketch} class.
   */
  static class CountMinSketchCoder<T> extends CustomCoder<Sketch<T>> {

    private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();

    @Override
    public void encode(Sketch<T> value, OutputStream outStream) throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null Count-min Sketch");
      }
      BYTE_ARRAY_CODER.encode(CountMinSketch.serialize(value.sketch()), outStream);
    }

    @Override
    public Sketch<T> decode(InputStream inStream) throws IOException {
      byte[] sketchBytes = BYTE_ARRAY_CODER.decode(inStream);
      CountMinSketch sketch = CountMinSketch.deserialize(sketchBytes);
      return Sketch.<T>create(sketch);
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(Sketch<T> value) {
      return true;
    }

    @Override
    protected long getEncodedElementByteSize(Sketch<T> value) throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null Count-min Sketch");
      } else {
        // 8L is for the sketch's size (long)
        // 4L * 4 is for depth and width (ints) in Sketch<T> and in the Count-Min sketch
        // 8L * depth * (width + 1) is a factorization for the sizes of table (long[depth][width])
        // and hashA (long[depth])
        return 8L + 4L * 4 + 8L * value.depth() * (value.width() + 1);
      }
    }
  }
}
