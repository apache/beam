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

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.auto.value.AutoValue;
import com.google.common.base.MoreObjects;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@link PTransform}s for computing the approximate number of distinct elements in a stream.
 *
 * <p>This class relies on the HyperLogLog algorithm, and more precisely HyperLogLog+,
 * the improved version of Google.
 *
 *  <h2>References</h2>
 *
 * <p>The implementation comes from Addthis' Stream-lib library :
 * <a>https://github.com/addthis/stream-lib</a>
 * <br>The original paper of the HyperLogLog is available here :
 * <a>http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf</a>
 * <br>A paper from the same authors to have a clearer view of the algorithm is available here :
 * <a>http://cscubs.cs.uni-bonn.de/2016/proceedings/paper-03.pdf</a>
 * <br>Google's HyperLogLog+ version is detailed in this paper :
 * <a>https://research.google.com/pubs/pub40671.html</a>
 *
 * <p>Three parameters can be tuned according to the processing context :
 * <ul>
 *    <li> Input Coder : The coder of the objects to combine.
 *    <li> Precision : Controls the accuracy of the estimation. In general one can expect a
 *    relative error of about {@code 1.1 / sqrt(2^p)}.
 *    <li> Sparse Precision : Used to create a sparse representation in order to optimize memory
 *    and improve accuracy at small cardinalities ({@code < 12000}).
 * </ul>
 *
 * <h2>Using the Transforms</h2>
 *
 * <p>By default the Input Coder is inferred at runtime. One should specify a coder only if
 * custom type is used.
 * <br>By default the precision is set to {@code 12} for a relative error of around {@code 2%}.
 * <br>By default the sparse representation is not used. One should use it if the number of
 * distinct elements in the stream may be less than {@code 12000}.
 *
 * <h4>Example 1 : globally default use</h4>
 * <pre>{@code
 * PCollection<Integer> input = ...;
 * PCollection<HyperLogLogPlus> hllSketch = input.apply(ApproximateDistinct.<Integer>globally());
 * }</pre>
 *
 * <h4>Example 2 : per key default use</h4>
 * <pre>{@code
 * PCollection<Integer, String> input = ...;
 * PCollection<Integer, HyperLogLogPlus> hllSketches = input.apply(ApproximateDistinct
 *                .<Integer, String>perKey());
 * }</pre>
 *
 * <h4>Example 3 : tune precision and use sparse representation</h4>
 * <pre>{@code
 * int precision = 15;
 * int sparsePrecision = 25;
 * PCollection<Double> input = ...;
 * PCollection<HyperLogLogPlus> hllSketch = input.apply(ApproximateDistinct
 *                .<Double>globally()
 *                .withPrecision(precision)
 *                .withSparsePrecision(sparsePrecision));
 * }</pre>
 *
 * <h4>Example 4 : use a custom object and tune precision</h4>
 * <pre>{@code
 * int precision = 18;
 * PCollection<MyClass> input = ...;
 * PCollection<HyperLogLogPlus> hllSketch = input.apply(ApproximateDistinct.<MyClass>globally()
 *                  .withInputCoder(MyClassCoder.of())
 *                  .withPrecision(precision));
 * }</pre>
 *
 * <p>The tuning works exactly the same with {@link #perKey()}.
 *
 * <h2>Using the CombineFn</h2>
 *
 * <p>The {@link CombineFn} is also exposed directly so it can be composed or used as a state cell
 * in a stateful {@link org.apache.beam.sdk.transforms.ParDo}. See {@link ApproximateDistinctFn}.
 *
 * <p>By default, one must always specify a coder, using the
 * {@link ApproximateDistinctFn#create(Coder)} method.
 * <br>By default, the precision is set to {@code 12} for a relative error of around {@code 2%}.
 * <br>By default, the sparse representation is not used. One should use it if the cardinality
 * may be less than {@code 12000}.
 *
 * <h4>Example 1 : default use</h4>
 * <pre>{@code
 * PCollection<Integer> input = ...;
 * PCollection<HyperLogLogPlus> output = input.apply(Combine.globally(ApproximateDistinctFn
 *                .<Integer>create(BigEndianIntegerCoder.of()));
 * }</pre>
 *
 * <h4>Example 2 : tune the parameters</h4>
 *
 * <p>You can tune the precision and sparse precision by successively calling
 * {@link ApproximateDistinctFn#withPrecision(int)} and
 * {@link ApproximateDistinctFn#withSparseRepresentation(int)} methods.
 *
 * <pre>{@code
 * PCollection<Object> input = ...;
 * PCollection<HyperLogLogPlus> output = input.apply(Combine.globally(ApproximateDistinctFn
 *                .<Object>create(new ObjectCoder())
 *                .withPrecision(18)
 *                .withSparseRepresentation(24)));
 * }</pre>
 *
 * <h4>Example 3 : using the {@link CombineFn} in a stateful
 * {@link org.apache.beam.sdk.transforms.ParDo}</h4>
 *
 * <p>One may want to use the {@link ApproximateDistinctFn} in a stateful ParDo in order to
 * make some processing depending on the current cardinality of the stream.
 * <br>For more information about stateful processing see :
 * <a>https://beam.apache.org/blog/2017/02/13/stateful-processing.html</a>
 *
 * <p>Here is an example of {@link org.apache.beam.sdk.transforms.DoFn} using an
 * {@link ApproximateDistinctFn} as a {@link org.apache.beam.sdk.state.CombiningState} :
 *
 * <pre>{@code
 * class StatefulCardinality<V> extends DoFn<V>, OutputT>> {
 *   {@literal @}StateId("hyperloglog")
 *   private final StateSpec<CombiningState<V, HyperLogLogPlus, HyperLogLogPlus>> indexSpec;
 *
 *   public StatefulCardinality(ApproximateDistinctFn<V> fn) {
 *     indexSpec = StateSpecs.combining(fn);
 *   }
 *
 *   {@literal @}ProcessElement
 *   public void processElement(
 *      ProcessContext context,
 *      {@literal @}StateId("hllSketch")
 *      CombiningState<V, HyperLogLogPlus, HyperLogLogPlus> hllSketch) {
 *     long current = MoreObjects.firstNonNull(hllSketch.getAccum().cardinality(), 0L);
 *     hllSketch.add(context.element());
 *     context.output(...);
 *     }
 * }
 * }</pre>
 *
 * <p>Then the {@link org.apache.beam.sdk.transforms.DoFn} can be called like this :
 * <pre>{@code
 * PCollection<Object> input = ...;
 * ApproximateDistinctFn myFn = ApproximateDistinctFn.create(new ObjectCoder());
 * PCollection<OutputT> = input.apply(ParDo.of(new StatefulCardinality(myFn)));
 * }</pre>
 *
 */
public final class ApproximateDistinct {

  /**
   * The {@link PTransform} takes an input {@link PCollection} of objects and returns a
   * {@link PCollection} whose contents is a sketch that can be queried in order to retrieve
   * the approximate number of distinct elements in the input {@link PCollection}.
   *
   * @param <InputT>    the type of the elements in the input {@link PCollection}
   */
  public static <InputT> GloballyDistinct<InputT> globally() {
    return GloballyDistinct.<InputT>builder()
            .build();
  }

  /**
   * A {@link PTransform} that takes an input {@code PCollection<KV<K, V>}
   * and returns a {@code PCollection<KV<K, HyperLogLogPlus>>} that contains an
   * output element mapping each distinct key in the input {@link PCollection}
   * to a structure wrapping a {@link HyperLogLogPlus}. It can be queried in
   * order to retrieve the approximate number of distinct values associated with
   * each key in the input {@link PCollection}.
   *
   * @param <K>         type of the keys mapping the elements
   * @param <V>         type of the values being combined per key
   */
  public static <K, V> PerKeyDistinct<K, V> perKey() {
    return PerKeyDistinct.<K, V>builder()
            .build();
  }

  /**
   * Implementation of {@link #globally()}.
   *
   * @param <InputT>    the type of the elements in the input {@link PCollection}
   */
  @AutoValue
  public abstract static class GloballyDistinct<InputT>
          extends PTransform<PCollection<InputT>, PCollection<HyperLogLogPlus>> {

    @Nullable abstract Coder<InputT> inputCoder();
    abstract int precision();
    abstract int sparsePrecision();
    abstract Builder<InputT> toBuilder();

    static <InputT> Builder<InputT> builder() {
      return new AutoValue_ApproximateDistinct_GloballyDistinct.Builder<InputT>()
              .setInputCoder(null)
              .setPrecision(12)
              .setSparsePrecision(0);
    }

    @AutoValue.Builder
    abstract static class Builder<InputT> {
      abstract Builder<InputT> setInputCoder(Coder<InputT> coder);
      abstract Builder<InputT> setPrecision(int p);
      abstract Builder<InputT> setSparsePrecision(int sp);
      abstract GloballyDistinct<InputT> build();
    }


    public GloballyDistinct<InputT> withInputCoder(Coder<InputT> coder) {
      return toBuilder().setInputCoder(coder).build();
    }

    public GloballyDistinct<InputT> withPrecision(int p) {
      return toBuilder().setPrecision(p).build();
    }

    public GloballyDistinct<InputT> withSparsePrecision(int sp) {
      return toBuilder().setSparsePrecision(sp).build();
    }

    @Override
    public PCollection<HyperLogLogPlus> expand(PCollection<InputT> input) {
      return input.apply(Combine.globally(ApproximateDistinctFn
              .<InputT>create(MoreObjects.firstNonNull(this.inputCoder(), input.getCoder()))
              .withPrecision(this.precision())
              .withSparseRepresentation(this.sparsePrecision())));
    }
  }

  /**
   * Implementation of {@link #perKey()}.
   *
   * @param <K>       type of the keys mapping the elements
   * @param <V>       type of the values being combined per key
   */
  @AutoValue
  public abstract static class PerKeyDistinct<K, V>
          extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, HyperLogLogPlus>>> {

    @Nullable abstract Coder<V> inputCoder();
    abstract int precision();
    abstract int sparsePrecision();
    abstract Builder<K, V> toBuilder();

    static <K, V> Builder<K, V> builder() {
      return new AutoValue_ApproximateDistinct_PerKeyDistinct.Builder<K, V>()
              .setInputCoder(null)
              .setPrecision(12)
              .setSparsePrecision(0);
    }

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setInputCoder(Coder<V> coder);
      abstract Builder<K, V> setPrecision(int p);
      abstract Builder<K, V> setSparsePrecision(int sp);
      abstract PerKeyDistinct<K, V> build();
    }

    public PerKeyDistinct<K, V> withInputCoder(Coder<V> coder) {
      return toBuilder().setInputCoder(coder).build();
    }

    public PerKeyDistinct<K, V> withPrecision(int p) {
      return toBuilder().setPrecision(p).build();
    }

    public PerKeyDistinct<K, V> withSparsePrecision(int sp) {
      return toBuilder().setSparsePrecision(sp).build();
    }

    @Override
    public PCollection<KV<K, HyperLogLogPlus>> expand(PCollection<KV<K, V>> input) {
      KvCoder<K, V> inputCoder = (KvCoder<K, V>) input.getCoder();
      return input.apply(Combine.<K, V, HyperLogLogPlus>perKey(ApproximateDistinctFn
              .<V>create(MoreObjects.firstNonNull(this.inputCoder(), inputCoder.getValueCoder()))
              .withPrecision(this.precision())
              .withSparseRepresentation(this.sparsePrecision())));
    }
  }

  /**
   * Implements the {@link CombineFn} of {@link ApproximateDistinct} transforms.
   *
   * @param <InputT>      the type of the elements in the input {@link PCollection}
   */
  public static class ApproximateDistinctFn<InputT>
      extends CombineFn<InputT, HyperLogLogPlus, HyperLogLogPlus> {

    private final int p;

    private final int sp;

    private final Coder<InputT> inputCoder;

    private ApproximateDistinctFn(int p, int sp, Coder<InputT> coder) {
      this.p = p;
      this.sp = sp;
      inputCoder = coder;
    }

    /**
     * Returns an {@link ApproximateDistinctFn} combiner with the given input coder.
     *
     * @param <InputT>    the type of the elements in the input {@link PCollection}
     * @param coder       the coder that encodes the elements' type
     */
    public static <InputT> ApproximateDistinctFn<InputT> create(Coder<InputT> coder) {
      try {
        coder.verifyDeterministic();
      } catch (Coder.NonDeterministicException e) {
        throw new IllegalArgumentException("Coder is not deterministic ! " + e.getMessage(), e);
      }
      return new ApproximateDistinctFn<>(12, 0, coder);
    }

    /**
     * Returns a new {@link ApproximateDistinctFn} combiner with a new precision {@code p}.
     *
     * <p>The precision value will have an impact on the number of buckets used to store information
     * about the distinct elements.
     * <br>In general, you can expect a relative error of about :
     * <pre>{@code 1.1 / sqrt(2^p)}</pre>
     * For instance, the estimation {@code ApproximateDistinct.globally(12)}
     * will have a relative error of about 2%.
     * <br>Also keep in mind that {@code p} cannot be lower than 4, because the estimation
     * would be too inaccurate.
     *
     * <p>See {@link ApproximateDistinct#precisionForRelativeError(double)} and
     * {@link ApproximateDistinct#relativeErrorForPrecision(int)} to have more information about
     * the relationship between precision and relative error.
     *
     * @param p           the precision value for the normal representation
     */
    public ApproximateDistinctFn<InputT> withPrecision(int p) {
      checkArgument(p >= 4, "Expected : p >= 4. Actual : p = %s", p);
      return new ApproximateDistinctFn<>(p, this.sp, this.inputCoder);
    }

    /**
     * Returns a new {@link ApproximateDistinctFn} combiner with a sparse representation
     * of precision {@code sp}.
     *
     * <p>The sparse representation is used to optimize memory and improve accuracy at small
     * cardinalities. Thus the value of {@code sp} should be greater than {@code p}.
     * <br>Moreover, values above 32 are not yet supported by the AddThis version of HyperLogLog+.
     *
     * <p>Fore more information about the sparse representation, read Google's paper at
     * <a>https://research.google.com/pubs/pub40671.html</a>
     *
     * @param sp          the precision of HyperLogLog+' sparse representation
     */
    public ApproximateDistinctFn<InputT> withSparseRepresentation(int sp) {
      checkArgument((sp > this.p && sp < 32) || (sp == 0), "Expected : p <= sp <= 32."
              + "Actual : p = %s, sp = %s", this.p, sp);
      return new ApproximateDistinctFn<>(this.p, sp, this.inputCoder);
    }

    @Override
    public HyperLogLogPlus createAccumulator() {
      return new HyperLogLogPlus(p, sp);
    }

    @Override
    public HyperLogLogPlus addInput(HyperLogLogPlus acc, InputT record) {
      try {
        acc.offer(CoderUtils.encodeToByteArray(inputCoder, record));
      } catch (CoderException e) {
        throw new IllegalStateException(
                "The input value cannot be encoded : " + e.getMessage(), e);
      }
      return acc;
    }

    /**
     * Output the whole structure so it can be queried, reused or stored easily.
     */
    @Override
    public HyperLogLogPlus extractOutput(HyperLogLogPlus accumulator) {
      return accumulator;
    }

    @Override
    public HyperLogLogPlus mergeAccumulators(Iterable<HyperLogLogPlus> accumulators) {
      HyperLogLogPlus mergedAccum = createAccumulator();
      for (HyperLogLogPlus accum : accumulators) {
        try {
          mergedAccum.addAll(accum);
        } catch (CardinalityMergeException e) {
          // Should never happen because only HyperLogLogPlus accumulators are instantiated.
          throw new IllegalStateException("The accumulators cannot be merged : "
                  + e.getMessage(), e);
        }
      }
      return mergedAccum;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
              .add(DisplayData.item("p", p)
                      .withLabel("precision"))
              .add(DisplayData.item("sp", sp)
                      .withLabel("sparse representation precision"));
      }
  }

  /**
   * Coder for {@link HyperLogLogPlus} class.
   */
  public static class HyperLogLogPlusCoder extends CustomCoder<HyperLogLogPlus> {

    private static final HyperLogLogPlusCoder INSTANCE = new HyperLogLogPlusCoder();

    private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();

    public static HyperLogLogPlusCoder of() {
      return INSTANCE;
    }

    @Override public void encode(HyperLogLogPlus value, OutputStream outStream)
            throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null HyperLogLogPlus sketch");
      }
      BYTE_ARRAY_CODER.encode(value.getBytes(), outStream);
    }

    @Override public HyperLogLogPlus decode(InputStream inStream) throws IOException {
      return HyperLogLogPlus.Builder.build(BYTE_ARRAY_CODER.decode(inStream));
    }

    @Override public boolean isRegisterByteSizeObserverCheap(HyperLogLogPlus value) {
      return true;
    }

    @Override protected long getEncodedElementByteSize(HyperLogLogPlus value) throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null HyperLogLogPlus sketch");
      }
      return value.sizeof();
    }
  }

  /**
   * Computes the precision based on the desired relative error.
   *
   * <p>According to the paper, the mean squared error is bounded by the following formula :
   * <pre>b(m) / sqrt(m)
   * Where m is the number of buckets used ({@code p = log2(m)})
   * and {@code b(m) < 1.106} for {@code m > 16 (and p > 4)}.
   * </pre>
   *
   * <br><b>WARNING:</b>
   * <br>This does not mean relative error in the estimation <b>can't</b> be higher.
   * <br>This only means that on average the relative error will be
   * lower than the desired relative error.
   * <br>Nevertheless, the more elements arrive in the {@link PCollection}, the lower
   * the variation will be.
   * <br>Indeed, this is like when you throw a dice millions of time : the relative frequency of
   * each different result <code>{1,2,3,4,5,6}</code> will get closer to {@code 1/6}.
   *
   * @param relativeError   the mean squared error should be in the interval ]0,1]
   * @return  the minimum precision p in order to have the desired relative error on average.
   */
  static long precisionForRelativeError(double relativeError) {
    return Math.round(Math.ceil(Math.log(
            Math.pow(1.106, 2.0)
                    / Math.pow(relativeError, 2.0))
            / Math.log(2)));
  }

  /**
   * @param p              the precision i.e. the number of bits used for indexing the buckets
   * @return  the Mean squared error of the Estimation of cardinality to expect
   * for the given value of p.
   */
  static double relativeErrorForPrecision(int p) {
    if (p < 4) {
      return 1.0;
    }
    double betaM;
    switch(p) {
      case 4 : betaM = 1.156;
        break;
      case 5 : betaM = 1.2;
        break;
      case 6 : betaM = 1.104;
        break;
      case 7 : betaM = 1.096;
        break;
      default : betaM = 1.05;
        break;
    }
    return betaM / Math.sqrt(Math.exp(p * Math.log(2)));
  }
}
