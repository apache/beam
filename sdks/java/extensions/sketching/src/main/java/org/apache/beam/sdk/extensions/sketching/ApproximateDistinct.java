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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.KvCoder;
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
 * {@link PTransform}s for computing the approximate number of distinct elements in a stream.
 *
 * <p>This class relies on the HyperLogLog algorithm, and more precisely HyperLogLog+, the improved
 * version of Google.
 *
 * <h2>References</h2>
 *
 * <p>The implementation comes from <a href="https://github.com/addthis/stream-lib">Addthis'
 * Stream-lib library</a>. <br>
 * The original paper of the HyperLogLog is available <a
 * href="http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf">here</a>. <br>
 * A paper from the same authors to have a clearer view of the algorithm is available <a
 * href="http://cscubs.cs.uni-bonn.de/2016/proceedings/paper-03.pdf">here</a>. <br>
 * Google's HyperLogLog+ version is detailed in <a
 * href="https://research.google.com/pubs/pub40671.html">this paper</a>.
 *
 * <h2>Parameters</h2>
 *
 * <p>Two parameters can be tuned in order to control the computation's accuracy:
 *
 * <ul>
 *   <li><b>Precision: {@code p}</b> <br>
 *       Controls the accuracy of the estimation. The precision value will have an impact on the
 *       number of buckets used to store information about the distinct elements. <br>
 *       In general one can expect a relative error of about {@code 1.1 / sqrt(2^p)}. The value
 *       should be of at least 4 to guarantee a minimal accuracy. <br>
 *       By default, the precision is set to {@code 12} for a relative error of around {@code 2%}.
 *   <li><b>Sparse Precision: {@code sp}</b> <br>
 *       Used to create a sparse representation in order to optimize memory and improve accuracy at
 *       small cardinalities. <br>
 *       The value of {@code sp} should be greater than {@code p}, but lower than 32. <br>
 *       By default, the sparse representation is not used ({@code sp = 0}). One should use it if
 *       the cardinality may be less than {@code 12000}.
 * </ul>
 *
 * <h2>Examples</h2>
 *
 * <p>There are 2 ways of using this class:
 *
 * <ul>
 *   <li>Use the {@link PTransform}s that return {@code PCollection<Long>} corresponding to the
 *       estimate number of distinct elements in the input {@link PCollection} of objects or for
 *       each key in a {@link PCollection} of {@link KV}s.
 *   <li>Use the {@link ApproximateDistinctFn} {@code CombineFn} that is exposed in order to make
 *       advanced processing involving the {@link HyperLogLogPlus} structure which resumes the
 *       stream.
 * </ul>
 *
 * <h3>Using the Transforms</h3>
 *
 * <h4>Example 1: globally default use</h4>
 *
 * <pre>{@code
 * PCollection<Integer> input = ...;
 * PCollection<Long> hllSketch = input.apply(ApproximateDistinct.<Integer>globally());
 * }</pre>
 *
 * <h4>Example 2: per key default use</h4>
 *
 * <pre>{@code
 * PCollection<Integer, String> input = ...;
 * PCollection<Integer, Long> hllSketches = input.apply(ApproximateDistinct
 *                .<Integer, String>perKey());
 * }</pre>
 *
 * <h4>Example 3: tune precision and use sparse representation</h4>
 *
 * <p>One can tune the precision and sparse precision parameters in order to control the accuracy
 * and the memory. The tuning works exactly the same for {@link #globally()} and {@link #perKey()}.
 *
 * <pre>{@code
 * int precision = 15;
 * int sparsePrecision = 25;
 * PCollection<Double> input = ...;
 * PCollection<Long> hllSketch = input.apply(ApproximateDistinct
 *                .<Double>globally()
 *                .withPrecision(precision)
 *                .withSparsePrecision(sparsePrecision));
 * }</pre>
 *
 * <h3>Using the {@link ApproximateDistinctFn} CombineFn</h3>
 *
 * <p>The CombineFn does the same thing as the transform but it can be used in cases where you want
 * to manipulate the {@link HyperLogLogPlus} sketch, for example if you want to store it in a
 * database to have a backup. It can also be used in stateful processing or in {@link
 * org.apache.beam.sdk.transforms.CombineFns.ComposedCombineFn}.
 *
 * <h4>Example 1: basic use</h4>
 *
 * <p>This example is not really interesting but show how you can properly create an {@link
 * ApproximateDistinctFn}. One must always specify a coder using the {@link
 * ApproximateDistinctFn#create(Coder)} method.
 *
 * <pre>{@code
 * PCollection<Integer> input = ...;
 * PCollection<HyperLogLogPlus> output = input.apply(Combine.globally(ApproximateDistinctFn
 *                 .<Integer>create(BigEndianIntegerCoder.of()));
 * }</pre>
 *
 * <h4>Example 2: use the {@link CombineFn} in a stateful {@link ParDo}</h4>
 *
 * <p>One may want to use the {@link ApproximateDistinctFn} in a stateful ParDo in order to make
 * some processing depending on the current cardinality of the stream. <br>
 * For more information about stateful processing see the blog spot on this topic <a
 * href="https://beam.apache.org/blog/2017/02/13/stateful-processing.html">here</a>.
 *
 * <p>Here is an example of {@link DoFn} using an {@link ApproximateDistinctFn} as a {@link
 * org.apache.beam.sdk.state.CombiningState}:
 *
 * <pre><code>
 * {@literal class StatefulCardinality<V> extends DoFn<V, OutputT>} {
 *   {@literal @StateId}("hyperloglog")
 *   {@literal private final StateSpec<CombiningState<V, HyperLogLogPlus, HyperLogLogPlus>>}
 *      indexSpec;
 *
 *   {@literal public StatefulCardinality(ApproximateDistinctFn<V> fn)} {
 *     indexSpec = StateSpecs.combining(fn);
 *   }
 *
 *  {@literal @ProcessElement}
 *   public void processElement(
 *      ProcessContext context,
 *      {@literal @StateId}("hllSketch")
 *      {@literal CombiningState<V, HyperLogLogPlus, HyperLogLogPlus> hllSketch)} {
 *     long current = MoreObjects.firstNonNull(hllSketch.getAccum().cardinality(), 0L);
 *     hllSketch.add(context.element());
 *     context.output(...);
 *   }
 * }
 * </code></pre>
 *
 * <p>Then the {@link DoFn} can be called like this:
 *
 * <pre>{@code
 * PCollection<V> input = ...;
 * ApproximateDistinctFn<V> myFn = ApproximateDistinctFn.create(input.getCoder());
 * PCollection<V> = input.apply(ParDo.of(new StatefulCardinality<>(myFn)));
 * }</pre>
 *
 * <h4>Example 3: use the {@link RetrieveCardinality} utility class</h4>
 *
 * <p>One may want to retrieve the cardinality as a long after making some advanced processing using
 * the {@link HyperLogLogPlus} structure. <br>
 * The {@link RetrieveCardinality} utility class provides an easy way to do so:
 *
 * <pre>{@code
 * PCollection<MyObject> input = ...;
 * PCollection<HyperLogLogPlus> hll = input.apply(Combine.globally(ApproximateDistinctFn
 *                  .<MyObject>create(new MyObjectCoder())
 *                  .withSparseRepresentation(20)));
 *
 *  // Some advanced processing
 *  PCollection<SomeObject> advancedResult = hll.apply(...);
 *
 *  PCollection<Long> cardinality = hll.apply(ApproximateDistinct.RetrieveCardinality.globally());
 *
 * }</pre>
 *
 * <p><b>Warning: this class is experimental.</b> Its API is subject to change in future versions of
 * Beam. For example, it may be merged with the {@link
 * org.apache.beam.sdk.transforms.ApproximateUnique} transform.
 */
@Experimental
public final class ApproximateDistinct {

  /**
   * Computes the approximate number of distinct elements in the input {@code PCollection<InputT>}
   * and returns a {@code PCollection<Long>}.
   *
   * @param <InputT> the type of the elements in the input {@link PCollection}
   */
  public static <InputT> GloballyDistinct<InputT> globally() {
    return GloballyDistinct.<InputT>builder().build();
  }

  /**
   * Like {@link #globally} but per key, i.e computes the approximate number of distinct values per
   * key in a {@code PCollection<KV<K, V>>} and returns {@code PCollection<KV<K, Long>>}.
   *
   * @param <K> type of the keys mapping the elements
   * @param <V> type of the values being combined per key
   */
  public static <K, V> PerKeyDistinct<K, V> perKey() {
    return PerKeyDistinct.<K, V>builder().build();
  }

  /**
   * Implementation of {@link #globally()}.
   *
   * @param <InputT> the type of the elements in the input {@link PCollection}
   */
  @AutoValue
  public abstract static class GloballyDistinct<InputT>
      extends PTransform<PCollection<InputT>, PCollection<Long>> {

    abstract int precision();

    abstract int sparsePrecision();

    abstract Builder<InputT> toBuilder();

    static <InputT> Builder<InputT> builder() {
      return new AutoValue_ApproximateDistinct_GloballyDistinct.Builder<InputT>()
          .setPrecision(12)
          .setSparsePrecision(0);
    }

    @AutoValue.Builder
    abstract static class Builder<InputT> {
      abstract Builder<InputT> setPrecision(int p);

      abstract Builder<InputT> setSparsePrecision(int sp);

      abstract GloballyDistinct<InputT> build();
    }

    /**
     * Returns a new {@link PTransform} with a new precision {@code p}.
     *
     * <p>Keep in mind that {@code p} cannot be lower than 4, because the estimation would be too
     * inaccurate.
     *
     * <p>See {@link ApproximateDistinct#precisionForRelativeError(double)} and {@link
     * ApproximateDistinct#relativeErrorForPrecision(int)} to have more information about the
     * relationship between precision and relative error.
     *
     * @param p the precision value for the normal representation
     */
    public GloballyDistinct<InputT> withPrecision(int p) {
      return toBuilder().setPrecision(p).build();
    }

    /**
     * Returns a new {@link PTransform} with a sparse representation of precision {@code sp}.
     *
     * <p>Values above 32 are not yet supported by the AddThis version of HyperLogLog+.
     *
     * <p>Fore more information about the sparse representation, read Google's paper available <a
     * href="https://research.google.com/pubs/pub40671.html">here</a>.
     *
     * @param sp the precision of HyperLogLog+' sparse representation
     */
    public GloballyDistinct<InputT> withSparsePrecision(int sp) {
      return toBuilder().setSparsePrecision(sp).build();
    }

    @Override
    public PCollection<Long> expand(PCollection<InputT> input) {
      return input
          .apply(
              "Compute HyperLogLog Structure",
              Combine.globally(
                  ApproximateDistinctFn.create(input.getCoder())
                      .withPrecision(this.precision())
                      .withSparseRepresentation(this.sparsePrecision())))
          .apply("Retrieve Cardinality", ParDo.of(RetrieveCardinality.globally()));
    }
  }

  /**
   * Implementation of {@link #perKey()}.
   *
   * @param <K> type of the keys mapping the elements
   * @param <V> type of the values being combined per key
   */
  @AutoValue
  public abstract static class PerKeyDistinct<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Long>>> {

    abstract int precision();

    abstract int sparsePrecision();

    abstract Builder<K, V> toBuilder();

    static <K, V> Builder<K, V> builder() {
      return new AutoValue_ApproximateDistinct_PerKeyDistinct.Builder<K, V>()
          .setPrecision(12)
          .setSparsePrecision(0);
    }

    @AutoValue.Builder
    abstract static class Builder<K, V> {
      abstract Builder<K, V> setPrecision(int p);

      abstract Builder<K, V> setSparsePrecision(int sp);

      abstract PerKeyDistinct<K, V> build();
    }

    /**
     * Returns a new {@link PTransform} with a new precision {@code p}.
     *
     * <p>Keep in mind that {@code p} cannot be lower than 4, because the estimation would be too
     * inaccurate.
     *
     * <p>See {@link ApproximateDistinct#precisionForRelativeError(double)} and {@link
     * ApproximateDistinct#relativeErrorForPrecision(int)} to have more information about the
     * relationship between precision and relative error.
     *
     * @param p the precision value for the normal representation
     */
    public PerKeyDistinct<K, V> withPrecision(int p) {
      return toBuilder().setPrecision(p).build();
    }

    /**
     * Returns a new {@link PTransform} with a sparse representation of precision {@code sp}.
     *
     * <p>Values above 32 are not yet supported by the AddThis version of HyperLogLog+.
     *
     * <p>Fore more information about the sparse representation, read Google's paper available <a
     * href="https://research.google.com/pubs/pub40671.html">here</a>.
     *
     * @param sp the precision of HyperLogLog+' sparse representation
     */
    public PerKeyDistinct<K, V> withSparsePrecision(int sp) {
      return toBuilder().setSparsePrecision(sp).build();
    }

    @Override
    public PCollection<KV<K, Long>> expand(PCollection<KV<K, V>> input) {
      KvCoder<K, V> inputCoder = (KvCoder<K, V>) input.getCoder();
      return input
          .apply(
              Combine.perKey(
                  ApproximateDistinctFn.create(inputCoder.getValueCoder())
                      .withPrecision(this.precision())
                      .withSparseRepresentation(this.sparsePrecision())))
          .apply("Retrieve Cardinality", ParDo.of(RetrieveCardinality.perKey()));
    }
  }

  /**
   * Implements the {@link CombineFn} of {@link ApproximateDistinct} transforms.
   *
   * @param <InputT> the type of the elements in the input {@link PCollection}
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
     * @param coder the coder that encodes the elements' type
     */
    public static <InputT> ApproximateDistinctFn<InputT> create(Coder<InputT> coder) {
      try {
        coder.verifyDeterministic();
      } catch (Coder.NonDeterministicException e) {
        throw new IllegalArgumentException("Coder must be deterministic to perform this sketch."
                + e.getMessage(), e);
      }
      return new ApproximateDistinctFn<>(12, 0, coder);
    }

    /**
     * Returns a new {@link ApproximateDistinctFn} combiner with a new precision {@code p}.
     *
     * <p>Keep in mind that {@code p} cannot be lower than 4, because the estimation would be too
     * inaccurate.
     *
     * <p>See {@link ApproximateDistinct#precisionForRelativeError(double)} and {@link
     * ApproximateDistinct#relativeErrorForPrecision(int)} to have more information about the
     * relationship between precision and relative error.
     *
     * @param p the precision value for the normal representation
     */
    public ApproximateDistinctFn<InputT> withPrecision(int p) {
      checkArgument(p >= 4, "Expected: p >= 4. Actual: p = %s", p);
      return new ApproximateDistinctFn<>(p, this.sp, this.inputCoder);
    }

    /**
     * Returns a new {@link ApproximateDistinctFn} combiner with a sparse representation of
     * precision {@code sp}.
     *
     * <p>Values above 32 are not yet supported by the AddThis version of HyperLogLog+.
     *
     * <p>Fore more information about the sparse representation, read Google's paper available <a
     * href="https://research.google.com/pubs/pub40671.html">here</a>.
     *
     * @param sp the precision of HyperLogLog+' sparse representation
     */
    public ApproximateDistinctFn<InputT> withSparseRepresentation(int sp) {
      checkArgument(
          (sp > this.p && sp < 32) || (sp == 0),
          "Expected: p <= sp <= 32." + "Actual: p = %s, sp = %s",
          this.p,
          sp);
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
        throw new IllegalStateException("The input value cannot be encoded: " + e.getMessage(), e);
      }
      return acc;
    }

    /** Output the whole structure so it can be queried, reused or stored easily. */
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
          throw new IllegalStateException(
              "The accumulators cannot be merged: " + e.getMessage(), e);
        }
      }
      return mergedAccum;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(DisplayData.item("p", p).withLabel("precision"))
          .add(DisplayData.item("sp", sp).withLabel("sparse representation precision"));
    }
  }

  /** Coder for {@link HyperLogLogPlus} class. */
  public static class HyperLogLogPlusCoder extends CustomCoder<HyperLogLogPlus> {

    private static final HyperLogLogPlusCoder INSTANCE = new HyperLogLogPlusCoder();

    private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();

    public static HyperLogLogPlusCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(HyperLogLogPlus value, OutputStream outStream) throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null HyperLogLogPlus sketch");
      }
      BYTE_ARRAY_CODER.encode(value.getBytes(), outStream);
    }

    @Override
    public HyperLogLogPlus decode(InputStream inStream) throws IOException {
      return HyperLogLogPlus.Builder.build(BYTE_ARRAY_CODER.decode(inStream));
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(HyperLogLogPlus value) {
      return true;
    }

    @Override
    protected long getEncodedElementByteSize(HyperLogLogPlus value) throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null HyperLogLogPlus sketch");
      }
      return value.sizeof();
    }
  }

  /**
   * Utility class that provides {@link DoFn}s to retrieve the cardinality from a {@link
   * HyperLogLogPlus} structure in a global or perKey context.
   */
  public static class RetrieveCardinality {

    public static <K> DoFn<KV<K, HyperLogLogPlus>, KV<K, Long>> perKey() {
      return new DoFn<KV<K, HyperLogLogPlus>, KV<K, Long>>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          KV<K, HyperLogLogPlus> kv = c.element();
          c.output(KV.of(kv.getKey(), kv.getValue().cardinality()));
        }
      };
    }

    public static DoFn<HyperLogLogPlus, Long> globally() {
      return new DoFn<HyperLogLogPlus, Long>() {
        @ProcessElement
        public void apply(ProcessContext c) {
          c.output(c.element().cardinality());
        }
      };
    }
  }

  /**
   * Computes the precision based on the desired relative error.
   *
   * <p>According to the paper, the mean squared error is bounded by the following formula:
   *
   * <pre>b(m) / sqrt(m)
   * Where m is the number of buckets used ({@code p = log2(m)})
   * and {@code b(m) < 1.106} for {@code m > 16 (and p > 4)}.
   * </pre>
   *
   * <br>
   * <b>WARNING:</b> <br>
   * This does not mean relative error in the estimation <b>can't</b> be higher. <br>
   * This only means that on average the relative error will be lower than the desired relative
   * error. <br>
   * Nevertheless, the more elements arrive in the {@link PCollection}, the lower the variation will
   * be. <br>
   * Indeed, this is like when you throw a dice millions of time: the relative frequency of each
   * different result <code>{1,2,3,4,5,6}</code> will get closer to {@code 1/6}.
   *
   * @param relativeError the mean squared error should be in the interval ]0,1]
   * @return the minimum precision p in order to have the desired relative error on average.
   */
  public static long precisionForRelativeError(double relativeError) {
    return Math.round(
        Math.ceil(Math.log(Math.pow(1.106, 2.0) / Math.pow(relativeError, 2.0)) / Math.log(2)));
  }

  /**
   * @param p the precision i.e. the number of bits used for indexing the buckets
   * @return the Mean squared error of the Estimation of cardinality to expect for the given value
   *     of p.
   */
  public static double relativeErrorForPrecision(int p) {
    if (p < 4) {
      return 1.0;
    }
    double betaM;
    switch (p) {
      case 4:
        betaM = 1.156;
        break;
      case 5:
        betaM = 1.2;
        break;
      case 6:
        betaM = 1.104;
        break;
      case 7:
        betaM = 1.096;
        break;
      default:
        betaM = 1.05;
        break;
    }
    return betaM / Math.sqrt(Math.exp(p * Math.log(2)));
  }
}
