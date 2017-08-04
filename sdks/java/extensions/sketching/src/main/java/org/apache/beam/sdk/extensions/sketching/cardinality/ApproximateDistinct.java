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
package org.apache.beam.sdk.extensions.sketching.cardinality;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code PTransform}s for computing the number of distinct elements in a {@code PCollection}, or
 * the number of distinct values associated with each key in a {@code PCollection} of {@code KV}s.
 *
 * <p>This class uses the HyperLogLog algorithm, and more precisely
 * the improved version of Google (HyperLogLog+).
 *
 * <br>The implementation comes from Addthis' library Stream-lib :
 * <a>https://github.com/addthis/stream-lib</a>
 *
 * <br>The original paper of the HyperLogLog is available here :
 * <a>http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf</a>
 *
 * <br>A paper from the same authors to have a clearer view of the algorithm is available here :
 * <a>http://cscubs.cs.uni-bonn.de/2016/proceedings/paper-03.pdf</a>
 *
 * <br>Google's HyperLogLog+ version is detailed in this paper :
 * <a>https://research.google.com/pubs/pub40671.html</a>
 */
public class ApproximateDistinct {

  private static final Logger LOG = LoggerFactory.getLogger(ApproximateDistinct.class);

    // do not instantiate
  private ApproximateDistinct() {
  }

  /**
   * A {@code PTransform} that takes an inputT {@code PCollection} of objects and returns a
   * {@code PCollection<HyperLogLogPlus>} whose contents is a sketch which approximates
   * the number of distinct element in the input {@code PCollection}.
   *
   * <p>The parameter {@code p} controls the accuracy of the estimation. It represents
   * the number of bits that will be used to index the elements,
   * thus the number of different "buckets" in the HyperLogLog+ sketch.
   * <br>In general, you can expect a relative error of about :
   * <pre>{@code 1.1 / sqrt(2^p)}</pre>
   * For instance, the estimation {@code ApproximateDistinct.globally(12)}
   * will have a relative error of about 2%.
   * <br>Also keep in mind that {@code p} cannot be lower than 4,
   * because the estimation would be too inaccurate.
   * <br>See {@link ApproximateDistinctFn} for more details about the algorithm's principle.
   *
   * <p>HyperLogLog+ version of Google uses a sparse representation in order to
   * optimize memory and improve accuracy for small cardinalities.
   * By calling this builder, you will not use the sparse representation.
   * If you want to, see {@link ApproximateDistinctFn#withSparseRepresentation(int)}
   *
   * <p>Example of use
   * <pre>{@code PCollection<String> input = ...;
   * PCollection<HyperLogLogPlus> hllSketch = input
   *        .apply(ApproximateDistinct.<String>globally(15));
   * }</pre>
   *
   * @param <InputT>    type of elements being combined
   * @param p           number of bits for indexes in the HyperLogLogPlus
   *
   *
   */
  public static <InputT> Combine.Globally<InputT, HyperLogLogPlus> globally(int p) {
    return Combine.<InputT, HyperLogLogPlus>globally(ApproximateDistinctFn.<InputT>create(p));
  }

  /**
   * Do the same as {@link ApproximateDistinct#globally(int)},
   * but with a default value of 18 for p.
   *
   * @param <InputT>  the type of the elements in the input {@code PCollection}
   */
  public static <InputT> Combine.Globally<InputT, HyperLogLogPlus> globally() {
    return globally(18);
  }

  /**
   * A {@code PTransform} that takes an input {@code PCollection<KV<K, InputT>>} and returns a
   * {@code PCollection<KV<K, HyperLogLogPlus>>} that contains an output element mapping each
   * distinct key in the input {@code PCollection} to a structure wrapping a {@link HyperLogLogPlus}
   * which approximates the number of distinct values associated with that key in the input
   * {@code PCollection}.
   *
   * <p>The parameter {@code p} controls the accuracy of the estimation. It represents
   * the number of bits that will be used to index the elements,
   * thus the number of different "buckets" in the HyperLogLog+ sketch.
   * <br>In general, you can expect a relative error of about :
   * <pre>{@code 1.1 / sqrt(2^p)}</pre>
   * For instance, the estimation {@code ApproximateDistinct.globally(12)}
   * will have a relative error of about 2%.
   * <br>Also keep in mind that {@code p} cannot be lower than 4,
   * because the estimation would be too inaccurate.
   * <br>See {@link ApproximateDistinctFn} for more details about the algorithm's principle.
   *
   * <p>HyperLogLog+ version of Google uses a sparse representation in order to
   * optimize memory and improve accuracy for small cardinalities.
   * By calling this builder, you will not use the sparse representation.
   * If you want to, see {@link ApproximateDistinctFn#withSparseRepresentation(int)}
   *
   * <p>Example of use
   * <pre>{@code PCollection<KV<Integer, String>> input = ...;
   * PCollection<KV<Integer, HyperLogLogPlus>> hllSketch = input
   *        .apply(ApproximateDistinct.<Integer, String>perKey(15));
   * }</pre>
   *
   * @param p       number of bits for indexes in the HyperLogLogPlus.
   *
   */
  public static <K, InputT> Combine.PerKey<K, InputT, HyperLogLogPlus> perKey(int p) {
    return Combine.<K, InputT, HyperLogLogPlus>perKey(ApproximateDistinctFn.<InputT>create(p));
  }

  /**
   * Do the same as {@link ApproximateDistinct#globally(int)},
   * but with a default value of 18 for p.
   *
   * @param <K>       the type of the keys in the input and output {@code PCollection}s
   * @param <InputT>  the type of values in the input {@code PCollection}
   */
  public static <K, InputT> Combine.PerKey<K, InputT, HyperLogLogPlus> perKey() {
    return perKey(18);
  }

  /**
   * A {@code Combine.CombineFn} that computes the stream into a {@link HyperLogLogPlus}
   * sketch, useful as an argument to {@link Combine#globally} or {@link Combine#perKey}.
   *
   * <p>The HyperLogLog algorithm relies on the principle that the overall cardinality
   * can be estimated thanks to the longest run of starting 0s of the hashed elements.
   * The longer the run is, the more unlikely it was to happen. Thus, the greater the number
   * of elements that have been hashed before getting such a run.
   * <br>Because this algorithm relies mainly on randomness, the stream is divided into buckets
   * in order to reduce the variance of the estimation.
   * Therefore, an estimation is applied on several samples and the overall estimation
   * is then computed using an average (Harmonic mean).
   *
   * @param <InputT>    the type of the elements being combined
   */
  public static class ApproximateDistinctFn<InputT>
      extends Combine.CombineFn<InputT, HyperLogLogPlus, HyperLogLogPlus> {

    private final int p;

    private final int sp;

    private ApproximateDistinctFn(int p, int sp) {
      this.p = p;
      this.sp = sp;
    }

    /**
     * Returns an {@code ApproximateDistinctFn} combiner with the given precision value p.
     * This means that the input elements will be dispatched into 2^p buckets
     * in order to estimate the cardinality.
     *
     * @param p           precision value for the normal representation
     * @param <InputT>    the type of the input {@code Pcollection}'s elements being combined.
     */
    public static <InputT> ApproximateDistinctFn<InputT> create(int p) {
      return new ApproximateDistinctFn<>(p, 0);
    }

    /**
     * Returns an {@code ApproximateDistinctFn} combiner with the precision value p
     * of this combiner and the given precision value sp for the sparse representation,
     * meaning that the combiner will be using a sparse representation at small cardinalities.
     *
     * <p>The sparse representation does not initialize every buckets to 0 at the beginning.
     * Indeed, for small cardinalities a lot of them will remain empty.
     * Instead it builds a linked-list that grows when new indexes appear in the hashed values.
     * To reduce collision of indexes and thus improve precision, we can define {@code sp > p}
     * When the sparse representation would require more memory than the normal one,
     * it is converted and the normal algorithm applies for the remaining elements.
     *
     * <p><b>WARNING : </b>Choose sp such that {@code p <= sp <= 32}
     *
     * <p>Example of use :
     * <pre>{@code PCollection<Integer> input = ...;
     * PCollection<HyperLogLogPlus> hllSketch = input
     *        .apply(Combine.globally(ApproximateDistinct.ApproximateDistinctFn.<Integer>create(p)
     *                .withSparseRepresentation(sp)));
     * }</pre>
     *
     * @param sp          the precision of HyperLogLog+' sparse representation
     */
    public ApproximateDistinctFn<InputT> withSparseRepresentation(int sp) {
      return new ApproximateDistinctFn<>(this.p, sp);
    }

    @Override
    public HyperLogLogPlus createAccumulator() {
      return new HyperLogLogPlus(p, sp);
    }

    @Override
    public HyperLogLogPlus addInput(HyperLogLogPlus acc, InputT record) {
      acc.offer(record);
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
          throw new IllegalStateException("The accumulators cannot be merged : " + e.getMessage());
          // LOG.error("The accumulators cannot be merged : " + e.getMessage(), e);
        }
      }
      return mergedAccum;
    }

    @Override
    public Coder<HyperLogLogPlus> getAccumulatorCoder(CoderRegistry registry, Coder inputCoder) {
      return HyperLogLogPlusCoder.of();
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

  static class HyperLogLogPlusCoder extends CustomCoder<HyperLogLogPlus> {

    private static final HyperLogLogPlusCoder INSTANCE = new HyperLogLogPlusCoder();

    private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();

    public static HyperLogLogPlusCoder of() {
      return INSTANCE;
    }

    @Override public void encode(HyperLogLogPlus value, OutputStream outStream) throws IOException {
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
   * Where m is the number of buckets used (p = log2(m))
   * and b(m) < 1.106 for m > 16 (p > 4).
   * </pre>
   *
   * <br><b>WARNING : </b>
   * <br>This does not mean relative error in the estimation <b>can't</b> be higher.
   * <br>This only means that on average the relative error will be
   * lower than the desired relative error.
   * <br>Nevertheless, the more elements arrive in the {@code PCollection}, the lower
   * the variation will be.
   * <br>Indeed, this is like when you throw a dice millions of time :
   * The relative frequency of each different result {1,2,3,4,5,6} will get closer to 1/6.
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
  static double mseForP(int p) {
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
