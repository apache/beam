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

import com.google.auto.value.AutoValue;
import com.tdunning.math.stats.MergingDigest;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * {@code PTransform}s for getting information about quantiles in a stream.
 *
 * <p>This class uses the T-Digest structure introduced by Ted Dunning, and more precisely the
 * {@link MergingDigest} implementation.
 *
 * <h2>References</h2>
 *
 * <p>The paper and implementation are available on Ted Dunning's <a
 * href="https://github.com/tdunning/t-digest">Github profile</a>
 *
 * <h2>Parameters</h2>
 *
 * <p>Only one parameter can be tuned in order to control the tradeoff between the estimation
 * accuracy and the memory use. <br>
 *
 * <p>Stream elements are compressed into a linked list of centroids. The compression factor {@code
 * cf} is used to limit the number of elements represented by each centroid as well as the total
 * number of centroids. <br>
 * The relative error will always be a small fraction of 1% for values at extreme quantiles and
 * always be less than 3/cf at middle quantiles. <br>
 *
 * <p>By default the compression factor is set to 100, which guarantees a relative error less than
 * 3%.
 *
 * <h2>Examples</h2>
 *
 * <p>There are 2 ways of using this class:
 *
 * <ul>
 *   <li>Use the {@link PTransform}s that return a {@link PCollection} which contains a {@link
 *       MergingDigest} for querying the value at a given quantile or the approximate quantile
 *       position of an element.
 *   <li>Use the {@link TDigestQuantilesFn} {@code CombineFn} that is exposed in order to make
 *       advanced processing involving the {@link MergingDigest}.
 * </ul>
 *
 * <h3>Example 1: Default use</h3>
 *
 * <p>The simplest use is to call the {@link #globally()} or {@link #perKey()} method in order to
 * retrieve the digest, and then to query the structure.
 *
 * <pre><code>
 * {@literal PCollection<Double>} pc = ...;
 * {@literal PCollection<MergingDigest>} countMinSketch = pc.apply(TDigestQuantiles
 *         .globally()); // .perKey()
 * </code></pre>
 *
 * <h3>Example 2: tune accuracy parameters</h3>
 *
 * <p>One can tune the compression factor {@code cf} in order to control accuracy and memory. <br>
 * This tuning works exactly the same for {@link #globally()} and {@link #perKey()}.
 *
 * <pre><code>
 *  double cf = 500;
 * {@literal PCollection<Double>} pc = ...;
 * {@literal PCollection<MergingDigest>} countMinSketch = pc.apply(TDigestQuantiles
 *         .globally() // .perKey()
 *         .withCompression(cf);
 * </code></pre>
 *
 * <h3>Example 3 : Query the resulting structure</h3>
 *
 * <p>This example shows how to query the resulting structure, for example to build {@code
 * PCollection} of {@link KV}s with each pair corresponding to a couple (quantile, value).
 *
 * <pre><code>
 * {@literal PCollection<MergingDigest>} pc = ...;
 * {@literal PCollection<KV<Double, Double>>} quantiles = pc.apply(ParDo.of(
 *        {@literal new DoFn<MergingDigest, KV<Double, Double>>()} {
 *          {@literal @ProcessElement}
 *           public void processElement(ProcessContext c) {
 *             double[] quantiles = {0.01, 0.25, 0.5, 0.75, 0.99}
 *             for (double q : quantiles) {
 *                c.output(KV.of(q, c.element().quantile(q));
 *             }
 *           }}));
 * </code></pre>
 *
 * <p>One can also retrieve the approximate quantile position of a given element in the stream using
 * {@code cdf(double)} method instead of {@code quantile(double)}.
 *
 * <h3>Example 4: Using the CombineFn</h3>
 *
 * <p>The {@code CombineFn} does the same thing as the {@code PTransform}s but it can be used for
 * doing stateful processing or in {@link
 * org.apache.beam.sdk.transforms.CombineFns.ComposedCombineFn}.
 *
 * <p>This example is not really interesting but it shows how one can properly create a {@link
 * TDigestQuantilesFn}.
 *
 * <pre><code>
 *  double cf = 250;
 * {@literal PCollection<Double>} input = ...;
 * {@literal PCollection<MergingDigest>} output = input.apply(Combine
 *         .globally(TDigestQuantilesFn.create(cf)));
 * </code></pre>
 */
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public final class TDigestQuantiles {

  /**
   * Compute the stream in order to build a T-Digest structure (MergingDigest) for keeping track of
   * the stream distribution and returns a {@code PCollection<MergingDigest>}. <br>
   * The resulting structure can be queried in order to retrieve the approximate value at a given
   * quantile or the approximate quantile position of a given element.
   */
  public static GlobalDigest globally() {
    return GlobalDigest.builder().build();
  }

  /**
   * Like {@link #globally()}, but builds a digest for each key in the stream.
   *
   * @param <K> the type of the keys
   */
  public static <K> PerKeyDigest<K> perKey() {
    return PerKeyDigest.<K>builder().build();
  }

  /** Implementation of {@link #globally()}. */
  @AutoValue
  public abstract static class GlobalDigest
      extends PTransform<PCollection<Double>, PCollection<MergingDigest>> {

    abstract double compression();

    abstract Builder toBuilder();

    static Builder builder() {
      return new AutoValue_TDigestQuantiles_GlobalDigest.Builder().setCompression(100);
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setCompression(double cf);

      abstract GlobalDigest build();
    }

    /**
     * Sets the compression factor {@code cf}.
     *
     * <p>Keep in mind that a compression factor {@code cf} of c guarantees a relative error less
     * than 3/c at mid quantiles. <br>
     * The accuracy will always be significantly less than 1% at extreme quantiles.
     *
     * @param cf the bound value for centroid and digest sizes.
     */
    public GlobalDigest withCompression(double cf) {
      return toBuilder().setCompression(cf).build();
    }

    @Override
    public PCollection<MergingDigest> expand(PCollection<Double> input) {
      return input.apply(
          "Compute T-Digest Structure",
          Combine.globally(TDigestQuantilesFn.create(this.compression())));
    }
  }

  /** Implementation of {@link #perKey()}. */
  @AutoValue
  public abstract static class PerKeyDigest<K>
      extends PTransform<PCollection<KV<K, Double>>, PCollection<KV<K, MergingDigest>>> {

    abstract double compression();

    abstract Builder<K> toBuilder();

    static <K> Builder<K> builder() {
      return new AutoValue_TDigestQuantiles_PerKeyDigest.Builder<K>().setCompression(100);
    }

    @AutoValue.Builder
    abstract static class Builder<K> {
      abstract Builder<K> setCompression(double cf);

      abstract PerKeyDigest<K> build();
    }

    /**
     * Sets the compression factor {@code cf}.
     *
     * <p>Keep in mind that a compression factor {@code cf} of c guarantees a relative error less
     * than 3/c at mid quantiles. <br>
     * The accuracy will always be significantly less than 1% at extreme quantiles.
     *
     * @param cf the bound value for centroid and digest sizes.
     */
    public PerKeyDigest<K> withCompression(double cf) {
      return toBuilder().setCompression(cf).build();
    }

    @Override
    public PCollection<KV<K, MergingDigest>> expand(PCollection<KV<K, Double>> input) {
      return input.apply(
          "Compute T-Digest Structure",
          Combine.perKey(TDigestQuantilesFn.create(this.compression())));
    }
  }

  /** Implements the {@link Combine.CombineFn} of {@link TDigestQuantiles} transforms. */
  public static class TDigestQuantilesFn
      extends Combine.CombineFn<Double, MergingDigest, MergingDigest> {

    private final double compression;

    private TDigestQuantilesFn(double compression) {
      this.compression = compression;
    }

    /**
     * Returns {@link TDigestQuantilesFn} combiner with the given compression factor.
     *
     * <p>Keep in mind that a compression factor {@code cf} of c guarantees a relative error less
     * than 3/c at mid quantiles. <br>
     * The accuracy will always be significantly less than 1% at extreme quantiles.
     *
     * @param compression the bound value for centroid and digest sizes.
     */
    public static TDigestQuantilesFn create(double compression) {
      if (compression > 0) {
        return new TDigestQuantilesFn(compression);
      }
      throw new IllegalArgumentException("Compression factor should be greater than 0.");
    }

    @Override
    public MergingDigest createAccumulator() {
      return new MergingDigest(compression);
    }

    @Override
    public MergingDigest addInput(MergingDigest accum, Double value) {
      accum.add(value);
      return accum;
    }

    /** Output the whole structure so it can be queried, reused or stored easily. */
    @Override
    public MergingDigest extractOutput(MergingDigest accum) {
      return accum;
    }

    @Override
    public MergingDigest mergeAccumulators(Iterable<MergingDigest> accumulators) {
      Iterator<MergingDigest> it = accumulators.iterator();
      MergingDigest merged = it.next();
      while (it.hasNext()) {
        merged.add(it.next());
      }
      return merged;
    }

    @Override
    public Coder<MergingDigest> getAccumulatorCoder(CoderRegistry registry, Coder inputCoder) {
      return new MergingDigestCoder();
    }

    @Override
    public Coder<MergingDigest> getDefaultOutputCoder(CoderRegistry registry, Coder inputCoder) {
      return new MergingDigestCoder();
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("compression", compression).withLabel("Compression factor"));
    }
  }

  /** Coder for {@link MergingDigest} class. */
  static class MergingDigestCoder extends CustomCoder<MergingDigest> {

    private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();

    @Override
    public void encode(MergingDigest value, OutputStream outStream) throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null T-Digest sketch");
      }
      ByteBuffer buf = ByteBuffer.allocate(value.byteSize());
      value.asBytes(buf);
      BYTE_ARRAY_CODER.encode(buf.array(), outStream);
    }

    @Override
    public MergingDigest decode(InputStream inStream) throws IOException {
      byte[] bytes = BYTE_ARRAY_CODER.decode(inStream);
      ByteBuffer buf = ByteBuffer.wrap(bytes);
      return MergingDigest.fromBytes(buf);
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(MergingDigest value) {
      return true;
    }

    @Override
    protected long getEncodedElementByteSize(MergingDigest value) throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null T-Digest sketch");
      }
      return value.byteSize();
    }
  }
}
