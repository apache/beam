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
package org.apache.beam.sdk.extensions.sketching.quantiles;

import com.tdunning.math.stats.MergingDigest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.transforms.Combine;

/**
 * {@code PTransform}s for getting information about quantiles in the input {@code PCollection},
 * or the occurrences of values associated with each key in a {@code PCollection} of {@code KV}s.
 *
 * <p>This class uses the T-Digest structure, an improvement of Q-Digest made by Ted Dunning.
 * The paper and implementation are available on his Github profile :
 * <a>https://github.com/tdunning/t-digest</a>
 *
 * <p><b>For Your Information :</b>
 * <br>The current release of t-digest (3.1) has non-serializable implementations. This problem
 * has been issued and corrected on the master. The new release should be available soon.
 * <br>Until then a wrapper is used, see {@link SerializableTDigest}.
 */
public class TDigestQuantiles {

  // do not instantiate
  private TDigestQuantiles() {
  }

  /**
   * A {@code PTransform} that takes an input {@code PCollection<Double>} and returns a
   * {@code PCollection<SerializableTDigest>} whose contents is a TDigest sketch for querying
   * the quantiles of the set of input {@code PCollection}'s elements.
   *
   * <p>The compression factor controls the accuracy of the queries. For a compression equal to C
   * the relative error will be at most 3/C.
   *
   * <p>Example of use :
   * <pre>{@code PCollection<Double> input = ...
   * PCollection<SerializableTDigest> sketch = input.apply(TDigestQuantiles.globally(1000));
   * }</pre>
   *
   * @param compression     the compression factor guarantees a relative error of at most
   *                        {@code 3 / compression} on quantiles.
   */
  public static Combine.Globally<Double, SerializableTDigest> globally(int compression) {
    return Combine.<Double, SerializableTDigest>globally(new QuantileFn(compression));
  }

  /**
   * A {@code PTransform} that takes an input {@code PCollection<KV<K, Double>>} and returns a
   * {@code PCollection<KV<K, SerializableTDigest>>} mapping each distinct key in the input
   * {@code PCollection} to the TDigest sketch for querying the quantiles of the set of
   * elements associated with that key in the input {@code PCollection}.
   *
   * <p>The compression factor controls the accuracy of the queries. For a compression equal to C
   * the relative error will be at most 3/C.
   *
   * <p>Example of use :
   * <pre>{@code PCollection<KV<Integer, Double>> input = ...
   * PCollection<KV<Integer, SerializableTDigest>> sketch = input
   *                .apply(TDigestQuantiles.perKey(1000));
   * }</pre>
   *
   * @param compression     the compression factor guarantees a relative error of at most
   *                        {@code 3 / compression} on quantiles.
   * @param <K>             the type of the keys
   */
  public static <K> Combine.PerKey<K, Double, SerializableTDigest> perKey(int compression) {
    return Combine.<K, Double, SerializableTDigest>perKey(new QuantileFn(compression));
  }

  /**
   * A {@code Combine.CombineFn} that computes the {@link SerializableTDigest} structure
   * of an {@code Iterable} of Doubles, useful as an argument to {@link Combine#globally} or
   * {@link Combine#perKey}.
   */
  static class QuantileFn
      extends Combine.CombineFn<Double, SerializableTDigest, SerializableTDigest> {

    private final int compression;

    public QuantileFn(int compression) {
      this.compression = compression;
    }

    @Override public SerializableTDigest createAccumulator() {
      return new SerializableTDigest(compression);
    }

    @Override public SerializableTDigest addInput(SerializableTDigest accum, Double value) {
      accum.add(value);
      return accum;
    }

    @Override public SerializableTDigest extractOutput(SerializableTDigest accum) {
      return accum;
    }

    @Override public SerializableTDigest mergeAccumulators(
        Iterable<SerializableTDigest> accumulators) {
      return SerializableTDigest.merge(accumulators);
    }

    @Override public Coder<SerializableTDigest> getAccumulatorCoder(CoderRegistry registry,
        Coder inputCoder) {
      return SerializableTDigestCoder.of();
    }

    @Override public Coder<SerializableTDigest> getDefaultOutputCoder(CoderRegistry registry,
        Coder inputCoder) {
      return SerializableTDigestCoder.of();
    }

    @Override public SerializableTDigest defaultValue() {
      return new SerializableTDigest(10);
    }
  }

  /**
   * This class is a wrapper for MergingDigest class because it is not serializable.
   * The problem has been issued and corrected on 3.2 version of Ted Dunning's implementation :
   * <a>https://github.com/tdunning/t-digest</a>
   * However, this version has not been released yet so the issue is still up-to-date.
   */
  public static class SerializableTDigest implements Serializable {

    private transient MergingDigest sketch;

    public SerializableTDigest(int compression) {
      sketch = new MergingDigest(compression);
    }

    private SerializableTDigest(MergingDigest sketch) {
      this.sketch = sketch;
    }

    public void add(Double input) {
      this.sketch.add(input, 1);
    }

    public void encode(OutputStream out) throws IOException {
      ByteBuffer buf = ByteBuffer.allocate(sketch.smallByteSize());
      sketch.asSmallBytes(buf);
      ByteArrayCoder.of().encode(buf.array(), out);
    }

    public static SerializableTDigest decode(InputStream in) throws IOException {
      byte[] bytes = ByteArrayCoder.of().decode(in);
      return new SerializableTDigest(MergingDigest.fromBytes(ByteBuffer.wrap(bytes)));
    }

    public static SerializableTDigest merge(Iterable<SerializableTDigest> list) {
      Iterator<SerializableTDigest> it = list.iterator();
      if (!it.hasNext()) {
        return null;
      }
      SerializableTDigest mergedDigest = it.next();
      while (it.hasNext()) {
        SerializableTDigest next = it.next();
        if (next.getSketch().centroids().size() > 1) {
          mergedDigest.sketch.add(next.sketch);
        }
      }
      return mergedDigest;
    }

    public MergingDigest getSketch() {
      return this.sketch;
    }
  }

  static class SerializableTDigestCoder extends CustomCoder<SerializableTDigest> {

    private static final SerializableTDigestCoder INSTANCE = new SerializableTDigestCoder();

    public static SerializableTDigestCoder of() {
      return INSTANCE;
    }

    @Override public void encode(SerializableTDigest value, OutputStream outStream)
          throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null T-Digest sketch");
      }
      value.encode(outStream);
    }

    @Override public SerializableTDigest decode(InputStream inStream) throws IOException {
      return SerializableTDigest.decode(inStream);
    }

    @Override public boolean isRegisterByteSizeObserverCheap(SerializableTDigest value) {
      return true;
    }

    @Override protected long getEncodedElementByteSize(SerializableTDigest value)
          throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null T-Digest sketch");
      }
      return value.getSketch().smallByteSize();
    }
  }
}
