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
package org.apache.beam.sdk.extensions.sketching.frequency;

import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.clearspring.analytics.stream.frequency.FrequencyMergeException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code PTransform}s that records an estimation of the frequency of each element in a
 * {@code PCollection}, or the occurrences of values associated with each key in a
 * {@code PCollection} of {@code KV}s.
 *
 * <p>This class uses the Count-min Sketch structure. The papers and other useful information
 * about it is available on this website : <a>https://sites.google.com/site/countminsketch/</a>
 * <br>The implementation comes from Apache Spark :
 * <a>https://github.com/apache/spark/tree/master/common/sketch</a>
 */
public class SketchFrequencies {

  private static final Logger LOG = LoggerFactory.getLogger(SketchFrequencies.class);

  // do not instantiate
  private SketchFrequencies() {
  }

  /**
   * A {@code PTransform} that takes an input {@code PCollection<String>} and returns a
   * {@code PCollection<CountMinSketch>} whose contents is a Count-min sketch that allows to query
   * the number of hits for a specific element in the input {@code PCollection}.
   *
   * <p>The {@code seed} parameters will be used to randomly generate different hash functions.
   * Thus, the result can be different for the same stream in different seeds are used.
   * The {@code seed} parameter will be used to generate a and b for each hash function.
   * <br>The Count-min sketch size is constant through the process so the memory use is fixed.
   * However, the dimensions are directly linked to the accuracy.
   * <br>By default, the relative error is set to 1% with 1% probability that the estimation
   * breaks this limit.
   * <br>Also keep in mind that this algorithm works well on highly skewed data but gives poor
   * results if the elements are evenly distributed.
   *
   * <p>See {@link CountMinSketchFn#withAccuracy(double, double)} in order to tune the parameters.
   * <br>Also see {@link CountMinSketchFn} for more details about the algorithm's principle.
   *
   * <p>Example of use:
   * <pre>{@code
   * PCollection<String> pc = ...;
   * PCollection<CountMinSketch> countMinSketch =
   *     pc.apply(SketchFrequencies.<String>globally(1234));
   * }</pre>
   *
   * <p>Also see {@link CountMinSketchFn} for more details about the algorithm's principle.
   *
   * @param seed        the seed used for generating randomly different hash functions
   */
  public static Combine.Globally<String, CountMinSketch> globally(int seed) {
    return Combine.<String, CountMinSketch>globally(CountMinSketchFn
            .create(seed).withAccuracy(0.001, 0.99));
  }

  /**
   * A {@code PTransform} that takes an input {@code PCollection<KV<K, InputT>>} and
   * returns a {@code PCollection<KV<K, CountMinSketch>>} that contains an output element mapping
   * each distinct key in the input {@code PCollection} to a structure that allows to query the
   * count of a specific element associated with that key in the input {@code PCollection}.
   *
   * <p>The {@code seed} parameters will be used to randomly generate different hash functions.
   * Thus, the result can be different for the same stream in different seeds are used.
   * The {@code seed} parameter will be used to generate a and b for each hash function.
   * <br>The Count-min sketch size is constant through the process so the memory use is fixed.
   * However, the dimensions are directly linked to the accuracy.
   * <br>By default, the relative error is set to 1% with 1% probability that the estimation
   * breaks this limit.
   * <br>also keep in mind that this algorithm works well on highly skewed data but gives poor
   * results if the elements are evenly distributed.
   *
   * <p>See {@link CountMinSketchFn#withAccuracy(double, double)} in order to tune the parameters.
   * <br>Also see {@link CountMinSketchFn} for more details about the algorithm's principle.
   *
   * <p>Example of use:
   * <pre>{@code
   * PCollection<KV<Integer, String>> pc = ...;
   * PCollection<KV<Integer, CountMinSketch>> countMinSketch =
   *     pc.apply(SketchFrequencies.<Integer, String>perKey(1234));
   * }</pre>
   *
   * @param seed        the seed used for generating different hash functions
   * @param <K>         the type of the keys in the input and output {@code PCollection}s
   */
  public static <K> Combine.PerKey<K, String, CountMinSketch> perKey(int seed) {
    return Combine.<K, String, CountMinSketch>perKey(CountMinSketchFn
            .create(seed).withAccuracy(0.001, 0.99));
  }

  /**
   * A {@code Combine.CombineFn} that computes the {@link CountMinSketch} Structure
   * of an {@code Iterable} of Strings, useful as an argument to {@link Combine#globally} or
   * {@link Combine#perKey}.
   *
   * <p>When an element is added to the Count-min sketch, it is mapped to one column in each
   * row using different hash functions, and a counter is updated in each column.
   * <br>Collisions will happen as far as the number of distinct elements in the stream is greater
   * than the width of the sketch. Each counter might be associated to many items so the frequency
   * of an element is always overestimated. On average the relative error on a counter is bounded,
   * but some counters can be very inaccurate.
   * <br>That's why different hash functions are used to map the same element to different
   * counters. Thus, the overestimation for each counter will differ as there will be different
   * collisions, and one will probably be less inaccurate than the average.
   *
   * <p>Both the average relative error and the probability to have an estimation overcoming this
   * error can be computed by knowing the dimensions of the sketch, and vice-versa.
   * Thus, for Count-min sketch with 10 000 columns and 7 rows, the relative error should not be no
   * more than 0.02% in 99% of the cases.
   *
   */
  public static class CountMinSketchFn
          extends Combine.CombineFn<String, CountMinSketch, CountMinSketch> {

    private final int depth;

    private final int width;

    private final int seed;

    private CountMinSketchFn(double eps, double confidence, int seed) {
      this.width = (int) Math.ceil(2 / eps);
      this.depth = (int) Math.ceil(-Math.log(1 - confidence) / Math.log(2));
      this.seed = seed;
    }

    private CountMinSketchFn(int width, int depth, int seed) {
      this.width = width;
      this.depth = depth;
      this.seed = seed;
    }

    /**
     * Returns an {@code CountMinSketchFn} combiner that will have a Count-min sketch
     * which will estimate the frequencies with about 1% of error guaranteed at 99%.
     * the resulting dimensions are 2000 x 7. It will stay constant during all the aggregation.
     *
     * <p>the {@code seed} parameters is used to generate different hash functions of the form :
     * <pre>a * i + b % p % width ,</pre>
     * where a, b are chosen randomly and p is a prime number larger than the maximum i value.
     *
     * <p>Example of use:
     * <br>1) Globally :
     * <pre>{@code
     * PCollection<String> pc = ...;
     * PCollection<CountMinSketch> countMinSketch =
     *     pc.apply(Combine.globally(CountMinSketchFn.<String>create(1234));
     * }</pre>
     * <br>2) Per key :
     * <pre>{@code
     * PCollection<KV<Integer, String>> pc = ...;
     * PCollection<KV<Integer, CountMinSketch>> countMinSketch =
     *     pc.apply(Combine.perKey(CountMinSketchFn.<String>create(1234));
     * }</pre>
     *
     * @param seed        the seed used for generating different hash functions
     */
    public static CountMinSketchFn create(int seed) {
      return new CountMinSketchFn(0.001, 0.99, seed);
    }

    /**
     * Returns an {@code CountMinSketchFn} combiner that will have a Count-min sketch of
     * dimensions {@code width x depth}, that will stay constant during all the aggregation.
     * This method can only be applied from a {@link CountMinSketchFn} already created with the
     * method {@link CountMinSketchFn#create(int)}.
     *
     * <p>The greater the {@code width}, the lower the expected relative error {@code epsilon} :
     * <pre>{@code epsilon = 2 / width}</pre>
     *
     * <p>The greater the {@code depth}, the lower the probability to actually have
     * a greater relative error than expected.
     * <pre>{@code confidence = 1 - 2^-depth}</pre>
     *
     * <p>Example of use:
     * <br>1) Globally :
     * <pre> {@code
     * PCollection<String> pc = ...;
     * PCollection<CountMinSketch> countMinSketch =
     *     pc.apply(Combine.globally(CountMinSketchFn.<String>create(1234)
     *                  withDimensions(10000, 7));
     * } </pre>
     * <br>2) Per key :
     * <pre> {@code
     * PCollection<KV<Integer, String>> pc = ...;
     * PCollection<KV<Integer, CountMinSketch>> countMinSketch =
     *     pc.apply(Combine.perKey(CountMinSketchFn.<String>create(1234)
     *                  withDimensions(10000, 7)););
     * } </pre>
     *
     * @param width Number of columns, i.e. number of counters for the stream.
     * @param depth Number of lines, i.e. number of hash functions
     */
    public CountMinSketchFn withDimensions(int width, int depth) {
      if (width <= 0 || depth <= 0) {
          throw new IllegalArgumentException("depth and width must be positive.");
      }
      return new CountMinSketchFn(width, depth, this.seed);
    }

    /**
     * Returns an {@code CountMinSketchFn} combiner that will be as accurate as specified. The
     * relative error {@code epsilon} can be guaranteed only with a certain {@code confidence},
     * which has to be between 0 and 1 (1 being of course impossible). Those parameters will
     * determine the size of the Count-min sketch in which the elements will be aggregated.
     * This method can only be applied to a {@link CountMinSketchFn} already created with the
     * method {@link CountMinSketchFn#create(int)}.
     *
     * <p>The lower the {@code epsilon} value, the greater the width.
     * <pre>{@code width = (int) 2 / epsilon)}</pre>
     *
     * <p>The greater the confidence, the greater the depth.
     * <pre>{@code depth = (int) -log2(1 - confidence)}</pre>
     *
     * <p>Example of use:
     * <br>1) Globally :
     * <pre>{@code
     * PCollection<String> pc = ...;
     * PCollection<CountMinSketch> countMinSketch =
     *     pc.apply(Combine.globally(CountMinSketchFn.<String>create(1234)
     *                  withDimensions(0.001, 0.99));
     * }</pre>
     * <br>2) Per key :
     * <pre>{@code
     * PCollection<KV<Integer, String>> pc = ...;
     * PCollection<KV<Integer, CountMinSketch>> countMinSketch =
     *     pc.apply(Combine.perKey(CountMinSketchFn.<String>create(1234)
     *                  withAccuracy(0.001, 0.99)););
     * }</pre>
     *
     *
     * @param epsilon the relative error of the result
     * @param confidence the confidence in the result to not overcome the relative error
     */
    public CountMinSketchFn withAccuracy(double epsilon, double confidence) {
      return new CountMinSketchFn(epsilon, confidence, this.seed);
    }

    @Override public CountMinSketch createAccumulator() {
      return new CountMinSketch(this.depth, this.width, this.seed);
    }

    @Override public CountMinSketch addInput(CountMinSketch accumulator, String element) {
      accumulator.add(element, 1);
      return accumulator;
    }

    @Override public CountMinSketch mergeAccumulators(Iterable<CountMinSketch> accumulators) {
      Iterator<CountMinSketch> it = accumulators.iterator();
      if (!it.hasNext()) {
        return new CountMinSketch(seed, width, depth);
      }
      CountMinSketch merged = it.next();
      try {
        while (it.hasNext()) {
          merged = CountMinSketch.merge(merged, it.next());
        }
      } catch (FrequencyMergeException e) {
        // Should never happen because all the accumulators created are of the same type.
        LOG.error(e.getMessage(), e);
      }
      return merged;
    }

    @Override public CountMinSketch extractOutput(CountMinSketch accumulator) {
      return accumulator;
    }

    @Override public Coder<CountMinSketch> getAccumulatorCoder(CoderRegistry registry,
        Coder inputCoder) {
      return new CountMinSketchCoder();
    }

    @Override public Coder<CountMinSketch> getDefaultOutputCoder(CoderRegistry registry,
        Coder inputCoder) throws CannotProvideCoderException {
      return new CountMinSketchCoder();
    }

    @Override public CountMinSketch defaultValue() {
      return new CountMinSketch(1, 1, 1);
    }
  }

  static class CountMinSketchCoder extends CustomCoder<CountMinSketch> {

    private static final ByteArrayCoder BYTE_ARRAY_CODER = ByteArrayCoder.of();

    @Override public void encode(CountMinSketch value, OutputStream outStream) throws IOException {
        if (value == null) {
          throw new CoderException("cannot encode a null Count-min Sketch");
        }
        BYTE_ARRAY_CODER.encode(CountMinSketch.serialize(value), outStream);
    }

    @Override public CountMinSketch decode(InputStream inStream) throws IOException {
      return CountMinSketch.deserialize(BYTE_ARRAY_CODER.decode(inStream));
    }

    @Override public boolean consistentWithEquals() {
      return false;
    }

    @Override public boolean isRegisterByteSizeObserverCheap(CountMinSketch value) {
      return true;
    }

    @Override protected long getEncodedElementByteSize(CountMinSketch value) throws IOException {
      if (value == null) {
        throw new CoderException("cannot encode a null Count-min Sketch");
      } else {
        // depth and width as computed in the CountMinSketch constructor from the relative error and
        // confidence.
        int width = (int) Math.ceil(2 / value.getRelativeError());
        int depth = (int) Math.ceil(-Math.log(1 - value.getConfidence()) / Math.log(2));

        // 8L is for the sketch's size (long)
        // 4L * 2 is for depth and width (ints)
        // 8L * depth * (width + 1) is a factorization for the sizes of table (long[depth][width])
        // and hashA (long[depth])
        return 8L + 4L * 2 + 8L * depth * (width + 1);
      }
    }
  }
}
