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
package org.apache.beam.sdk.extensions.zetasketch;

import com.google.zetasketch.HyperLogLogPlusPlus;
import java.nio.ByteBuffer;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code PTransform}s to compute HyperLogLogPlusPlus (HLL++) sketches on data streams based on the
 * <a href="https://github.com/google/zetasketch">ZetaSketch</a> implementation.
 *
 * <p>HLL++ is an algorithm implemented by Google that estimates the count of distinct elements in a
 * data stream. HLL++ requires significantly less memory than the linear memory needed for exact
 * computation, at the cost of a small error. Cardinalities of arbitrary breakdowns can be computed
 * using the HLL++ sketch. See this <a
 * href="http://static.googleusercontent.com/media/research.google.com/en/us/pubs/archive/40671.pdf">published
 * paper</a> for details about the algorithm.
 *
 * <p>HLL++ functions are also supported in <a
 * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions">Google Cloud
 * BigQuery</a>. The {@code HllCount PTransform}s provided here produce and consume sketches
 * compatible with BigQuery.
 *
 * <p>For detailed design of this class, see https://s.apache.org/hll-in-beam.
 *
 * <h3>Examples</h3>
 *
 * <h4>Example 1: Create long-type sketch for a {@code PCollection<Long>} and specify precision</h4>
 *
 * <pre>{@code
 * PCollection<Long> input = ...;
 * int p = ...;
 * PCollection<byte[]> sketch = input.apply(HllCount.Init.forLongs().withPrecision(p).globally());
 * }</pre>
 *
 * <h4>Example 2: Create bytes-type sketch for a {@code PCollection<KV<String, byte[]>>}</h4>
 *
 * <pre>{@code
 * PCollection<KV<String, byte[]>> input = ...;
 * PCollection<KV<String, byte[]>> sketch = input.apply(HllCount.Init.forBytes().perKey());
 * }</pre>
 *
 * <h4>Example 3: Merge existing sketches in a {@code PCollection<byte[]>} into a new one</h4>
 *
 * <pre>{@code
 * PCollection<byte[]> sketches = ...;
 * PCollection<byte[]> mergedSketch = sketches.apply(HllCount.MergePartial.globally());
 * }</pre>
 *
 * <h4>Example 4: Estimates the count of distinct elements in a {@code PCollection<String>}</h4>
 *
 * <pre>{@code
 * PCollection<String> input = ...;
 * PCollection<Long> countDistinct =
 *     input.apply(HllCount.Init.forStrings().globally()).apply(HllCount.Extract.globally());
 * }</pre>
 *
 * Note: Currently HllCount does not work on FnAPI workers. See <a
 * href="https://issues.apache.org/jira/browse/BEAM-7879">Jira ticket [BEAM-7879]</a>.
 */
@Experimental
public final class HllCount {

  private static final Logger LOG = LoggerFactory.getLogger(HllCount.class);

  /**
   * The minimum {@code precision} value you can set in {@link Init.Builder#withPrecision(int)} is
   * {@value}.
   */
  public static final int MINIMUM_PRECISION = HyperLogLogPlusPlus.MINIMUM_PRECISION;

  /**
   * The maximum {@code precision} value you can set in {@link Init.Builder#withPrecision(int)} is
   * {@value}.
   */
  public static final int MAXIMUM_PRECISION = HyperLogLogPlusPlus.MAXIMUM_PRECISION;

  /**
   * The default {@code precision} value used in {@link Init.Builder#withPrecision(int)} is
   * {@value}.
   */
  public static final int DEFAULT_PRECISION = HyperLogLogPlusPlus.DEFAULT_NORMAL_PRECISION;

  // Cannot be instantiated. This class is intended to be a namespace only.
  private HllCount() {}

  /**
   * Converts the passed-in sketch from {@code ByteBuffer} to {@code byte[]}, mapping {@code null
   * ByteBuffer}s (representing empty sketches) to empty {@code byte[]}s.
   *
   * <p>Utility method to convert sketches materialized with ZetaSQL/BigQuery to valid inputs for
   * Beam {@code HllCount} transforms.
   */
  public static byte[] getSketchFromByteBuffer(@Nullable ByteBuffer bf) {
    if (bf == null) {
      return new byte[0];
    } else {
      byte[] result = new byte[bf.remaining()];
      bf.get(result);
      return result;
    }
  }

  /**
   * Provides {@code PTransform}s to aggregate inputs into HLL++ sketches. The four supported input
   * types are {@code Integer}, {@code Long}, {@code String}, and {@code byte[]}.
   *
   * <p>Sketches are represented using the {@code byte[]} type. Sketches of the same type can be
   * merged into a new sketch using {@link HllCount.MergePartial}. Estimated count of distinct
   * elements can be extracted from sketches using {@link HllCount.Extract}.
   *
   * <p>An "empty sketch" represented by an byte array of length 0 is returned if the input {@code
   * PCollection} is empty.
   *
   * <p>Corresponds to the {@code HLL_COUNT.INIT(input [, precision])} function in <a
   * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions">BigQuery</a>.
   */
  public static final class Init {

    // Cannot be instantiated. This class is intended to be a namespace only.
    private Init() {}

    /**
     * Returns a {@link Builder} for a {@code HllCount.Init} combining {@code PTransform} that
     * computes integer-type HLL++ sketches. Call {@link Builder#globally()} or {@link
     * Builder#perKey()} on the returning {@link Builder} to finalize the {@code PTransform}.
     *
     * <p>Calling {@link Builder#globally()} returns a {@code PTransform} that takes an input {@code
     * PCollection<Integer>} and returns a {@code PCollection<byte[]>} which consists of the
     * integer-type HLL++ sketch computed from the elements in the input {@code PCollection}.
     *
     * <p>Calling {@link Builder#perKey()} returns a {@code PTransform} that takes an input {@code
     * PCollection<KV<K, Integer>>} and returns a {@code PCollection<KV<K, byte[]>>} which consists
     * of the per-key integer-type HLL++ sketch computed from the values matching each key in the
     * input {@code PCollection}.
     *
     * <p>Integer-type sketches cannot be merged with sketches of other types.
     */
    public static Builder<Integer> forIntegers() {
      return new Builder<>(HllCountInitFn.forInteger());
    }

    /**
     * Returns a {@link Builder} for a {@code HllCount.Init} combining {@code PTransform} that
     * computes long-type HLL++ sketches. Call {@link Builder#globally()} or {@link
     * Builder#perKey()} on the returning {@link Builder} to finalize the {@code PTransform}.
     *
     * <p>Calling {@link Builder#globally()} returns a {@code PTransform} that takes an input {@code
     * PCollection<Long>} and returns a {@code PCollection<byte[]>} which consists of the long-type
     * HLL++ sketch computed from the elements in the input {@code PCollection}.
     *
     * <p>Calling {@link Builder#perKey()} returns a {@code PTransform} that takes an input {@code
     * PCollection<KV<K, Long>>} and returns a {@code PCollection<KV<K, byte[]>>} which consists of
     * the per-key long-type HLL++ sketch computed from the values matching each key in the input
     * {@code PCollection}.
     *
     * <p>Long-type sketches cannot be merged with sketches of other types.
     */
    public static Builder<Long> forLongs() {
      return new Builder<>(HllCountInitFn.forLong());
    }

    /**
     * Returns a {@link Builder} for a {@code HllCount.Init} combining {@code PTransform} that
     * computes string-type HLL++ sketches. Call {@link Builder#globally()} or {@link
     * Builder#perKey()} on the returning {@link Builder} to finalize the {@code PTransform}.
     *
     * <p>Calling {@link Builder#globally()} returns a {@code PTransform} that takes an input {@code
     * PCollection<String>} and returns a {@code PCollection<byte[]>} which consists of the
     * string-type HLL++ sketch computed from the elements in the input {@code PCollection}.
     *
     * <p>Calling {@link Builder#perKey()} returns a {@code PTransform} that takes an input {@code
     * PCollection<KV<K, String>>} and returns a {@code PCollection<KV<K, byte[]>>} which consists
     * of the per-key string-type HLL++ sketch computed from the values matching each key in the
     * input {@code PCollection}.
     *
     * <p>String-type sketches cannot be merged with sketches of other types.
     */
    public static Builder<String> forStrings() {
      return new Builder<>(HllCountInitFn.forString());
    }

    /**
     * Returns a {@link Builder} for a {@code HllCount.Init} combining {@code PTransform} that
     * computes bytes-type HLL++ sketches. Call {@link Builder#globally()} or {@link
     * Builder#perKey()} on the returning {@link Builder} to finalize the {@code PTransform}.
     *
     * <p>Calling {@link Builder#globally()} returns a {@code PTransform} that takes an input {@code
     * PCollection<byte[]>} and returns a {@code PCollection<byte[]>} which consists of the
     * bytes-type HLL++ sketch computed from the elements in the input {@code PCollection}.
     *
     * <p>Calling {@link Builder#perKey()} returns a {@code PTransform} that takes an input {@code
     * PCollection<KV<K, byte[]>>} and returns a {@code PCollection<KV<K, byte[]>>} which consists
     * of the per-key bytes-type HLL++ sketch computed from the values matching each key in the
     * input {@code PCollection}.
     *
     * <p>Bytes-type sketches cannot be merged with sketches of other types.
     */
    public static Builder<byte[]> forBytes() {
      return new Builder<>(HllCountInitFn.forBytes());
    }

    /**
     * Builder for the {@code HllCount.Init} combining {@code PTransform}.
     *
     * <p>Call {@link #withPrecision(int)} to customize the {@code precision} parameter of the
     * sketch.
     *
     * <p>Call {@link #globally()} or {@link #perKey()} to finalize the {@code PTransform}.
     *
     * @param <InputT> element type or value type in {@code KV}s of the input {@code PCollection} to
     *     the {@code PTransform} being built
     */
    public static final class Builder<InputT extends @Nullable Object> {

      private final HllCountInitFn<InputT, ?> initFn;

      private Builder(HllCountInitFn<InputT, ?> initFn) {
        this.initFn = initFn;
      }

      /**
       * Explicitly set the {@code precision} parameter used to compute HLL++ sketch.
       *
       * <p>Valid range is between {@link #MINIMUM_PRECISION} and {@link #MAXIMUM_PRECISION}. If
       * this method is not called, {@link #DEFAULT_PRECISION} will be used. Sketches computed using
       * different {@code precision}s cannot be merged together.
       *
       * @param precision the {@code precision} parameter used to compute HLL++ sketch
       */
      public Builder<InputT> withPrecision(int precision) {
        initFn.setPrecision(precision);
        return this;
      }

      /**
       * Returns a {@link Combine.Globally} {@code PTransform} that takes an input {@code
       * PCollection<InputT>} and returns a {@code PCollection<byte[]>} which consists of the HLL++
       * sketch computed from the elements in the input {@code PCollection}.
       *
       * <p>Returns a singleton {@code PCollection} with an "empty sketch" (byte array of length 0)
       * if the input {@code PCollection} is empty.
       */
      public Combine.Globally<InputT, byte[]> globally() {
        return Combine.globally(initFn);
      }

      /**
       * Returns a {@link Combine.PerKey} {@code PTransform} that takes an input {@code
       * PCollection<KV<K, InputT>>} and returns a {@code PCollection<KV<K, byte[]>>} which consists
       * of the per-key HLL++ sketch computed from the values matching each key in the input {@code
       * PCollection}.
       */
      public <K> Combine.PerKey<K, InputT, byte[]> perKey() {
        return Combine.perKey(initFn);
      }
    }
  }

  /**
   * Provides {@code PTransform}s to merge HLL++ sketches into a new sketch.
   *
   * <p>Only sketches of the same type can be merged together. If incompatible sketches are
   * provided, a runtime error will occur.
   *
   * <p>If sketches of different {@code precision}s are merged, the merged sketch will get the
   * minimum precision encountered among all the input sketches.
   *
   * <p>An "empty sketch" represented by an byte array of length 0 is returned if the input {@code
   * PCollection} is empty.
   *
   * <p>Corresponds to the {@code HLL_COUNT.MERGE_PARTIAL(sketch)} function in <a
   * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions">BigQuery</a>.
   */
  public static final class MergePartial {

    // Cannot be instantiated. This class is intended to be a namespace only.
    private MergePartial() {}

    /**
     * Returns a {@link Combine.Globally} {@code PTransform} that takes an input {@code
     * PCollection<byte[]>} of HLL++ sketches and returns a {@code PCollection<byte[]>} of a new
     * sketch merged from the input sketches.
     *
     * <p>Only sketches of the same type can be merged together. If incompatible sketches are
     * provided, a runtime error will occur.
     *
     * <p>If sketches of different {@code precision}s are merged, the merged sketch will get the
     * minimum precision encountered among all the input sketches.
     *
     * <p>Returns a singleton {@code PCollection} with an "empty sketch" (byte array of length 0) if
     * the input {@code PCollection} is empty.
     */
    public static Combine.Globally<byte @Nullable [], byte[]> globally() {
      return Combine.globally(HllCountMergePartialFn.create());
    }

    /**
     * Returns a {@link Combine.PerKey} {@code PTransform} that takes an input {@code
     * PCollection<KV<K, byte[]>>} of (key, HLL++ sketch) pairs and returns a {@code
     * PCollection<KV<K, byte[]>>} of (key, new sketch merged from the input sketches under the
     * key).
     *
     * <p>If sketches of different {@code precision}s are merged, the merged sketch will get the
     * minimum precision encountered among all the input sketches.
     *
     * <p>Only sketches of the same type can be merged together. If incompatible sketches are
     * provided, a runtime error will occur.
     */
    public static <K> Combine.PerKey<K, byte @Nullable [], byte[]> perKey() {
      return Combine.perKey(HllCountMergePartialFn.create());
    }
  }

  /**
   * Provides {@code PTransform}s to extract the estimated count of distinct elements (as {@code
   * Long}s) from each HLL++ sketch.
   *
   * <p>When extracting from an "empty sketch" represented by an byte array of length 0, the result
   * returned is 0.
   *
   * <p>Corresponds to the {@code HLL_COUNT.EXTRACT(sketch)} function in <a
   * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions">BigQuery</a>.
   */
  public static final class Extract {

    // Cannot be instantiated. This class is intended to be a namespace only.
    private Extract() {}

    /**
     * Returns a {@code PTransform} that takes an input {@code PCollection<byte[]>} of HLL++
     * sketches and returns a {@code PCollection<Long>} of the estimated count of distinct elements
     * extracted from each sketch.
     *
     * <p>Returns 0 if the input element is an "empty sketch" (byte array of length 0).
     */
    public static PTransform<PCollection<byte[]>, PCollection<Long>> globally() {
      return new Globally();
    }

    /**
     * Returns a {@code PTransform} that takes an input {@code PCollection<KV<K, byte[]>>} of (key,
     * HLL++ sketch) pairs and returns a {@code PCollection<KV<K, Long>>} of (key, estimated count
     * of distinct elements extracted from each sketch).
     */
    public static <K> PTransform<PCollection<KV<K, byte[]>>, PCollection<KV<K, Long>>> perKey() {
      return new PerKey<K>();
    }

    private static final class Globally extends PTransform<PCollection<byte[]>, PCollection<Long>> {

      @Override
      public PCollection<Long> expand(PCollection<byte[]> input) {
        return input.apply(
            ParDo.of(
                new DoFn<byte[], Long>() {
                  @ProcessElement
                  public void processElement(
                      @Element byte[] sketch, OutputReceiver<Long> receiver) {
                    if (sketch == null) {
                      LOG.warn(
                          "Received a null and treated it as an empty sketch. "
                              + "Consider replacing nulls with empty byte arrays (byte[0]) "
                              + "in upstream transforms for better space-efficiency and safety.");
                      receiver.output(0L);
                    } else if (sketch.length == 0) {
                      receiver.output(0L);
                    } else {
                      receiver.output(HyperLogLogPlusPlus.forProto(sketch).result());
                    }
                  }
                }));
      }
    }

    private static final class PerKey<K>
        extends PTransform<PCollection<KV<K, byte[]>>, PCollection<KV<K, Long>>> {

      @Override
      public PCollection<KV<K, Long>> expand(PCollection<KV<K, byte[]>> input) {
        return input.apply(
            ParDo.of(
                new DoFn<KV<K, byte[]>, KV<K, Long>>() {
                  @ProcessElement
                  public void processElement(
                      @Element KV<K, byte[]> kv, OutputReceiver<KV<K, Long>> receiver) {
                    byte[] sketch = kv.getValue();
                    if (sketch == null) {
                      LOG.warn(
                          "Received a null and treated it as an empty sketch. "
                              + "Consider replacing nulls with empty byte arrays (byte[0]) "
                              + "in upstream transforms for better space-efficiency and safety.");
                      receiver.output(KV.of(kv.getKey(), 0L));
                    } else if (sketch.length == 0) {
                      receiver.output(KV.of(kv.getKey(), 0L));
                    } else {
                      receiver.output(
                          KV.of(kv.getKey(), HyperLogLogPlusPlus.forProto(sketch).result()));
                    }
                  }
                }));
      }
    }
  }
}
