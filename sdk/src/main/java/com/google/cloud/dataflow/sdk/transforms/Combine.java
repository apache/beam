/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * {@code PTransform}s for combining {@code PCollection} elements
 * globally and per-key.
 */
@SuppressWarnings("serial")
public class Combine {

  /**
   * Returns a {@link Globally Combine.Globally} {@code PTransform}
   * that uses the given {@code SerializableFunction} to combine all
   * the elements of the input {@code PCollection} into a singleton
   * {@code PCollection} value.  The types of the input elements and the
   * output value must be the same.
   *
   * <p>If the input {@code PCollection} is empty, the ouput will contain a the
   * default value of the combining function if the input is windowed into
   * the {@link GlobalWindows}; otherwise, the output will be empty.  Note: this
   * behavior is subject to change.
   *
   * <p> See {@link Globally Combine.Globally} for more information.
   */
  public static <V> Globally<V, V> globally(
      SerializableFunction<Iterable<V>, V> combiner) {
    return globally(SimpleCombineFn.of(combiner));
  }

  /**
   * Returns a {@link Globally Combine.Globally} {@code PTransform}
   * that uses the given {@code CombineFn} to combine all the elements
   * of the input {@code PCollection} into a singleton {@code PCollection}
   * value.  The types of the input elements and the output value can
   * differ.
   *
   * If the input {@code PCollection} is empty, the ouput will contain a the
   * default value of the combining function if the input is windowed into
   * the {@link GlobalWindows}; otherwise, the output will be empty.  Note: this
   * behavior is subject to change.
   *
   * <p> See {@link Globally Combine.Globally} for more information.
   */
  public static <VI, VO> Globally<VI, VO> globally(
      CombineFn<? super VI, ?, VO> fn) {
    return new Globally<>(fn);
  }

  /**
   * Returns a {@link PerKey Combine.PerKey} {@code PTransform} that
   * first groups its input {@code PCollection} of {@code KV}s by keys and
   * windows, then invokes the given function on each of the values lists to
   * produce a combined value, and then returns a {@code PCollection}
   * of {@code KV}s mapping each distinct key to its combined value for each
   * window.
   *
   * <p> Each output element is in the window by which its corresponding input
   * was grouped, and has the timestamp of the end of that window.  The output
   * {@code PCollection} has the same
   * {@link com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn}
   * as the input.
   *
   * <p> See {@link PerKey Combine.PerKey} for more information.
   */
  public static <K, V> PerKey<K, V, V> perKey(
      SerializableFunction<Iterable<V>, V> fn) {
    return perKey(Combine.SimpleCombineFn.of(fn));
  }

  /**
   * Returns a {@link PerKey Combine.PerKey} {@code PTransform} that
   * first groups its input {@code PCollection} of {@code KV}s by keys and
   * windows, then invokes the given function on each of the values lists to
   * produce a combined value, and then returns a {@code PCollection}
   * of {@code KV}s mapping each distinct key to its combined value for each
   * window.
   *
   * <p> Each output element is in the window by which its corresponding input
   * was grouped, and has the timestamp of the end of that window.  The output
   * {@code PCollection} has the same
   * {@link com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn}
   * as the input.
   *
   * <p> See {@link PerKey Combine.PerKey} for more information.
   */
  public static <K, VI, VO> PerKey<K, VI, VO> perKey(
      CombineFn<? super VI, ?, VO> fn) {
    return perKey(fn.<K>asKeyedFn());
  }

  /**
   * Returns a {@link PerKey Combine.PerKey} {@code PTransform} that
   * first groups its input {@code PCollection} of {@code KV}s by keys and
   * windows, then invokes the given function on each of the key/values-lists
   * pairs to produce a combined value, and then returns a
   * {@code PCollection} of {@code KV}s mapping each distinct key to
   * its combined value for each window.
   *
   * <p> Each output element is in the window by which its corresponding input
   * was grouped, and has the timestamp of the end of that window.  The output
   * {@code PCollection} has the same
   * {@link com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn}
   * as the input.
   *
   * <p> See {@link PerKey Combine.PerKey} for more information.
   */
  public static <K, VI, VO> PerKey<K, VI, VO> perKey(
      KeyedCombineFn<? super K, ? super VI, ?, VO> fn) {
    return new PerKey<>(fn);
  }

  /**
   * Returns a {@link GroupedValues Combine.GroupedValues}
   * {@code PTransform} that takes a {@code PCollection} of
   * {@code KV}s where a key maps to an {@code Iterable} of values, e.g.,
   * the result of a {@code GroupByKey}, then uses the given
   * {@code SerializableFunction} to combine all the values associated
   * with a key, ignoring the key.  The type of the input and
   * output values must be the same.
   *
   * <p> Each output element has the same timestamp and is in the same window
   * as its corresponding input element, and the output
   * {@code PCollection} has the same
   * {@link com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn}
   * associated with it as the input.
   *
   * <p> See {@link GroupedValues Combine.GroupedValues} for more information.
   *
   * <p> Note that {@link #perKey(SerializableFunction)} is typically
   * more convenient to use than {@link GroupByKey} followed by
   * {@code groupedValues(...)}.
   */
  public static <K, V> GroupedValues<K, V, V> groupedValues(
      SerializableFunction<Iterable<V>, V> fn) {
    return groupedValues(SimpleCombineFn.of(fn));
  }

  /**
   * Returns a {@link GroupedValues Combine.GroupedValues}
   * {@code PTransform} that takes a {@code PCollection} of
   * {@code KV}s where a key maps to an {@code Iterable} of values, e.g.,
   * the result of a {@code GroupByKey}, then uses the given
   * {@code CombineFn} to combine all the values associated with a
   * key, ignoring the key.  The types of the input and output values
   * can differ.
   *
   * <p> Each output element has the same timestamp and is in the same window
   * as its corresponding input element, and the output
   * {@code PCollection} has the same
   * {@link com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn}
   * associated with it as the input.
   *
   * <p> See {@link GroupedValues Combine.GroupedValues} for more information.
   *
   * <p> Note that {@link #perKey(CombineFn)} is typically
   * more convenient to use than {@link GroupByKey} followed by
   * {@code groupedValues(...)}.
   */
  public static <K, VI, VO> GroupedValues<K, VI, VO> groupedValues(
      CombineFn<? super VI, ?, VO> fn) {
    return groupedValues(fn.<K>asKeyedFn());
  }

  /**
   * Returns a {@link GroupedValues Combine.GroupedValues}
   * {@code PTransform} that takes a {@code PCollection} of
   * {@code KV}s where a key maps to an {@code Iterable} of values, e.g.,
   * the result of a {@code GroupByKey}, then uses the given
   * {@code KeyedCombineFn} to combine all the values associated with
   * each key.  The combining function is provided the key.  The types
   * of the input and output values can differ.
   *
   * <p> Each output element has the same timestamp and is in the same window
   * as its corresponding input element, and the output
   * {@code PCollection} has the same
   * {@link com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn}
   * associated with it as the input.
   *
   * <p> See {@link GroupedValues Combine.GroupedValues} for more information.
   *
   * <p> Note that {@link #perKey(KeyedCombineFn)} is typically
   * more convenient to use than {@link GroupByKey} followed by
   * {@code groupedValues(...)}.
   */
  public static <K, VI, VO> GroupedValues<K, VI, VO> groupedValues(
      KeyedCombineFn<? super K, ? super VI, ?, VO> fn) {
    return new GroupedValues<>(fn);
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@code CombineFn<VI, VA, VO>} specifies how to combine a
   * collection of input values of type {@code VI} into a single
   * output value of type {@code VO}.  It does this via one or more
   * intermediate mutable accumulator values of type {@code VA}.
   *
   * <p> The overall process to combine a collection of input
   * {@code VI} values into a single output {@code VO} value is as
   * follows:
   *
   * <ol>
   *
   * <li> The input {@code VI} values are partitioned into one or more
   * batches.
   *
   * <li> For each batch, the {@link #createAccumulator} operation is
   * invoked to create a fresh mutable accumulator value of type
   * {@code VA}, initialized to represent the combination of zero
   * values.
   *
   * <li> For each input {@code VI} value in a batch, the
   * {@link #addInput} operation is invoked to add the value to that
   * batch's accumulator {@code VA} value.  The accumulator may just
   * record the new value (e.g., if {@code VA == List<VI>}, or may do
   * work to represent the combination more compactly.
   *
   * <li> The {@link #mergeAccumulators} operation is invoked to
   * combine a collection of accumulator {@code VA} values into a
   * single combined output accumulator {@code VA} value, once the
   * merging accumulators have had all all the input values in their
   * batches added to them.  This operation is invoked repeatedly,
   * until there is only one accumulator value left.
   *
   * <li> The {@link #extractOutput} operation is invoked on the final
   * accumulator {@code VA} value to get the output {@code VO} value.
   *
   * </ol>
   *
   * <p> For example:
   * <pre> {@code
   * public class AverageFn extends CombineFn<Integer, AverageFn.Accum, Double> {
   *   public static class Accum {
   *     int sum = 0;
   *     int count = 0;
   *   }
   *   public Accum createAccumulator() { return new Accum(); }
   *   public void addInput(Accum accum, Integer input) {
   *       accum.sum += input;
   *       accum.count++;
   *   }
   *   public Accum mergeAccumulators(Iterable<Accum> accums) {
   *     Accum merged = createAccumulator();
   *     for (Accum accum : accums) {
   *       merged.sum += accum.sum;
   *       merged.count += accum.count;
   *     }
   *     return merged;
   *   }
   *   public Double extractOutput(Accum accum) {
   *     return ((double) accum.sum) / accum.count;
   *   }
   * }
   * PCollection<Integer> pc = ...;
   * PCollection<Double> average = pc.apply(Combine.globally(new AverageFn()));
   * } </pre>
   *
   * <p> Combining functions used by {@link Combine.Globally},
   * {@link Combine.PerKey}, {@link Combine.GroupedValues}, and
   * {@code PTransforms} derived from them should be
   * <i>associative</i> and <i>commutative</i>.  Associativity is
   * required because input values are first broken up into subgroups
   * before being combined, and their intermediate results further
   * combined, in an arbitrary tree structure.  Commutativity is
   * required because any order of the input values is ignored when
   * breaking up input values into groups.
   *
   * @param <VI> type of input values
   * @param <VA> type of mutable accumulator values
   * @param <VO> type of output values
   */
  public abstract static class CombineFn<VI, VA, VO> implements Serializable {
    /**
     * Returns a new, mutable accumulator value, representing the
     * accumulation of zero input values.
     */
    public abstract VA createAccumulator();

    /**
     * Adds the given input value to the given accumulator,
     * modifying the accumulator.
     */
    public abstract void addInput(VA accumulator, VI input);

    /**
     * Returns an accumulator representing the accumulation of all the
     * input values accumulated in the merging accumulators.
     *
     * <p> May modify any of the argument accumulators.  May return a
     * fresh accumulator, or may return one of the (modified) argument
     * accumulators.
     */
    public abstract VA mergeAccumulators(Iterable<VA> accumulators);

    /**
     * Returns the output value that is the result of combining all
     * the input values represented by the given accumulator.
     */
    public abstract VO extractOutput(VA accumulator);

    /**
     * Applies this {@code CombineFn} to a collection of input values
     * to produce a combined output value.
     *
     * <p> Useful when testing the behavior of a {@code CombineFn}
     * separately from a {@code Combine} transform.
     */
    public VO apply(Iterable<? extends VI> inputs) {
      VA accum = createAccumulator();
      for (VI input : inputs) {
        addInput(accum, input);
      }
      return extractOutput(accum);
    }

    /**
     * Returns the {@code Coder} to use for accumulator {@code VA}
     * values, or null if it is not able to be inferred.
     *
     * <p> By default, uses the knowledge of the {@code Coder} being used
     * for {@code VI} values and the enclosing {@code Pipeline}'s
     * {@code CoderRegistry} to try to infer the Coder for {@code VA}
     * values.
     *
     * <p> This is the Coder used to send data through a communication-intensive
     * shuffle step, so a compact and efficient representation may have
     * significant performance benefits.
     */
    public Coder<VA> getAccumulatorCoder(
        CoderRegistry registry, Coder<VI> inputCoder) {
      return registry.getDefaultCoder(
          getClass(),
          CombineFn.class,
          ImmutableMap.of("VI", inputCoder),
          "VA");
    }

    /**
     * Returns the {@code Coder} to use by default for output
     * {@code VO} values, or null if it is not able to be inferred.
     *
     * <p> By default, uses the knowledge of the {@code Coder} being
     * used for input {@code VI} values and the enclosing
     * {@code Pipeline}'s {@code CoderRegistry} to try to infer the
     * Coder for {@code VO} values.
     */
    public Coder<VO> getDefaultOutputCoder(
        CoderRegistry registry, Coder<VI> inputCoder) {
      return registry.getDefaultCoder(
          getClass(),
          CombineFn.class,
          ImmutableMap.of("VI", inputCoder,
                          "VA", getAccumulatorCoder(registry, inputCoder)),
          "VO");
    }

    /**
     * Converts this {@code CombineFn} into an equivalent
     * {@link KeyedCombineFn}, which ignores the keys passed to it and
     * combines the values according to this {@code CombineFn}.
     *
     * @param <K> the type of the (ignored) keys
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <K> KeyedCombineFn<K, VI, VA, VO> asKeyedFn() {
      // The key, an object, is never even looked at.
      return new KeyedCombineFn<K, VI, VA, VO>() {
        @Override
        public VA createAccumulator(K key) {
          return CombineFn.this.createAccumulator();
        }

        @Override
        public void addInput(K key, VA accumulator, VI input) {
          CombineFn.this.addInput(accumulator, input);
        }

        @Override
        public VA mergeAccumulators(K key, Iterable<VA> accumulators) {
          return CombineFn.this.mergeAccumulators(accumulators);
        }

        @Override
        public VO extractOutput(K key, VA accumulator) {
          return CombineFn.this.extractOutput(accumulator);
        }

        @Override
        public Coder<VA> getAccumulatorCoder(
            CoderRegistry registry, Coder<K> keyCoder, Coder<VI> inputCoder) {
          return CombineFn.this.getAccumulatorCoder(registry, inputCoder);
        }

        @Override
        public Coder<VO> getDefaultOutputCoder(
            CoderRegistry registry, Coder<K> keyCoder, Coder<VI> inputCoder) {
          return CombineFn.this.getDefaultOutputCoder(registry, inputCoder);
        }
      };
    }
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@code CombineFn} that uses a subclass of
   * {@link AccumulatingCombineFn.Accumulator} as its accumulator
   * type.  By defining the operations of the {@code Accumulator}
   * helper class, the operations of the enclosing {@code CombineFn}
   * are automatically provided.  This can reduce the code required to
   * implement a {@code CombineFn}.
   *
   * <p> For example, the example from {@link CombineFn} above can be
   * expressed using {@code AccumulatingCombineFn} more concisely as
   * follows:
   *
   * <pre> {@code
   * public class AverageFn
   *     extends AccumulatingCombineFn<Integer, AverageFn.Accum, Double> {
   *   public Accum createAccumulator() { return new Accum(); }
   *   public class Accum
   *       extends AccumulatingCombineFn<Integer, AverageFn.Accum, Double>
   *               .Accumulator {
   *     private int sum = 0;
   *     private int count = 0;
   *     public void addInput(Integer input) {
   *       sum += input;
   *       count++;
   *     }
   *     public void mergeAccumulator(Accum other) {
   *       sum += other.sum;
   *       count += other.count;
   *     }
   *     public Double extractOutput() {
   *       return ((double) sum) / count;
   *     }
   *   }
   * }
   * PCollection<Integer> pc = ...;
   * PCollection<Double> average = pc.apply(Combine.globally(new AverageFn()));
   * } </pre>
   *
   * @param <VI> type of input values
   * @param <VA> type of mutable accumulator values
   * @param <VO> type of output values
   */
  public abstract static class AccumulatingCombineFn
      <VI, VA extends AccumulatingCombineFn.Accumulator<VI, VA, VO>, VO>
      extends CombineFn<VI, VA, VO> {

    /**
     * The type of mutable accumulator values used by this
     * {@code AccumulatingCombineFn}.
     */
    public abstract static interface Accumulator<VI, VA, VO> {
      /**
       * Adds the given input value to this accumulator, modifying
       * this accumulator.
       */
      public abstract void addInput(VI input);

      /**
       * Adds the input values represented by the given accumulator
       * into this accumulator.
       */
      public abstract void mergeAccumulator(VA other);

      /**
       * Returns the output value that is the result of combining all
       * the input values represented by this accumulator.
       */
      public abstract VO extractOutput();
    }

    @Override
    public final void addInput(VA accumulator, VI input) {
      accumulator.addInput(input);
    }

    @Override
    public final VA mergeAccumulators(Iterable<VA> accumulators) {
      VA accumulator = createAccumulator();
      for (VA partial : accumulators) {
        accumulator.mergeAccumulator(partial);
      }
      return accumulator;
    }

    @Override
    public final VO extractOutput(VA accumulator) {
      return accumulator.extractOutput();
    }
  }


  /////////////////////////////////////////////////////////////////////////////


  /**
   * A {@code KeyedCombineFn<K, VI, VA, VO>} specifies how to combine
   * a collection of input values of type {@code VI}, associated with
   * a key of type {@code K}, into a single output value of type
   * {@code VO}.  It does this via one or more intermediate mutable
   * accumulator values of type {@code VA}.
   *
   * <p> The overall process to combine a collection of input
   * {@code VI} values associated with an input {@code K} key into a
   * single output {@code VO} value is as follows:
   *
   * <ol>
   *
   * <li> The input {@code VI} values are partitioned into one or more
   * batches.
   *
   * <li> For each batch, the {@link #createAccumulator} operation is
   * invoked to create a fresh mutable accumulator value of type
   * {@code VA}, initialized to represent the combination of zero
   * values.
   *
   * <li> For each input {@code VI} value in a batch, the
   * {@link #addInput} operation is invoked to add the value to that
   * batch's accumulator {@code VA} value.  The accumulator may just
   * record the new value (e.g., if {@code VA == List<VI>}, or may do
   * work to represent the combination more compactly.
   *
   * <li> The {@link #mergeAccumulators} operation is invoked to
   * combine a collection of accumulator {@code VA} values into a
   * single combined output accumulator {@code VA} value, once the
   * merging accumulators have had all all the input values in their
   * batches added to them.  This operation is invoked repeatedly,
   * until there is only one accumulator value left.
   *
   * <li> The {@link #extractOutput} operation is invoked on the final
   * accumulator {@code VA} value to get the output {@code VO} value.
   *
   * </ol>
   *
   * All of these operations are passed the {@code K} key that the
   * values being combined are associated with.
   *
   * <p> For example:
   * <pre> {@code
   * public class ConcatFn
   *     extends KeyedCombineFn<String, Integer, ConcatFn.Accum, String> {
   *   public static class Accum {
   *     String s = "";
   *   }
   *   public Accum createAccumulator(String key) { return new Accum(); }
   *   public void addInput(String key, Accum accum, Integer input) {
   *       accum.s += "+" + input;
   *   }
   *   public Accum mergeAccumulators(String key, Iterable<Accum> accums) {
   *     Accum merged = new Accum();
   *     for (Accum accum : accums) {
   *       merged.s += accum.s;
   *     }
   *     return merged;
   *   }
   *   public String extractOutput(String key, Accum accum) {
   *     return key + accum.s;
   *   }
   * }
   * PCollection<KV<String, Integer>> pc = ...;
   * PCollection<KV<String, String>> pc2 = pc.apply(
   *     Combine.perKey(new ConcatFn()));
   * } </pre>
   *
   * <p> Keyed combining functions used by {@link Combine.PerKey},
   * {@link Combine.GroupedValues}, and {@code PTransforms} derived
   * from them should be <i>associative</i> and <i>commutative</i>.
   * Associativity is required because input values are first broken
   * up into subgroups before being combined, and their intermediate
   * results further combined, in an arbitrary tree structure.
   * Commutativity is required because any order of the input values
   * is ignored when breaking up input values into groups.
   *
   * @param <K> type of keys
   * @param <VI> type of input values
   * @param <VA> type of mutable accumulator values
   * @param <VO> type of output values
   */
  public abstract static class KeyedCombineFn<K, VI, VA, VO>
      implements Serializable {
    /**
     * Returns a new, mutable accumulator value representing the
     * accumulation of zero input values.
     *
     * @param key the key that all the accumulated values using the
     * accumulator are associated with
     */
    public abstract VA createAccumulator(K key);

    /**
     * Adds the given input value to the given accumulator,
     * modifying the accumulator.
     *
     * @param key the key that all the accumulated values using the
     * accumulator are associated with
     */
    public abstract void addInput(K key, VA accumulator, VI value);

    /**
     * Returns an accumulator representing the accumulation of all the
     * input values accumulated in the merging accumulators.
     *
     * <p> May modify any of the argument accumulators.  May return a
     * fresh accumulator, or may return one of the (modified) argument
     * accumulators.
     *
     * @param key the key that all the accumulators are associated
     * with
     */
    public abstract VA mergeAccumulators(K key, Iterable<VA> accumulators);

    /**
     * Returns the output value that is the result of combining all
     * the input values represented by the given accumulator.
     *
     * @param key the key that all the accumulated values using the
     * accumulator are associated with
     */
    public abstract VO extractOutput(K key, VA accumulator);

    /**
     * Applies this {@code KeyedCombineFn} to a key and a collection
     * of input values to produce a combined output value.
     *
     * <p> Useful when testing the behavior of a {@code KeyedCombineFn}
     * separately from a {@code Combine} transform.
     */
    public VO apply(K key, Iterable<? extends VI> inputs) {
      VA accum = createAccumulator(key);
      for (VI input : inputs) {
        addInput(key, accum, input);
      }
      return extractOutput(key, accum);
    }

    /**
     * Returns the {@code Coder} to use for accumulator {@code VA}
     * values, or null if it is not able to be inferred.
     *
     * <p> By default, uses the knowledge of the {@code Coder} being
     * used for {@code K} keys and input {@code VI} values and the
     * enclosing {@code Pipeline}'s {@code CoderRegistry} to try to
     * infer the Coder for {@code VA} values.
     *
     * <p> This is the Coder used to send data through a communication-intensive
     * shuffle step, so a compact and efficient representation may have
     * significant performance benefits.
     */
    public Coder<VA> getAccumulatorCoder(
        CoderRegistry registry, Coder<K> keyCoder, Coder<VI> inputCoder) {
      return registry.getDefaultCoder(
          getClass(),
          KeyedCombineFn.class,
          ImmutableMap.of("K", keyCoder, "VI", inputCoder),
          "VA");
    }

    /**
     * Returns the {@code Coder} to use by default for output
     * {@code VO} values, or null if it is not able to be inferred.
     *
     * <p> By default, uses the knowledge of the {@code Coder} being
     * used for {@code K} keys and input {@code VI} values and the
     * enclosing {@code Pipeline}'s {@code CoderRegistry} to try to
     * infer the Coder for {@code VO} values.
     */
    public Coder<VO> getDefaultOutputCoder(
        CoderRegistry registry, Coder<K> keyCoder, Coder<VI> inputCoder) {
      return registry.getDefaultCoder(
          getClass(),
          KeyedCombineFn.class,
          ImmutableMap.of(
              "K", keyCoder,
              "VI", inputCoder,
              "VA", getAccumulatorCoder(registry, keyCoder, inputCoder)),
          "VO");
    }
  }


  ////////////////////////////////////////////////////////////////////////////

  /**
   * {@code Combine.Globally<VI, VO>} takes a {@code PCollection<VI>}
   * and returns a {@code PCollection<VO>} whose single element is the result of
   * combining all the elements of the input {@code PCollection},
   * using a specified
   * {@link CombineFn CombineFn<VI, VA, VO>}.  It is common
   * for {@code VI == VO}, but not required.  Common combining
   * functions include sums, mins, maxes, and averages of numbers,
   * conjunctions and disjunctions of booleans, statistical
   * aggregations, etc.
   *
   * <p> Example of use:
   * <pre> {@code
   * PCollection<Integer> pc = ...;
   * PCollection<Integer> sum = pc.apply(
   *     Combine.globally(new Sum.SumIntegerFn()));
   * } </pre>
   *
   * <p> Combining can happen in parallel, with different subsets of the
   * input {@code PCollection} being combined separately, and their
   * intermediate results combined further, in an arbitrary tree
   * reduction pattern, until a single result value is produced.
   *
   * <p> By default, the {@code Coder} of the output {@code PValue<VO>}
   * is inferred from the concrete type of the
   * {@code CombineFn<VI, VA, VO>}'s output type {@code VO}.
   *
   * <p> See also {@link #perKey}/{@link PerKey Combine.PerKey} and
   * {@link #groupedValues}/{@link GroupedValues Combine.GroupedValues},
   * which are useful for combining values associated with each key in
   * a {@code PCollection} of {@code KV}s.
   *
   * @param <VI> type of input values
   * @param <VO> type of output values
   */
  public static class Globally<VI, VO>
      extends PTransform<PCollection<VI>, PCollection<VO>> {

    private final CombineFn<? super VI, ?, VO> fn;

    private Globally(CombineFn<? super VI, ?, VO> fn) {
      this.fn = fn;
    }

    @Override
    public PCollection<VO> apply(PCollection<VI> input) {
      PCollection<VO> output = input
          .apply(WithKeys.<Void, VI>of((Void) null))
          .setCoder(KvCoder.of(VoidCoder.of(), input.getCoder()))
          .apply(Combine.<Void, VI, VO>perKey(fn.<Void>asKeyedFn()))
          .apply(Values.<VO>create());

      if (input.getWindowFn().isCompatible(new GlobalWindows())) {
        return insertDefaultValueIfEmpty(output);
      } else {
        return output;
      }
    }

    private PCollection<VO> insertDefaultValueIfEmpty(PCollection<VO> maybeEmpty) {
      final PCollectionView<Iterable<VO>, ?> maybeEmptyView = maybeEmpty.apply(
          View.<VO>asIterable());
      return maybeEmpty.getPipeline()
          .apply(Create.of((Void) null)).setCoder(VoidCoder.of())
          .apply(ParDo.of(
              new DoFn<Void, VO>() {
                  @Override
                  public void processElement(DoFn<Void, VO>.ProcessContext c) {
                    Iterator<VO> combined = c.sideInput(maybeEmptyView).iterator();
                    if (combined.hasNext()) {
                      c.output(combined.next());
                    } else {
                      c.output(fn.apply(Collections.<VI>emptyList()));
                    }
                  }
              }).withSideInputs(maybeEmptyView))
          .setCoder(maybeEmpty.getCoder());
    }

    @Override
    protected String getKindString() {
      return "Combine.Globally";
    }
  }

  /**
   * Converts a {@link SerializableFunction} from {@code Iterable<V>}s
   * to {@code V}s into a simple {@link CombineFn} over {@code V}s.
   *
   * <p> Used in the implementation of convenience methods like
   * {@link #globally(SerializableFunction)},
   * {@link #perKey(SerializableFunction)}, and
   * {@link #groupedValues(SerializableFunction)}.
   */
  static class SimpleCombineFn<V> extends CombineFn<V, List<V>, V> {
    /**
     * Returns a {@code CombineFn} that uses the given
     * {@code SerializableFunction} to combine values.
     */
    public static <V> SimpleCombineFn<V> of(
        SerializableFunction<Iterable<V>, V> combiner) {
      return new SimpleCombineFn<>(combiner);
    }

    /**
     * The number of values to accumulate before invoking the combiner
     * function to combine them.
     */
    private static final int BUFFER_SIZE = 20;

    /** The combiner function. */
    private final SerializableFunction<Iterable<V>, V> combiner;

    private SimpleCombineFn(SerializableFunction<Iterable<V>, V> combiner) {
      this.combiner = combiner;
    }

    @Override
    public List<V> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public void addInput(List<V> accumulator, V input) {
      accumulator.add(input);
      if (accumulator.size() > BUFFER_SIZE) {
        V combined = combiner.apply(accumulator);
        accumulator.clear();
        accumulator.add(combined);
      }
    }

    @Override
    public List<V> mergeAccumulators(Iterable<List<V>> accumulators) {
      List<V> singleton = new ArrayList<>();
      singleton.add(combiner.apply(Iterables.concat(accumulators)));
      return singleton;
    }

    @Override
    public V extractOutput(List<V> accumulator) {
      return combiner.apply(accumulator);
    }
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * {@code PerKey<K, VI, VO>} takes a
   * {@code PCollection<KV<K, VI>>}, groups it by key, applies a
   * combining function to the {@code VI} values associated with each
   * key to produce a combined {@code VO} value, and returns a
   * {@code PCollection<KV<K, VO>>} representing a map from each
   * distinct key of the input {@code PCollection} to the corresponding
   * combined value.  {@code VI} and {@code VO} are often the same.
   *
   * <p> This is a concise shorthand for an application of
   * {@link GroupByKey} followed by an application of
   * {@link GroupedValues Combine.GroupedValues}.  See those
   * operations for more details on how keys are compared for equality
   * and on the default {@code Coder} for the output.
   *
   * <p> Example of use:
   * <pre> {@code
   * PCollection<KV<String, Double>> salesRecords = ...;
   * PCollection<KV<String, Double>> totalSalesPerPerson =
   *     salesRecords.apply(Combine.<String, Double>perKey(
   *         new Sum.SumDoubleFn()));
   * } </pre>
   *
   * <p> Each output element is in the window by which its corresponding input
   * was grouped, and has the timestamp of the end of that window.  The output
   * {@code PCollection} has the same
   * {@link com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn}
   * as the input.
   *
   * @param <K> the type of the keys of the input and output
   * {@code PCollection}s
   * @param <VI> the type of the values of the input {@code PCollection}
   * @param <VO> the type of the values of the output {@code PCollection}
   */
  public static class PerKey<K, VI, VO>
    extends PTransform<PCollection<KV<K, VI>>, PCollection<KV<K, VO>>> {

    private final transient KeyedCombineFn<? super K, ? super VI, ?, VO> fn;

    private PerKey(
        KeyedCombineFn<? super K, ? super VI, ?, VO> fn) {
      this.fn = fn;
    }

    @Override
    public PCollection<KV<K, VO>> apply(PCollection<KV<K, VI>> input) {
      return input
        .apply(GroupByKey.<K, VI>create())
        .apply(Combine.<K, VI, VO>groupedValues(fn));
    }

    @Override
    protected String getKindString() {
      return "Combine.PerKey";
    }
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * {@code GroupedValues<K, VI, VO>} takes a
   * {@code PCollection<KV<K, Iterable<VI>>>}, such as the result of
   * {@link GroupByKey}, applies a specified
   * {@link KeyedCombineFn KeyedCombineFn<K, VI, VA, VO>}
   * to each of the input {@code KV<K, Iterable<VI>>} elements to
   * produce a combined output {@code KV<K, VO>} element, and returns a
   * {@code PCollection<KV<K, VO>>} containing all the combined output
   * elements.  It is common for {@code VI == VO}, but not required.
   * Common combining functions include sums, mins, maxes, and averages
   * of numbers, conjunctions and disjunctions of booleans, statistical
   * aggregations, etc.
   *
   * <p> Example of use:
   * <pre> {@code
   * PCollection<KV<String, Integer>> pc = ...;
   * PCollection<KV<String, Iterable<Integer>>> groupedByKey = pc.apply(
   *     new GroupByKey<String, Integer>());
   * PCollection<KV<String, Integer>> sumByKey = groupedByKey.apply(
   *     Combine.<String, Integer>groupedValues(
   *         new Sum.SumIntegerFn()));
   * } </pre>
   *
   * <p> See also {@link #perKey}/{@link PerKey Combine.PerKey}
   * which captures the common pattern of "combining by key" in a
   * single easy-to-use {@code PTransform}.
   *
   * <p> Combining for different keys can happen in parallel.  Moreover,
   * combining of the {@code Iterable<VI>} values associated a single
   * key can happen in parallel, with different subsets of the values
   * being combined separately, and their intermediate results combined
   * further, in an arbitrary tree reduction pattern, until a single
   * result value is produced for each key.
   *
   * <p> By default, the {@code Coder} of the keys of the output
   * {@code PCollection<KV<K, VO>>} is that of the keys of the input
   * {@code PCollection<KV<K, VI>>}, and the {@code Coder} of the values
   * of the output {@code PCollection<KV<K, VO>>} is inferred from the
   * concrete type of the {@code KeyedCombineFn<K, VI, VA, VO>}'s output
   * type {@code VO}.
   *
   * <p> Each output element has the same timestamp and is in the same window
   * as its corresponding input element, and the output
   * {@code PCollection} has the same
   * {@link com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn}
   * associated with it as the input.
   *
   * <p> See also {@link #globally}/{@link Globally Combine.Globally},
   * which combines all the values in a {@code PCollection} into a
   * single value in a {@code PCollection}.
   *
   * @param <K> type of input and output keys
   * @param <VI> type of input values
   * @param <VO> type of output values
   */
  public static class GroupedValues<K, VI, VO>
      extends PTransform
                        <PCollection<? extends KV<K, ? extends Iterable<VI>>>,
                         PCollection<KV<K, VO>>> {

    private final KeyedCombineFn<? super K, ? super VI, ?, VO> fn;

    private GroupedValues(KeyedCombineFn<? super K, ? super VI, ?, VO> fn) {
      this.fn = fn;
    }

    /**
     * Returns the KeyedCombineFn used by this Combine operation.
     */
    public KeyedCombineFn<? super K, ? super VI, ?, VO> getFn() {
      return fn;
    }

    @Override
    public PCollection<KV<K, VO>> apply(
        PCollection<? extends KV<K, ? extends Iterable<VI>>> input) {
      Coder<KV<K, VO>> outputCoder = getDefaultOutputCoder();
      return input.apply(ParDo.of(
          new DoFn<KV<K, ? extends Iterable<VI>>, KV<K, VO>>() {
            @Override
            public void processElement(ProcessContext c) {
              K key = c.element().getKey();
              c.output(KV.of(key, fn.apply(key, c.element().getValue())));
            }
          })).setCoder(outputCoder);
    }

    private KvCoder<K, VI> getKvCoder() {
      Coder<? extends KV<K, ? extends Iterable<VI>>> inputCoder =
          getInput().getCoder();
      if (!(inputCoder instanceof KvCoder)) {
        throw new IllegalStateException(
            "Combine.GroupedValues requires its input to use KvCoder");
      }
      @SuppressWarnings({"unchecked", "rawtypes"})
      KvCoder<K, ? extends Iterable<VI>> kvCoder = (KvCoder) inputCoder;
      Coder<K> keyCoder = kvCoder.getKeyCoder();
      Coder<? extends Iterable<VI>> kvValueCoder = kvCoder.getValueCoder();
      if (!(kvValueCoder instanceof IterableCoder)) {
        throw new IllegalStateException(
            "Combine.GroupedValues requires its input values to use "
            + "IterableCoder");
      }
      @SuppressWarnings("unchecked")
      IterableCoder<VI> inputValuesCoder = (IterableCoder<VI>) kvValueCoder;
      Coder<VI> inputValueCoder = inputValuesCoder.getElemCoder();
      return KvCoder.of(keyCoder, inputValueCoder);
    }

    @SuppressWarnings("unchecked")
    public Coder<?> getAccumulatorCoder() {
      KvCoder<K, VI> kvCoder = getKvCoder();
      return ((KeyedCombineFn<K, VI, ?, VO>) fn).getAccumulatorCoder(
          getCoderRegistry(), kvCoder.getKeyCoder(), kvCoder.getValueCoder());
    }

    @Override
    public Coder<KV<K, VO>> getDefaultOutputCoder() {
      KvCoder<K, VI> kvCoder = getKvCoder();
      @SuppressWarnings("unchecked")
      Coder<VO> outputValueCoder = ((KeyedCombineFn<K, VI, ?, VO>) fn)
          .getDefaultOutputCoder(
              getCoderRegistry(), kvCoder.getKeyCoder(), kvCoder.getValueCoder());
      return KvCoder.of(kvCoder.getKeyCoder(), outputValueCoder);
    }
  }
}
