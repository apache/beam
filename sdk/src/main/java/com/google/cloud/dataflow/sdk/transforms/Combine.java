/*
 * Copyright (C) 2015 Google Inc.
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

import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.CustomCoder;
import com.google.cloud.dataflow.sdk.coders.DelegateCoder;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.util.common.CounterProvider;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * {@code PTransform}s for combining {@code PCollection} elements
 * globally and per-key.
 */
@SuppressWarnings("serial")
public class Combine {

  /**
   * Returns a {@link Globally Combine.Globally} {@code PTransform}
   * that uses the given {@code SerializableFunction} to combine all
   * the elements in each window of the input {@code PCollection} into a
   * single value in the output {@code PCollection}.  The types of the input
   * elements and the output elements must be the same.
   *
   * <p> If the input {@code PCollection} is windowed into {@link GlobalWindows},
   * a default value in the {@link GlobalWindow} will be output if the input
   * {@code PCollection} is empty.  To use this with inputs with other windowing,
   * either {@link Globally#withoutDefaults} or {@link Globally#asSingletonView}
   * must be called.
   *
   * <p> See {@link Globally Combine.Globally} for more information.
   */
  public static <V> Globally<V, V> globally(
      SerializableFunction<Iterable<V>, V> combiner) {
    return globally(SimpleCombineFn.of(combiner));
  }

  /**
   * Returns a {@link Globally Combine.Globally} {@code PTransform}
   * that uses the given {@code SerializableFunction} to combine all
   * the elements in each window of the input {@code PCollection} into a
   * single value in the output {@code PCollection}.  The types of the input
   * elements and the output elements can differ
   *
   * <p> If the input {@code PCollection} is windowed into {@link GlobalWindows},
   * a default value in the {@link GlobalWindow} will be output if the input
   * {@code PCollection} is empty.  To use this with inputs with other windowing,
   * either {@link Globally#withoutDefaults} or {@link Globally#asSingletonView}
   * must be called.
   *
   * <p> See {@link Globally Combine.Globally} for more information.
   */
  public static <InputT, OutputT> Globally<InputT, OutputT> globally(
      CombineFn<? super InputT, ?, OutputT> fn) {
    return new Globally<>("Globally", fn, true, 0);
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
  public static <K, InputT, OutputT> PerKey<K, InputT, OutputT> perKey(
      CombineFn<? super InputT, ?, OutputT> fn) {
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
  public static <K, InputT, OutputT> PerKey<K, InputT, OutputT> perKey(
      KeyedCombineFn<? super K, ? super InputT, ?, OutputT> fn) {
    return new PerKey<>("PerKey", fn, false /*fewKeys*/);
  }

  /**
   * Returns a {@link PerKey Combine.PerKey}, and set fewKeys
   * in {@link GroupByKey}.
   */
  private static <K, InputT, OutputT> PerKey<K, InputT, OutputT> fewKeys(
      KeyedCombineFn<? super K, ? super InputT, ?, OutputT> fn) {
    return new PerKey<>("PerKey", fn, true /*fewKeys*/);
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
  public static <K, InputT, OutputT> GroupedValues<K, InputT, OutputT> groupedValues(
      CombineFn<? super InputT, ?, OutputT> fn) {
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
  public static <K, InputT, OutputT> GroupedValues<K, InputT, OutputT> groupedValues(
      KeyedCombineFn<? super K, ? super InputT, ?, OutputT> fn) {
    return new GroupedValues<>(fn);
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@code CombineFn<InputT, AccumT, OutputT>} specifies how to combine a
   * collection of input values of type {@code InputT} into a single
   * output value of type {@code OutputT}.  It does this via one or more
   * intermediate mutable accumulator values of type {@code AccumT}.
   *
   * <p> The overall process to combine a collection of input
   * {@code InputT} values into a single output {@code OutputT} value is as
   * follows:
   *
   * <ol>
   *
   * <li> The input {@code InputT} values are partitioned into one or more
   * batches.
   *
   * <li> For each batch, the {@link #createAccumulator} operation is
   * invoked to create a fresh mutable accumulator value of type
   * {@code AccumT}, initialized to represent the combination of zero
   * values.
   *
   * <li> For each input {@code InputT} value in a batch, the
   * {@link #addInput} operation is invoked to add the value to that
   * batch's accumulator {@code AccumT} value.  The accumulator may just
   * record the new value (e.g., if {@code AccumT == List<InputT>}, or may do
   * work to represent the combination more compactly.
   *
   * <li> The {@link #mergeAccumulators} operation is invoked to
   * combine a collection of accumulator {@code AccumT} values into a
   * single combined output accumulator {@code AccumT} value, once the
   * merging accumulators have had all all the input values in their
   * batches added to them.  This operation is invoked repeatedly,
   * until there is only one accumulator value left.
   *
   * <li> The {@link #extractOutput} operation is invoked on the final
   * accumulator {@code AccumT} value to get the output {@code OutputT} value.
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
   *   public Accum createAccumulator() {
   *     return new Accum();
   *   }
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
   * @param <InputT> type of input values
   * @param <AccumT> type of mutable accumulator values
   * @param <OutputT> type of output values
   */
  public abstract static class CombineFn<InputT, AccumT, OutputT> implements Serializable {
    /**
     * Returns a new, mutable accumulator value, representing the
     * accumulation of zero input values.
     */
    public abstract AccumT createAccumulator();

    /**
     * Adds the given input value to the given accumulator, returning the
     * new accumulator value.
     *
     * <P> For efficiency, the input accumulator may be modified and returned.
     */
    public abstract AccumT addInput(AccumT accumulator, InputT input);

    /**
     * Returns an accumulator representing the accumulation of all the
     * input values accumulated in the merging accumulators.
     *
     * <p> May modify any of the argument accumulators.  May return a
     * fresh accumulator, or may return one of the (modified) argument
     * accumulators.
     */
    public abstract AccumT mergeAccumulators(Iterable<AccumT> accumulators);

    /**
     * Returns the output value that is the result of combining all
     * the input values represented by the given accumulator.
     */
    public abstract OutputT extractOutput(AccumT accumulator);

    /**
     * Applies this {@code CombineFn} to a collection of input values
     * to produce a combined output value.
     *
     * <p> Useful when testing the behavior of a {@code CombineFn}
     * separately from a {@code Combine} transform.
     */
    public OutputT apply(Iterable<? extends InputT> inputs) {
      AccumT accum = createAccumulator();
      for (InputT input : inputs) {
        accum = addInput(accum, input);
      }
      return extractOutput(accum);
    }

    /**
     * Returns a {@link TypeDescriptor} capturing what is known statically
     * about the output type of this {@code CombineFn} instance's
     * most-derived class.
     *
     * <p> In the normal case of a concrete {@code CombineFn} subclass with
     * no generic type parameters of its own, this will be a complete
     * non-generic type.
     */
    public TypeDescriptor<OutputT> getOutputType() {
      return new TypeDescriptor<OutputT>(getClass()) {};
    }

    @SuppressWarnings("unchecked")
    private TypeVariable<Class<CombineFn<?, ?, ?>>> getInputTVariable() {
      return (TypeVariable<Class<CombineFn<?, ?, ?>>>)
          new TypeDescriptor<InputT>(CombineFn.class) {}
          .getType();
    }

    @SuppressWarnings("unchecked")
    private TypeVariable<Class<CombineFn<?, ?, ?>>> getAccumTVariable() {
      return (TypeVariable<Class<CombineFn<?, ?, ?>>>)
          new TypeDescriptor<AccumT>(CombineFn.class) {}
          .getType();
    }

    @SuppressWarnings("unchecked")
    private TypeVariable<Class<CombineFn<?, ?, ?>>> getOutputTVariable() {
      return (TypeVariable<Class<CombineFn<?, ?, ?>>>)
          new TypeDescriptor<OutputT>(CombineFn.class) {}
          .getType();
    }

    /**
     * Returns the {@code Coder} to use for accumulator {@code AccumT}
     * values, or null if it is not able to be inferred.
     *
     * <p> By default, uses the knowledge of the {@code Coder} being used
     * for {@code InputT} values and the enclosing {@code Pipeline}'s
     * {@code CoderRegistry} to try to infer the Coder for {@code AccumT}
     * values.
     *
     * <p> This is the Coder used to send data through a communication-intensive
     * shuffle step, so a compact and efficient representation may have
     * significant performance benefits.
     */
    public Coder<AccumT> getAccumulatorCoder(
        CoderRegistry registry, Coder<InputT> inputCoder) throws CannotProvideCoderException {
      return registry.getDefaultCoder(getClass(), CombineFn.class,
          ImmutableMap.<Type, Coder<?>>of(getInputTVariable(), inputCoder),
          getAccumTVariable());
    }

    /**
     * Returns the {@code Coder} to use by default for output
     * {@code OutputT} values, or null if it is not able to be inferred.
     *
     * <p> By default, uses the knowledge of the {@code Coder} being
     * used for input {@code InputT} values and the enclosing
     * {@code Pipeline}'s {@code CoderRegistry} to try to infer the
     * Coder for {@code OutputT} values.
     */
    public Coder<OutputT> getDefaultOutputCoder(
        CoderRegistry registry, Coder<InputT> inputCoder) throws CannotProvideCoderException {
      return registry.getDefaultCoder(getClass(), CombineFn.class,
          ImmutableMap.<Type, Coder<?>>of(
              getInputTVariable(), inputCoder,
              getAccumTVariable(), getAccumulatorCoder(registry, inputCoder)),
          getOutputTVariable());
    }

    /**
     * Converts this {@code CombineFn} into an equivalent
     * {@link KeyedCombineFn} that ignores the keys passed to it and
     * combines the values according to this {@code CombineFn}.
     *
     * @param <K> the type of the (ignored) keys
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <K> KeyedCombineFn<K, InputT, AccumT, OutputT> asKeyedFn() {
      // The key, an object, is never even looked at.
      return new KeyedCombineFn<K, InputT, AccumT, OutputT>() {
        @Override
        public AccumT createAccumulator(K key) {
          return CombineFn.this.createAccumulator();
        }

        @Override
        public AccumT addInput(K key, AccumT accumulator, InputT input) {
          return CombineFn.this.addInput(accumulator, input);
        }

        @Override
        public AccumT mergeAccumulators(K key, Iterable<AccumT> accumulators) {
          return CombineFn.this.mergeAccumulators(accumulators);
        }

        @Override
        public OutputT extractOutput(K key, AccumT accumulator) {
          return CombineFn.this.extractOutput(accumulator);
        }

        @Override
        public Coder<AccumT> getAccumulatorCoder(
            CoderRegistry registry, Coder<K> keyCoder, Coder<InputT> inputCoder)
            throws CannotProvideCoderException {
          return CombineFn.this.getAccumulatorCoder(registry, inputCoder);
        }

        @Override
        public Coder<OutputT> getDefaultOutputCoder(
            CoderRegistry registry, Coder<K> keyCoder, Coder<InputT> inputCoder)
            throws CannotProvideCoderException {
          return CombineFn.this.getDefaultOutputCoder(registry, inputCoder);
        }
      };
    }
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * An abstract subclass of {@link CombineFn} for implementing combiners that are more
   * easily expressed as binary operations.
   */
  public abstract static class BinaryCombineFn<V> extends
      CombineFn<V, BinaryCombineFn.Holder<V>, V> {

    /**
     * Applies the binary operation to the two operands, returning the result.
     */
    public abstract V apply(V left, V right);

    /**
     * Returns the value that should be used for the combine of the empty set.
     */
    public V identity() {
      return null;
    }

    @Override
    public Holder<V> createAccumulator() {
      return new Holder<>();
    }

    @Override
    public Holder<V> addInput(Holder<V> accumulator, V input) {
      if (accumulator.present) {
        accumulator.set(apply(accumulator.value, input));
      } else {
        accumulator.set(input);
      }
      return accumulator;
    }

    @Override
    public Holder<V> mergeAccumulators(Iterable<Holder<V>> accumulators) {
      Holder<V> running = new Holder<>();
      for (Holder<V> accumulator : accumulators) {
        if (accumulator.present) {
          if (running.present) {
            running.set(apply(running.value, accumulator.value));
          } else {
            running.set(accumulator.value);
          }
        }
      }
      return running;
    }

    @Override
    public V extractOutput(Holder<V> accumulator) {
      if (accumulator.present) {
        return accumulator.value;
      } else {
        return identity();
      }
    }

    @Override
    public Coder<Holder<V>> getAccumulatorCoder(CoderRegistry registry, final Coder<V> inputCoder) {
      return new CustomCoder<Holder<V>>() {
        @Override
        public void encode(Holder<V> accumulator, OutputStream outStream, Context context)
            throws CoderException, IOException {
          if (accumulator.present) {
            outStream.write(1);
            inputCoder.encode(accumulator.value, outStream, context);
          } else {
            outStream.write(0);
          }
        }

        @Override
        public Holder<V> decode(InputStream inStream, Context context)
            throws CoderException, IOException {
          if (inStream.read() == 1) {
            return new Holder<>(inputCoder.decode(inStream, context));
          } else {
            return new Holder<>();
          }
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {
          inputCoder.verifyDeterministic();
        }
      };
    }

    @Override
    public Coder<V> getDefaultOutputCoder(CoderRegistry registry, Coder<V> inputCoder) {
      return inputCoder;
    }

    private static class Holder<V> {
      public V value;
      public boolean present;
      public Holder() { }
      public Holder(V value) {
        set(value);
      }
      public void set(V value) {
        this.present = true;
        this.value = value;
      }
    }
  }

  /**
   * An abstract subclass of {@link CombineFn} for implementing combiners that are more
   * easily expressed as binary operations on ints.
   */
  public abstract static class BinaryCombineIntegerFn extends
      CombineFn<Integer, int[], Integer> implements CounterProvider<Integer> {

    /**
     * Applies the binary operation to the two operands, returning the result.
     */
    public abstract int apply(int left, int right);

    /**
     * Returns the identity element of this operation, i.e. an element {@code e}
     * such that {@code apply(e, x) == apply(x, e) == x} for all values of {@code x}.
     */
    public abstract int identity();

    @Override
    public int[] createAccumulator() {
      return wrap(identity());
    }

    @Override
    public int[] addInput(int[] accumulator, Integer input) {
      accumulator[0] = apply(accumulator[0], input);
      return accumulator;
    }

    @Override
    public int[] mergeAccumulators(Iterable<int[]> accumulators) {
      Iterator<int[]> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator();
      } else {
        int running = iter.next()[0];
        while (iter.hasNext()) {
          running = apply(running, iter.next()[0]);
        }
        return wrap(running);
      }
    }

    @Override
    public Integer extractOutput(int[] accumulator) {
      return accumulator[0];
    }

    @Override
    public Coder<int[]> getAccumulatorCoder(CoderRegistry registry, Coder<Integer> inputCoder) {
      return DelegateCoder.of(
          inputCoder,
          new DelegateCoder.CodingFunction<int[], Integer>() {
            @Override
            public Integer apply(int[] accumulator) {
              return accumulator[0];
            }
          },
          new DelegateCoder.CodingFunction<Integer, int[]>() {
            @Override
            public int[] apply(Integer value) {
              return wrap(value);
            }
          });
    }

    @Override
    public Coder<Integer> getDefaultOutputCoder(CoderRegistry registry,
                                                Coder<Integer> inputCoder) {
      return inputCoder;
    }

    private int[] wrap(int value) {
      return new int[] { value };
    }
  }

  /**
   * An abstract subclass of {@link CombineFn} for implementing combiners that are more
   * easily expressed as binary operations on longs.
   */
  public abstract static class BinaryCombineLongFn extends
      CombineFn<Long, long[], Long> implements CounterProvider<Long> {

    /**
     * Applies the binary operation to the two operands, returning the result.
     */
    public abstract long apply(long left, long right);

    /**
     * Returns the identity element of this operation, i.e. an element {@code e}
     * such that {@code apply(e, x) == apply(x, e) == x} for all values of {@code x}.
     */
    public abstract long identity();

    @Override
    public long[] createAccumulator() {
      return wrap(identity());
    }

    @Override
    public long[] addInput(long[] accumulator, Long input) {
      accumulator[0] = apply(accumulator[0], input);
      return accumulator;
    }

    @Override
    public long[] mergeAccumulators(Iterable<long[]> accumulators) {
      Iterator<long[]> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator();
      } else {
        long running = iter.next()[0];
        while (iter.hasNext()) {
          running = apply(running, iter.next()[0]);
        }
        return wrap(running);
      }
    }

    @Override
    public Long extractOutput(long[] accumulator) {
      return accumulator[0];
    }

    @Override
    public Coder<long[]> getAccumulatorCoder(CoderRegistry registry, Coder<Long> inputCoder) {
      return DelegateCoder.of(
          inputCoder,
          new DelegateCoder.CodingFunction<long[], Long>() {
            @Override
            public Long apply(long[] accumulator) {
              return accumulator[0];
            }
          },
          new DelegateCoder.CodingFunction<Long, long[]>() {
            @Override
            public long[] apply(Long value) {
              return wrap(value);
            }
          });
    }

    @Override
    public Coder<Long> getDefaultOutputCoder(CoderRegistry registry, Coder<Long> inputCoder) {
      return inputCoder;
    }

    private long[] wrap(long value) {
      return new long[] { value };
    }
  }

  /**
   * An abstract subclass of {@link CombineFn} for implementing combiners that are more
   * easily expressed as binary operations on doubles.
   */
  public abstract static class BinaryCombineDoubleFn extends
      CombineFn<Double, double[], Double> implements CounterProvider<Double> {

    /**
     * Applies the binary operation to the two operands, returning the result.
     */
    public abstract double apply(double left, double right);

    /**
     * Returns the identity element of this operation, i.e. an element {@code e}
     * such that {@code apply(e, x) == apply(x, e) == x} for all values of {@code x}.
     */
    public abstract double identity();

    @Override
    public double[] createAccumulator() {
      return wrap(identity());
    }

    @Override
    public double[] addInput(double[] accumulator, Double input) {
      accumulator[0] = apply(accumulator[0], input);
      return accumulator;
    }

    @Override
    public double[] mergeAccumulators(Iterable<double[]> accumulators) {
      Iterator<double[]> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator();
      } else {
        double running = iter.next()[0];
        while (iter.hasNext()) {
          running = apply(running, iter.next()[0]);
        }
        return wrap(running);
      }
    }

    @Override
    public Double extractOutput(double[] accumulator) {
      return accumulator[0];
    }

    @Override
    public Coder<double[]> getAccumulatorCoder(CoderRegistry registry, Coder<Double> inputCoder) {
      return DelegateCoder.of(
          inputCoder,
          new DelegateCoder.CodingFunction<double[], Double>() {
            @Override
            public Double apply(double[] accumulator) {
              return accumulator[0];
            }
          },
          new DelegateCoder.CodingFunction<Double, double[]>() {
            @Override
            public double[] apply(Double value) {
              return wrap(value);
            }
          });
    }

    @Override
    public Coder<Double> getDefaultOutputCoder(CoderRegistry registry, Coder<Double> inputCoder) {
      return inputCoder;
    }

    private double[] wrap(double value) {
      return new double[] { value };
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
   *   public Accum createAccumulator() {
   *     return new Accum();
   *   }
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
   * @param <InputT> type of input values
   * @param <AccumT> type of mutable accumulator values
   * @param <OutputT> type of output values
   */
  public abstract static class AccumulatingCombineFn<
      InputT,
      AccumT extends AccumulatingCombineFn.Accumulator<InputT, AccumT, OutputT>,
      OutputT>
      extends CombineFn<InputT, AccumT, OutputT> {

    /**
     * The type of mutable accumulator values used by this
     * {@code AccumulatingCombineFn}.
     */
    public abstract static interface Accumulator<InputT, AccumT, OutputT> {
      /**
       * Adds the given input value to this accumulator, modifying
       * this accumulator.
       */
      public abstract void addInput(InputT input);

      /**
       * Adds the input values represented by the given accumulator
       * into this accumulator.
       */
      public abstract void mergeAccumulator(AccumT other);

      /**
       * Returns the output value that is the result of combining all
       * the input values represented by this accumulator.
       */
      public abstract OutputT extractOutput();
    }

    @Override
    public final AccumT addInput(AccumT accumulator, InputT input) {
      accumulator.addInput(input);
      return accumulator;
    }

    @Override
    public final AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
      AccumT accumulator = createAccumulator();
      for (AccumT partial : accumulators) {
        accumulator.mergeAccumulator(partial);
      }
      return accumulator;
    }

    @Override
    public final OutputT extractOutput(AccumT accumulator) {
      return accumulator.extractOutput();
    }
  }


  /////////////////////////////////////////////////////////////////////////////


  /**
   * A {@code KeyedCombineFn<K, InputT, AccumT, OutputT>} specifies how to combine
   * a collection of input values of type {@code InputT}, associated with
   * a key of type {@code K}, into a single output value of type
   * {@code OutputT}.  It does this via one or more intermediate mutable
   * accumulator values of type {@code AccumT}.
   *
   * <p> The overall process to combine a collection of input
   * {@code InputT} values associated with an input {@code K} key into a
   * single output {@code OutputT} value is as follows:
   *
   * <ol>
   *
   * <li> The input {@code InputT} values are partitioned into one or more
   * batches.
   *
   * <li> For each batch, the {@link #createAccumulator} operation is
   * invoked to create a fresh mutable accumulator value of type
   * {@code AccumT}, initialized to represent the combination of zero
   * values.
   *
   * <li> For each input {@code InputT} value in a batch, the
   * {@link #addInput} operation is invoked to add the value to that
   * batch's accumulator {@code AccumT} value.  The accumulator may just
   * record the new value (e.g., if {@code AccumT == List<InputT>}, or may do
   * work to represent the combination more compactly.
   *
   * <li> The {@link #mergeAccumulators} operation is invoked to
   * combine a collection of accumulator {@code AccumT} values into a
   * single combined output accumulator {@code AccumT} value, once the
   * merging accumulators have had all all the input values in their
   * batches added to them.  This operation is invoked repeatedly,
   * until there is only one accumulator value left.
   *
   * <li> The {@link #extractOutput} operation is invoked on the final
   * accumulator {@code AccumT} value to get the output {@code OutputT} value.
   *
   * </ol>
   *
   * <p> All of these operations are passed the {@code K} key that the
   * values being combined are associated with.
   *
   * <p> For example:
   * <pre> {@code
   * public class ConcatFn
   *     extends KeyedCombineFn<String, Integer, ConcatFn.Accum, String> {
   *   public static class Accum {
   *     String s = "";
   *   }
   *   public Accum createAccumulator(String key) {
   *     return new Accum();
   *   }
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
   * @param <InputT> type of input values
   * @param <AccumT> type of mutable accumulator values
   * @param <OutputT> type of output values
   */
  public abstract static class KeyedCombineFn<K, InputT, AccumT, OutputT>
      implements Serializable {
    /**
     * Returns a new, mutable accumulator value representing the
     * accumulation of zero input values.
     *
     * @param key the key that all the accumulated values using the
     * accumulator are associated with
     */
    public abstract AccumT createAccumulator(K key);

    /**
     * Adds the given input value to the given accumulator,
     * modifying the accumulator.
     *
     * <P> For efficiency, the input accumulator may be modified and returned.
     *
     * @param key the key that all the accumulated values using the
     * accumulator are associated with
     */
    public abstract AccumT addInput(K key, AccumT accumulator, InputT value);

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
    public abstract AccumT mergeAccumulators(K key, Iterable<AccumT> accumulators);

    /**
     * Returns the output value that is the result of combining all
     * the input values represented by the given accumulator.
     *
     * @param key the key that all the accumulated values using the
     * accumulator are associated with
     */
    public abstract OutputT extractOutput(K key, AccumT accumulator);

    /**
     * Applies this {@code KeyedCombineFn} to a key and a collection
     * of input values to produce a combined output value.
     *
     * <p> Useful when testing the behavior of a {@code KeyedCombineFn}
     * separately from a {@code Combine} transform.
     */
    public OutputT apply(K key, Iterable<? extends InputT> inputs) {
      AccumT accum = createAccumulator(key);
      for (InputT input : inputs) {
        accum = addInput(key, accum, input);
      }
      return extractOutput(key, accum);
    }

    /**
     * Returns the {@code Coder} to use for accumulator {@code AccumT}
     * values, or null if it is not able to be inferred.
     *
     * <p> By default, uses the knowledge of the {@code Coder} being
     * used for {@code K} keys and input {@code InputT} values and the
     * enclosing {@code Pipeline}'s {@code CoderRegistry} to try to
     * infer the Coder for {@code AccumT} values.
     *
     * <p> This is the Coder used to send data through a communication-intensive
     * shuffle step, so a compact and efficient representation may have
     * significant performance benefits.
     */
    public Coder<AccumT> getAccumulatorCoder(
        CoderRegistry registry, Coder<K> keyCoder, Coder<InputT> inputCoder)
        throws CannotProvideCoderException {
      return registry.getDefaultCoder(getClass(), KeyedCombineFn.class,
          ImmutableMap.<Type, Coder<?>>of(
              getKTypeVariable(), keyCoder,
              getInputTVariable(), inputCoder),
          getAccumTVariable());
    }

    /**
     * Returns the {@code Coder} to use by default for output
     * {@code OutputT} values, or null if it is not able to be inferred.
     *
     * <p> By default, uses the knowledge of the {@code Coder} being
     * used for {@code K} keys and input {@code InputT} values and the
     * enclosing {@code Pipeline}'s {@code CoderRegistry} to try to
     * infer the Coder for {@code OutputT} values.
     */
    public Coder<OutputT> getDefaultOutputCoder(
        CoderRegistry registry, Coder<K> keyCoder, Coder<InputT> inputCoder)
        throws CannotProvideCoderException {
      return registry.getDefaultCoder(getClass(), KeyedCombineFn.class,
          ImmutableMap.<Type, Coder<?>>of(
              getKTypeVariable(), keyCoder,
              getInputTVariable(), inputCoder,
              getAccumTVariable(), getAccumulatorCoder(registry, keyCoder, inputCoder)),
          getOutputTVariable());
    }

    @SuppressWarnings("unchecked")
    private TypeVariable<Class<KeyedCombineFn<?, ?, ?, ?>>> getKTypeVariable() {
      return (TypeVariable<Class<KeyedCombineFn<?, ?, ?, ?>>>)
          new TypeDescriptor<K>(KeyedCombineFn.class) {}
          .getType();
    }

    @SuppressWarnings("unchecked")
    private TypeVariable<Class<KeyedCombineFn<?, ?, ?, ?>>> getInputTVariable() {
      return (TypeVariable<Class<KeyedCombineFn<?, ?, ?, ?>>>)
          new TypeDescriptor<InputT>(KeyedCombineFn.class) {}
          .getType();
    }

    @SuppressWarnings("unchecked")
    private TypeVariable<Class<KeyedCombineFn<?, ?, ?, ?>>> getAccumTVariable() {
      return (TypeVariable<Class<KeyedCombineFn<?, ?, ?, ?>>>)
          new TypeDescriptor<AccumT>(KeyedCombineFn.class) {}
          .getType();
    }

    @SuppressWarnings("unchecked")
    private TypeVariable<Class<KeyedCombineFn<?, ?, ?, ?>>> getOutputTVariable() {
      return (TypeVariable<Class<KeyedCombineFn<?, ?, ?, ?>>>)
          new TypeDescriptor<OutputT>(KeyedCombineFn.class) {}
          .getType();
    }
  }


  ////////////////////////////////////////////////////////////////////////////

  /**
   * {@code Combine.Globally<InputT, OutputT>} takes a {@code PCollection<InputT>}
   * and returns a {@code PCollection<OutputT>} whose elements are the result of
   * combining all the elements in each window of the input {@code PCollection},
   * using a specified {@link CombineFn CombineFn&lt;InputT, AccumT, OutputT&gt;}.
   * It is common for {@code InputT == OutputT}, but not required.  Common combining
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
   * <p> If the input {@code PCollection} is windowed into {@link GlobalWindows},
   * a default value in the {@link GlobalWindow} will be output if the input
   * {@code PCollection} is empty.  To use this with inputs with other windowing,
   * either {@link #withoutDefaults} or {@link #asSingletonView} must be called.
   *
   * <p> By default, the {@code Coder} of the output {@code PValue<OutputT>}
   * is inferred from the concrete type of the
   * {@code CombineFn<InputT, AccumT, OutputT>}'s output type {@code OutputT}.
   *
   * <p> See also {@link #perKey}/{@link PerKey Combine.PerKey} and
   * {@link #groupedValues}/{@link GroupedValues Combine.GroupedValues}, which
   * are useful for combining values associated with each key in
   * a {@code PCollection} of {@code KV}s.
   *
   * @param <InputT> type of input values
   * @param <OutputT> type of output values
   */
  public static class Globally<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollection<OutputT>> {

    private final CombineFn<? super InputT, ?, OutputT> fn;
    private final boolean insertDefault;
    private final int fanout;

    private Globally(
        String name, CombineFn<? super InputT, ?, OutputT> fn, boolean insertDefault, int fanout) {
      super(name);
      this.fn = fn;
      this.insertDefault = insertDefault;
      this.fanout = fanout;
    }

    /**
     * Return a new {@code Globally} transform that's like this transform but with the
     * specified name. Does not modify this transform.
     */
    public Globally<InputT, OutputT> named(String name) {
      return new Globally<>(name, fn, insertDefault, fanout);
    }

    /**
     * Returns a {@link PTransform} that produces a {@code PCollectionView}
     * whose elements are the result of combining elements per-window in
     * the input {@code PCollection}.  If a value is requested from the view
     * for a window that is not present, the result of calling the {@code CombineFn}
     * on empty input will returned.
     */
    public GloballyAsSingletonView<InputT, OutputT> asSingletonView() {
      return new GloballyAsSingletonView<>(fn, insertDefault, fanout);
    }

    /**
     * Returns a {@link PTransform} identical to this, but that does not attempt to
     * provide a default value in the case of empty input.
     */
    public Globally<InputT, OutputT> withoutDefaults() {
      return new Globally<>(name, fn, false, fanout);
    }

    /**
     * Returns a {@link PTransform} identical to this, but that uses an intermediate node
     * to combine parts of the data to reduce load on the final global combine step.
     *
     * <p> The {@code fanout} parameter determines the number of intermediate keys
     * that will be used.
     */
    public Globally<InputT, OutputT> withFanout(int fanout) {
      return new Globally<>(name, fn, insertDefault, fanout);
    }

    @Override
    public PCollection<OutputT> apply(PCollection<InputT> input) {
      PCollection<KV<Void, InputT>> withKeys = input
          .apply(WithKeys.<Void, InputT>of((Void) null))
          .setCoder(KvCoder.of(VoidCoder.of(), input.getCoder()));

      PCollection<KV<Void, OutputT>> combined;
      if (fanout >= 2) {
        combined = withKeys.apply(
            Combine.<Void, InputT, OutputT>fewKeys(fn.<Void>asKeyedFn()).withHotKeyFanout(fanout));
      } else {
        combined = withKeys.apply(Combine.<Void, InputT, OutputT>fewKeys(fn.<Void>asKeyedFn()));
      }

      PCollection<OutputT> output = combined.apply(Values.<OutputT>create());

      if (insertDefault) {
        if (!output.getWindowingStrategy().getWindowFn().isCompatible(new GlobalWindows())) {
          throw new IllegalStateException(
              "Attempted to add default value to PCollection not windowed by GlobalWindows. "
              + "Instead, use Combine.globally().withoutDefaults() or "
              + "Combine.globally().asSingletonView().");
        }
        return insertDefaultValueIfEmpty(output);
      } else {
        return output;
      }
    }

    private PCollection<OutputT> insertDefaultValueIfEmpty(PCollection<OutputT> maybeEmpty) {
      final PCollectionView<Iterable<OutputT>> maybeEmptyView = maybeEmpty.apply(
          View.<OutputT>asIterable());
      return maybeEmpty.getPipeline()
        .apply("CreateVoid", Create.of((Void) null).withCoder(VoidCoder.of()))
          .apply(ParDo.named("ProduceDefault").of(
              new DoFn<Void, OutputT>() {
                @Override
                public void processElement(DoFn<Void, OutputT>.ProcessContext c) {
                  Iterator<OutputT> combined = c.sideInput(maybeEmptyView).iterator();
                  if (combined.hasNext()) {
                    c.output(combined.next());
                  } else {
                    c.output(fn.apply(Collections.<InputT>emptyList()));
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
   * {@code Combine.GloballyAsSingletonView<InputT, OutputT>} takes a {@code PCollection<InputT>}
   * and returns a {@code PCollectionView<OutputT>} whose elements are the result of
   * combining all the elements in each window of the input {@code PCollection},
   * using a specified {@link CombineFn CombineFn&lt;InputT, AccumT, OutputT&gt;}.
   * It is common for {@code InputT == OutputT}, but not required. Common combining
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
   * <p> If a value is requested from the view for a window that is not present
   * and {@code insertDefault} is true, the result of calling the {@code CombineFn}
   * on empty input will returned. If {@code insertDefault} is false, an
   * exception will be thrown instead.
   *
   * <p> By default, the {@code Coder} of the output {@code PValue<OutputT>}
   * is inferred from the concrete type of the
   * {@code CombineFn<InputT, AccumT, OutputT>}'s output type {@code OutputT}.
   *
   * <p> See also {@link #perKey}/{@link PerKey Combine.PerKey} and
   * {@link #groupedValues}/{@link GroupedValues Combine.GroupedValues}, which
   * are useful for combining values associated with each key in
   * a {@code PCollection} of {@code KV}s.
   *
   * @param <InputT> type of input values
   * @param <OutputT> type of output values
   */
  public static class GloballyAsSingletonView<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollectionView<OutputT>> {

    private final CombineFn<? super InputT, ?, OutputT> fn;
    private final boolean insertDefault;
    private final int fanout;

    private GloballyAsSingletonView(
        CombineFn<? super InputT, ?, OutputT> fn, boolean insertDefault, int fanout) {
      this.fn = fn;
      this.insertDefault = insertDefault;
      this.fanout = fanout;
    }


    @Override
    public PCollectionView<OutputT> apply(PCollection<InputT> input) {
      PCollection<OutputT> combined = input
          .apply(Combine.globally(fn).withoutDefaults().withFanout(fanout));
      if (insertDefault) {
        return combined
            .apply(View.<OutputT>asSingleton().withDefaultValue(
                fn.apply(Collections.<InputT>emptyList())));
      } else {
        return combined.apply(View.<OutputT>asSingleton());
      }
    }

    @Override
    protected String getKindString() {
      return "Combine.GloballyAsSingletonView";
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
  public static class SimpleCombineFn<V> extends CombineFn<V, List<V>, V> {
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
    public List<V> addInput(List<V> accumulator, V input) {
      accumulator.add(input);
      if (accumulator.size() > BUFFER_SIZE) {
        return mergeToSingleton(accumulator);
      } else {
        return accumulator;
      }
    }

    @Override
    public List<V> mergeAccumulators(Iterable<List<V>> accumulators) {
      return mergeToSingleton(Iterables.concat(accumulators));
    }

    @Override
    public V extractOutput(List<V> accumulator) {
      return combiner.apply(accumulator);
    }

    private List<V> mergeToSingleton(Iterable<V> values) {
      List<V> singleton = new ArrayList<>();
      singleton.add(combiner.apply(values));
      return singleton;
    }
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * {@code PerKey<K, InputT, OutputT>} takes a
   * {@code PCollection<KV<K, InputT>>}, groups it by key, applies a
   * combining function to the {@code InputT} values associated with each
   * key to produce a combined {@code OutputT} value, and returns a
   * {@code PCollection<KV<K, OutputT>>} representing a map from each
   * distinct key of the input {@code PCollection} to the corresponding
   * combined value.  {@code InputT} and {@code OutputT} are often the same.
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
   * @param <InputT> the type of the values of the input {@code PCollection}
   * @param <OutputT> the type of the values of the output {@code PCollection}
   */
  public static class PerKey<K, InputT, OutputT>
    extends PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> {

    private final transient KeyedCombineFn<? super K, ? super InputT, ?, OutputT> fn;
    private final boolean fewKeys;

    private PerKey(
        String name, KeyedCombineFn<? super K, ? super InputT, ?, OutputT> fn,
        boolean fewKeys) {
      super(name);
      this.fn = fn;
      this.fewKeys = fewKeys;
    }

    /**
     * Return a new {@code Globally} transform that's like this transform but with the
     * specified name. Does not modify this transform.
     */
    public PerKey<K, InputT, OutputT> named(String name) {
      return new PerKey<K, InputT, OutputT>(name, fn, fewKeys);
    }

    /**
     * If a single key has disproportionately many values, it may become a
     * bottleneck, especially in streaming mode.  This returns a new per-key
     * combining transform that inserts an intermediate node to combine "hot"
     * keys partially before performing the full combine.
     *
     * @param hotKeyFanout a function from keys to an integer N, where the key
     * will be spread among N intermediate nodes for partial combining.
     * If N is less than or equal to 1, this key will not be sent through an
     * intermediate node.
     */
    public PerKeyWithHotKeyFanout<K, InputT, OutputT> withHotKeyFanout(
        SerializableFunction<? super K, Integer> hotKeyFanout) {
      return new PerKeyWithHotKeyFanout<K, InputT, OutputT>(name, fn, hotKeyFanout);
    }

    /**
     * Like {@link #withHotKeyFanout(SerializableFunction)}, but returning the given
     * constant value for every key.
     */
    public PerKeyWithHotKeyFanout<K, InputT, OutputT> withHotKeyFanout(final int hotKeyFanout) {
      return withHotKeyFanout(
          new SerializableFunction<K, Integer>(){
            @Override
            public Integer apply(K unused) {
              return hotKeyFanout;
            }
          });
    }

    /**
     * Returns the KeyedCombineFn used by this Combine operation.
     */
    public KeyedCombineFn<? super K, ? super InputT, ?, OutputT> getFn() {
      return fn;
    }

    @Override
    public PCollection<KV<K, OutputT>> apply(PCollection<KV<K, InputT>> input) {
      return input
        .apply(GroupByKey.<K, InputT>create(fewKeys))
        .apply(Combine.<K, InputT, OutputT>groupedValues(fn));
    }

    @Override
    protected String getKindString() {
      return "Combine.PerKey";
    }
  }

  /**
   * Like {@link PerKey}, but sharding the combining of hot keys.
   */
  public static class PerKeyWithHotKeyFanout<K, InputT, OutputT>
      extends PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> {

    private final transient KeyedCombineFn<? super K, ? super InputT, ?, OutputT> fn;
    private final SerializableFunction<? super K, Integer> hotKeyFanout;

    private PerKeyWithHotKeyFanout(String name,
        KeyedCombineFn<? super K, ? super InputT, ?, OutputT> fn,
        SerializableFunction<? super K, Integer> hotKeyFanout) {
      super(name);
      this.fn = fn;
      this.hotKeyFanout = hotKeyFanout;
    }

    @Override
    public PCollection<KV<K, OutputT>> apply(PCollection<KV<K, InputT>> input) {
      return applyHelper(input);
    }

    private <AccumT> PCollection<KV<K, OutputT>> applyHelper(PCollection<KV<K, InputT>> input) {
      // Name the accumulator type.
      @SuppressWarnings("unchecked")
      final KeyedCombineFn<K, InputT, AccumT, OutputT> fn =
          (KeyedCombineFn<K, InputT, AccumT, OutputT>) this.fn;

      // A CombineFn's mergeAccumulator can be applied in a tree-like fashon.
      // Here we shard the key using an integer nonce, combine on that partial
      // set of values, then drop the nonce and do a final combine of the
      // aggregates.  We do this by splitting the original CombineFn into two,
      // on that does addInput + merge and another that does merge + extract.
      KeyedCombineFn<KV<K, Integer>, InputT, AccumT, AccumT> hotPreCombine =
          new KeyedCombineFn<KV<K, Integer>, InputT, AccumT, AccumT>() {
            @Override
            public AccumT createAccumulator(KV<K, Integer> key) {
              return fn.createAccumulator(key.getKey());
            }
            @Override
            public AccumT addInput(KV<K, Integer> key, AccumT accumulator, InputT value) {
              return fn.addInput(key.getKey(), accumulator, value);
            }
            @Override
            public AccumT mergeAccumulators(
                KV<K, Integer> key, Iterable<AccumT> accumulators) {
              return fn.mergeAccumulators(key.getKey(), accumulators);
            }
            @Override
            public AccumT extractOutput(KV<K, Integer> key, AccumT accumulator) {
              return accumulator;
            }
            @Override
            @SuppressWarnings("unchecked")
            public Coder<AccumT> getAccumulatorCoder(
                CoderRegistry registry, Coder<KV<K, Integer>> keyCoder, Coder<InputT> inputCoder)
                throws CannotProvideCoderException {
              return fn.getAccumulatorCoder(
                  registry, ((KvCoder<K, Integer>) keyCoder).getKeyCoder(), inputCoder);
            }
      };

      @SuppressWarnings("unchecked")
      final KvCoder<K, InputT> inputCoder = ((KvCoder<K, InputT>) input.getCoder());
      KeyedCombineFn<K, AccumT, AccumT, OutputT> hotPostCombine =
          new KeyedCombineFn<K, AccumT, AccumT, OutputT>() {
            @Override
            public AccumT createAccumulator(K key) {
              return fn.createAccumulator(key);
            }
            @Override
            public AccumT addInput(K key, AccumT accumulator, AccumT value) {
              return fn.mergeAccumulators(
                  key, ImmutableList.of(accumulator, value));
            }
            @Override
            public AccumT mergeAccumulators(K key, Iterable<AccumT> accumulators) {
              return fn.mergeAccumulators(key, accumulators);
            }
            @Override
            public OutputT extractOutput(K key, AccumT accumulator) {
              return fn.extractOutput(key, accumulator);
            }
            @Override
            public Coder<OutputT> getDefaultOutputCoder(
                CoderRegistry registry, Coder<K> keyCoder, Coder<AccumT> accumulatorCoder)
                throws CannotProvideCoderException {
              return fn.getDefaultOutputCoder(registry, keyCoder, inputCoder.getValueCoder());
            }
      };

      // Use the provided hotKeyFanout fn to split into "hot" and "cold" keys,
      // augmenting the hot keys with a nonce.
      final TupleTag<KV<KV<K, Integer>, InputT>> hot = new TupleTag<>();
      final TupleTag<KV<K, InputT>> cold = new TupleTag<>();
      PCollectionTuple split = input.apply(
          ParDo.named("AddNonce").of(
              new DoFn<KV<K, InputT>, KV<K, InputT>>() {
                transient int counter;
                @Override
                public void startBundle(Context c) {
                  counter = ThreadLocalRandom.current().nextInt(
                      Integer.MAX_VALUE);
                }

                @Override
                public void processElement(ProcessContext c) {
                  KV<K, InputT> kv = c.element();
                  int spread = hotKeyFanout.apply(kv.getKey());
                  if (spread <= 1) {
                    c.output(kv);
                  } else {
                    int nonce = counter++ % spread;
                    c.sideOutput(hot, KV.of(KV.of(kv.getKey(), nonce), kv.getValue()));
                  }
                }
              })
          .withOutputTags(cold, TupleTagList.of(hot)));

      // Combine the hot and cold keys separately.
      PCollection<KV<K, OutputT>> combinedHot = split
          .get(hot)
          .setCoder(KvCoder.of(KvCoder.of(inputCoder.getKeyCoder(), VarIntCoder.of()),
                               inputCoder.getValueCoder()))
          .apply("PreCombineHot", Combine.perKey(hotPreCombine))
          .apply(ParDo.named("StripNonce").of(
              new DoFn<KV<KV<K, Integer>, AccumT>, KV<K, AccumT>>() {
                @Override
                public void processElement(ProcessContext c) {
                  c.output(KV.of(c.element().getKey().getKey(), c.element().getValue()));
                }
              }))
          .apply(Window.<KV<K, AccumT>>remerge())
          .apply("PostCombineHot", Combine.perKey(hotPostCombine));
      PCollection<KV<K, OutputT>> combinedCold = split
          .get(cold)
          .setCoder(inputCoder)
          .apply("CombineCold", Combine.perKey(fn));

      // Return the union of the hot and cold key results.
      return PCollectionList.of(combinedHot).and(combinedCold)
          .apply(Flatten.<KV<K, OutputT>>pCollections());
    }

    @Override
    protected String getKindString() {
      return "Combine.PerKey";
    }
  }


  /////////////////////////////////////////////////////////////////////////////

  /**
   * {@code GroupedValues<K, InputT, OutputT>} takes a
   * {@code PCollection<KV<K, Iterable<InputT>>>}, such as the result of
   * {@link GroupByKey}, applies a specified
   * {@link KeyedCombineFn KeyedCombineFn&lt;K, InputT, AccumT, OutputT&gt;}
   * to each of the input {@code KV<K, Iterable<InputT>>} elements to
   * produce a combined output {@code KV<K, OutputT>} element, and returns a
   * {@code PCollection<KV<K, OutputT>>} containing all the combined output
   * elements.  It is common for {@code InputT == OutputT}, but not required.
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
   * <p> See also {@link #perKey}/{@link PerKey Combine.PerKey}, which
   * captures the common pattern of "combining by key" in a
   * single easy-to-use {@code PTransform}.
   *
   * <p> Combining for different keys can happen in parallel.  Moreover,
   * combining of the {@code Iterable<InputT>} values associated a single
   * key can happen in parallel, with different subsets of the values
   * being combined separately, and their intermediate results combined
   * further, in an arbitrary tree reduction pattern, until a single
   * result value is produced for each key.
   *
   * <p> By default, the {@code Coder} of the keys of the output
   * {@code PCollection<KV<K, OutputT>>} is that of the keys of the input
   * {@code PCollection<KV<K, InputT>>}, and the {@code Coder} of the values
   * of the output {@code PCollection<KV<K, OutputT>>} is inferred from the
   * concrete type of the {@code KeyedCombineFn<K, InputT, AccumT, OutputT>}'s output
   * type {@code OutputT}.
   *
   * <p> Each output element has the same timestamp and is in the same window
   * as its corresponding input element, and the output
   * {@code PCollection} has the same
   * {@link com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn}
   * associated with it as the input.
   *
   * <p> See also {@link #globally}/{@link Globally Combine.Globally}, which
   * combines all the values in a {@code PCollection} into a
   * single value in a {@code PCollection}.
   *
   * @param <K> type of input and output keys
   * @param <InputT> type of input values
   * @param <OutputT> type of output values
   */
  public static class GroupedValues<K, InputT, OutputT>
      extends PTransform
                        <PCollection<? extends KV<K, ? extends Iterable<InputT>>>,
                         PCollection<KV<K, OutputT>>> {

    private final KeyedCombineFn<? super K, ? super InputT, ?, OutputT> fn;

    private GroupedValues(KeyedCombineFn<? super K, ? super InputT, ?, OutputT> fn) {
      this.fn = fn;
    }

    /**
     * Returns the KeyedCombineFn used by this Combine operation.
     */
    public KeyedCombineFn<? super K, ? super InputT, ?, OutputT> getFn() {
      return fn;
    }

    @Override
    public PCollection<KV<K, OutputT>> apply(
        PCollection<? extends KV<K, ? extends Iterable<InputT>>> input) {

      PCollection<KV<K, OutputT>> output = input.apply(ParDo.of(
          new DoFn<KV<K, ? extends Iterable<InputT>>, KV<K, OutputT>>() {
            @Override
            public void processElement(ProcessContext c) {
              K key = c.element().getKey();
              c.output(KV.of(key, fn.apply(key, c.element().getValue())));
            }
          }));

      try {
        Coder<KV<K, OutputT>> outputCoder = getDefaultOutputCoder(input);
        output.setCoder(outputCoder);
      } catch (CannotProvideCoderException exc) {
        // let coder inference happen later, if it can
      }

      return output;
    }

    private KvCoder<K, InputT> getKvCoder(
        Coder<? extends KV<K, ? extends Iterable<InputT>>> inputCoder) {
      /*
      Coder<? extends KV<K, ? extends Iterable<InputT>>> inputCoder =
          getInput().getCoder();
          */
      if (!(inputCoder instanceof KvCoder)) {
        throw new IllegalStateException(
            "Combine.GroupedValues requires its input to use KvCoder");
      }
      @SuppressWarnings({"unchecked", "rawtypes"})
      KvCoder<K, ? extends Iterable<InputT>> kvCoder = (KvCoder) inputCoder;
      Coder<K> keyCoder = kvCoder.getKeyCoder();
      Coder<? extends Iterable<InputT>> kvValueCoder = kvCoder.getValueCoder();
      if (!(kvValueCoder instanceof IterableCoder)) {
        throw new IllegalStateException(
            "Combine.GroupedValues requires its input values to use "
            + "IterableCoder");
      }
      @SuppressWarnings("unchecked")
      IterableCoder<InputT> inputValuesCoder = (IterableCoder<InputT>) kvValueCoder;
      Coder<InputT> inputValueCoder = inputValuesCoder.getElemCoder();
      return KvCoder.of(keyCoder, inputValueCoder);
    }

    @SuppressWarnings("unchecked")
    public Coder<?> getAccumulatorCoder(
        CoderRegistry coderRegistry,
        PCollection<? extends KV<K, ? extends Iterable<InputT>>> input)
        throws CannotProvideCoderException {
      KvCoder<K, InputT> kvCoder = getKvCoder(input.getCoder());
      return ((KeyedCombineFn<K, InputT, ?, OutputT>) fn).getAccumulatorCoder(
          coderRegistry, kvCoder.getKeyCoder(), kvCoder.getValueCoder());
    }

    @Override
    public Coder<KV<K, OutputT>> getDefaultOutputCoder(
        PCollection<? extends KV<K, ? extends Iterable<InputT>>> input)
        throws CannotProvideCoderException {
      KvCoder<K, InputT> kvCoder = getKvCoder(input.getCoder());
      @SuppressWarnings("unchecked")
      Coder<OutputT> outputValueCoder = ((KeyedCombineFn<K, InputT, ?, OutputT>) fn)
          .getDefaultOutputCoder(
              input.getPipeline().getCoderRegistry(),
              kvCoder.getKeyCoder(), kvCoder.getValueCoder());
      return KvCoder.of(kvCoder.getKeyCoder(), outputValueCoder);
    }
  }
}
