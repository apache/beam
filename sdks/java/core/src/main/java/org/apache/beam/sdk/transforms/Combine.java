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
package org.apache.beam.sdk.transforms;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.DelegateCoder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.CombineFnBase.AbstractGlobalCombineFn;
import org.apache.beam.sdk.transforms.CombineFnBase.GlobalCombineFn;
import org.apache.beam.sdk.transforms.CombineWithContext.CombineFnWithContext;
import org.apache.beam.sdk.transforms.CombineWithContext.RequiresContextInternal;
import org.apache.beam.sdk.transforms.View.CreatePCollectionView;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.util.NameUtils;
import org.apache.beam.sdk.util.NameUtils.NameOverride;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PCollectionViews;
import org.apache.beam.sdk.values.PCollectionViews.TypeDescriptorSupplier;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.common.hash.Hashing;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * {@code PTransform}s for combining {@code PCollection} elements globally and per-key.
 *
 * <p>See the <a
 * href="https://beam.apache.org/documentation/programming-guide/#transforms-combine">documentation</a>
 * for how to use the operations in this class.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class Combine {
  private Combine() {
    // do not instantiate
  }

  /**
   * Returns a {@link Globally Combine.Globally} {@code PTransform} that uses the given {@code
   * SerializableFunction} to combine all the elements in each window of the input {@code
   * PCollection} into a single value in the output {@code PCollection}. The types of the input
   * elements and the output elements must be the same.
   *
   * <p>If the input {@code PCollection} is windowed into {@link GlobalWindows}, a default value in
   * the {@link GlobalWindow} will be output if the input {@code PCollection} is empty. To use this
   * with inputs with other windowing, either {@link Globally#withoutDefaults} or {@link
   * Globally#asSingletonView} must be called.
   *
   * <p>See {@link Globally Combine.Globally} for more information.
   */
  public static <V> Globally<V, V> globally(SerializableFunction<Iterable<V>, V> combiner) {
    return globally(IterableCombineFn.of(combiner), displayDataForFn(combiner));
  }

  /**
   * Returns a {@link Globally Combine.Globally} {@code PTransform} that uses the given {@code
   * SerializableBiFunction} to combine all the elements in each window of the input {@code
   * PCollection} into a single value in the output {@code PCollection}. The types of the input
   * elements and the output elements must be the same.
   *
   * <p>If the input {@code PCollection} is windowed into {@link GlobalWindows}, a default value in
   * the {@link GlobalWindow} will be output if the input {@code PCollection} is empty. To use this
   * with inputs with other windowing, either {@link Globally#withoutDefaults} or {@link
   * Globally#asSingletonView} must be called.
   *
   * <p>See {@link Globally Combine.Globally} for more information.
   */
  public static <V> Globally<V, V> globally(SerializableBiFunction<V, V, V> combiner) {
    return globally(BinaryCombineFn.of(combiner), displayDataForFn(combiner));
  }

  /**
   * Returns a {@link Globally Combine.Globally} {@code PTransform} that uses the given {@code
   * GloballyCombineFn} to combine all the elements in each window of the input {@code PCollection}
   * into a single value in the output {@code PCollection}. The types of the input elements and the
   * output elements can differ.
   *
   * <p>If the input {@code PCollection} is windowed into {@link GlobalWindows}, a default value in
   * the {@link GlobalWindow} will be output if the input {@code PCollection} is empty. To use this
   * with inputs with other windowing, either {@link Globally#withoutDefaults} or {@link
   * Globally#asSingletonView} must be called.
   *
   * <p>See {@link Globally Combine.Globally} for more information.
   */
  public static <InputT, OutputT> Globally<InputT, OutputT> globally(
      GlobalCombineFn<? super InputT, ?, OutputT> fn) {
    return globally(fn, displayDataForFn(fn));
  }

  private static <T> DisplayData.ItemSpec<? extends Class<?>> displayDataForFn(T fn) {
    return DisplayData.item("combineFn", fn.getClass()).withLabel("Combiner");
  }

  private static <InputT, OutputT> Globally<InputT, OutputT> globally(
      GlobalCombineFn<? super InputT, ?, OutputT> fn,
      DisplayData.ItemSpec<? extends Class<?>> fnDisplayData) {
    return new Globally<>(fn, fnDisplayData, true, 0, ImmutableList.of());
  }

  /**
   * Returns a {@link PerKey Combine.PerKey} {@code PTransform} that first groups its input {@code
   * PCollection} of {@code KV}s by keys and windows, then invokes the given function on each of the
   * values lists to produce a combined value, and then returns a {@code PCollection} of {@code KV}s
   * mapping each distinct key to its combined value for each window.
   *
   * <p>Each output element is in the window by which its corresponding input was grouped, and has
   * the timestamp of the end of that window. The output {@code PCollection} has the same {@link
   * org.apache.beam.sdk.transforms.windowing.WindowFn} as the input.
   *
   * <p>See {@link PerKey Combine.PerKey} for more information.
   */
  public static <K, V> PerKey<K, V, V> perKey(SerializableFunction<Iterable<V>, V> fn) {
    return perKey(IterableCombineFn.of(fn), displayDataForFn(fn));
  }

  /**
   * Returns a {@link PerKey Combine.PerKey} {@code PTransform} that first groups its input {@code
   * PCollection} of {@code KV}s by keys and windows, then invokes the given function on each of the
   * values lists to produce a combined value, and then returns a {@code PCollection} of {@code KV}s
   * mapping each distinct key to its combined value for each window.
   *
   * <p>Each output element is in the window by which its corresponding input was grouped, and has
   * the timestamp of the end of that window. The output {@code PCollection} has the same {@link
   * org.apache.beam.sdk.transforms.windowing.WindowFn} as the input.
   *
   * <p>See {@link PerKey Combine.PerKey} for more information.
   */
  public static <K, V> PerKey<K, V, V> perKey(SerializableBiFunction<V, V, V> fn) {
    return perKey(BinaryCombineFn.of(fn), displayDataForFn(fn));
  }

  /**
   * Returns a {@link PerKey Combine.PerKey} {@code PTransform} that first groups its input {@code
   * PCollection} of {@code KV}s by keys and windows, then invokes the given function on each of the
   * values lists to produce a combined value, and then returns a {@code PCollection} of {@code KV}s
   * mapping each distinct key to its combined value for each window.
   *
   * <p>Each output element is in the window by which its corresponding input was grouped, and has
   * the timestamp of the end of that window. The output {@code PCollection} has the same {@link
   * org.apache.beam.sdk.transforms.windowing.WindowFn} as the input.
   *
   * <p>See {@link PerKey Combine.PerKey} for more information.
   */
  public static <K, InputT, OutputT> PerKey<K, InputT, OutputT> perKey(
      GlobalCombineFn<? super InputT, ?, OutputT> fn) {
    return perKey(fn, displayDataForFn(fn));
  }

  public static <K, InputT, AccumT, OutputT>
      PerKeyWithBucketing<K, InputT, AccumT, OutputT> perKeyWithBucketing(
          CombineFn<InputT, AccumT, OutputT> fn, int numBucketBits) {
    return new PerKeyWithBucketing<>(fn, numBucketBits);
  }

  private static <K, InputT, OutputT> PerKey<K, InputT, OutputT> perKey(
      GlobalCombineFn<? super InputT, ?, OutputT> fn,
      DisplayData.ItemSpec<? extends Class<?>> fnDisplayData) {
    return new PerKey<>(fn, fnDisplayData, false /*fewKeys*/);
  }

  /** Returns a {@link PerKey Combine.PerKey}, and set fewKeys in {@link GroupByKey}. */
  @Internal
  public static <K, InputT, OutputT> PerKey<K, InputT, OutputT> fewKeys(
      GlobalCombineFn<? super InputT, ?, OutputT> fn) {
    return new PerKey<>(fn, displayDataForFn(fn), true /*fewKeys*/);
  }

  /** Returns a {@link PerKey Combine.PerKey}, and set fewKeys in {@link GroupByKey}. */
  private static <K, InputT, OutputT> PerKey<K, InputT, OutputT> fewKeys(
      GlobalCombineFn<? super InputT, ?, OutputT> fn,
      DisplayData.ItemSpec<? extends Class<?>> fnDisplayData) {
    return new PerKey<>(fn, fnDisplayData, true /*fewKeys*/);
  }

  /**
   * Returns a {@link GroupedValues Combine.GroupedValues} {@code PTransform} that takes a {@code
   * PCollection} of {@code KV}s where a key maps to an {@code Iterable} of values, e.g., the result
   * of a {@code GroupByKey}, then uses the given {@code SerializableFunction} to combine all the
   * values associated with a key, ignoring the key. The type of the input and output values must be
   * the same.
   *
   * <p>Each output element has the same timestamp and is in the same window as its corresponding
   * input element, and the output {@code PCollection} has the same {@link
   * org.apache.beam.sdk.transforms.windowing.WindowFn} associated with it as the input.
   *
   * <p>See {@link GroupedValues Combine.GroupedValues} for more information.
   *
   * <p>Note that {@link #perKey(SerializableFunction)} is typically more convenient to use than
   * {@link GroupByKey} followed by {@code groupedValues(...)}.
   */
  public static <K, V> GroupedValues<K, V, V> groupedValues(
      SerializableFunction<Iterable<V>, V> fn) {
    return groupedValues(IterableCombineFn.of(fn), displayDataForFn(fn));
  }

  /**
   * Returns a {@link GroupedValues Combine.GroupedValues} {@code PTransform} that takes a {@code
   * PCollection} of {@code KV}s where a key maps to an {@code Iterable} of values, e.g., the result
   * of a {@code GroupByKey}, then uses the given {@code SerializableFunction} to combine all the
   * values associated with a key, ignoring the key. The type of the input and output values must be
   * the same.
   *
   * <p>Each output element has the same timestamp and is in the same window as its corresponding
   * input element, and the output {@code PCollection} has the same {@link
   * org.apache.beam.sdk.transforms.windowing.WindowFn} associated with it as the input.
   *
   * <p>See {@link GroupedValues Combine.GroupedValues} for more information.
   *
   * <p>Note that {@link #perKey(SerializableBiFunction)} is typically more convenient to use than
   * {@link GroupByKey} followed by {@code groupedValues(...)}.
   */
  public static <K, V> GroupedValues<K, V, V> groupedValues(SerializableBiFunction<V, V, V> fn) {
    return groupedValues(BinaryCombineFn.of(fn), displayDataForFn(fn));
  }

  /**
   * Returns a {@link GroupedValues Combine.GroupedValues} {@code PTransform} that takes a {@code
   * PCollection} of {@code KV}s where a key maps to an {@code Iterable} of values, e.g., the result
   * of a {@code GroupByKey}, then uses the given {@code CombineFn} to combine all the values
   * associated with a key, ignoring the key. The types of the input and output values can differ.
   *
   * <p>Each output element has the same timestamp and is in the same window as its corresponding
   * input element, and the output {@code PCollection} has the same {@link
   * org.apache.beam.sdk.transforms.windowing.WindowFn} associated with it as the input.
   *
   * <p>See {@link GroupedValues Combine.GroupedValues} for more information.
   *
   * <p>Note that {@link #perKey(CombineFnBase.GlobalCombineFn)} is typically more convenient to use
   * than {@link GroupByKey} followed by {@code groupedValues(...)}.
   */
  public static <K, InputT, OutputT> GroupedValues<K, InputT, OutputT> groupedValues(
      GlobalCombineFn<? super InputT, ?, OutputT> fn) {
    return groupedValues(fn, displayDataForFn(fn));
  }

  private static <K, InputT, OutputT> GroupedValues<K, InputT, OutputT> groupedValues(
      GlobalCombineFn<? super InputT, ?, OutputT> fn,
      DisplayData.ItemSpec<? extends Class<?>> fnDisplayData) {
    return new GroupedValues<>(fn, fnDisplayData);
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@code CombineFn<InputT, AccumT, OutputT>} specifies how to combine a collection of input
   * values of type {@code InputT} into a single output value of type {@code OutputT}. It does this
   * via one or more intermediate mutable accumulator values of type {@code AccumT}.
   *
   * <p>The overall process to combine a collection of input {@code InputT} values into a single
   * output {@code OutputT} value is as follows:
   *
   * <ol>
   *   <li>The input {@code InputT} values are partitioned into one or more batches.
   *   <li>For each batch, the {@link #createAccumulator} operation is invoked to create a fresh
   *       mutable accumulator value of type {@code AccumT}, initialized to represent the
   *       combination of zero values.
   *   <li>For each input {@code InputT} value in a batch, the {@link #addInput} operation is
   *       invoked to add the value to that batch's accumulator {@code AccumT} value. The
   *       accumulator may just record the new value (e.g., if {@code AccumT == List<InputT>}, or
   *       may do work to represent the combination more compactly.
   *   <li>The {@link #mergeAccumulators} operation is invoked to combine a collection of
   *       accumulator {@code AccumT} values into a single combined output accumulator {@code
   *       AccumT} value, once the merging accumulators have had all all the input values in their
   *       batches added to them. This operation is invoked repeatedly, until there is only one
   *       accumulator value left.
   *   <li>The {@link #extractOutput} operation is invoked on the final accumulator {@code AccumT}
   *       value to get the output {@code OutputT} value.
   * </ol>
   *
   * <p>For example:
   *
   * <pre><code>
   * public class AverageFn extends{@literal CombineFn<Integer, AverageFn.Accum, Double>} {
   *   public static class Accum implements Serializable {
   *     int sum = 0;
   *     int count = 0;
   *
   *    {@literal @Override}
   *     public boolean equals(@Nullable Object other) {
   *       if (other == null) return false;
   *       if (other == this) return true;
   *       if (!(other instanceof Accum))return false;
   *
   *
   *       Accum o = (Accum)other;
   *       if (this.sum != o.sum || this.count != o.count) {
   *         return false;
   *       } else {
   *         return true;
   *       }
   *     }
   *   }
   *
   *   public Accum createAccumulator() {
   *     return new Accum();
   *   }
   *
   *   public Accum addInput(Accum accum, Integer input) {
   *       accum.sum += input;
   *       accum.count++;
   *       return accum;
   *   }
   *
   *   public Accum{@literal mergeAccumulators(Iterable<Accum> accums)} {
   *     Accum merged = createAccumulator();
   *     for (Accum accum : accums) {
   *       merged.sum += accum.sum;
   *       merged.count += accum.count;
   *     }
   *     return merged;
   *   }
   *
   *   public Double extractOutput(Accum accum) {
   *     return ((double) accum.sum) / accum.count;
   *   }
   * }{@literal
   * PCollection<Integer> pc = ...;
   * PCollection<Double> average = pc.apply(Combine.globally(new AverageFn()));
   * }</code></pre>
   *
   * <p>Combining functions used by {@link Combine.Globally}, {@link Combine.PerKey}, {@link
   * Combine.GroupedValues}, and {@code PTransforms} derived from them should be <i>associative</i>
   * and <i>commutative</i>. Associativity is required because input values are first broken up into
   * subgroups before being combined, and their intermediate results further combined, in an
   * arbitrary tree structure. Commutativity is required because any order of the input values is
   * ignored when breaking up input values into groups.
   *
   * <h3>Note on Data Encoding</h3>
   *
   * <p>Some form of data encoding is required when using custom types in a CombineFn which do not
   * have well-known coders. The sample code above uses a custom Accumulator which gets coder by
   * implementing {@link java.io.Serializable}. By doing this, we are relying on the generic {@link
   * org.apache.beam.sdk.coders.CoderProvider}, which is able to provide a coder for any {@link
   * java.io.Serializable} if applicable. In cases where {@link java.io.Serializable} is not
   * efficient, or inapplicable, in general there are two alternatives for encoding:
   *
   * <ul>
   *   <li>Default {@link org.apache.beam.sdk.coders.CoderRegistry}. For example, implement a coder
   *       class explicitly and use the {@code @DefaultCoder} tag. See the {@link
   *       org.apache.beam.sdk.coders.CoderRegistry} for the numerous ways in which to bind a type
   *       to a coder.
   *   <li>CombineFn specific way. While extending CombineFn, overwrite both {@link
   *       #getAccumulatorCoder} and {@link #getDefaultOutputCoder}.
   * </ul>
   *
   * @param <InputT> type of input values
   * @param <AccumT> type of mutable accumulator values
   * @param <OutputT> type of output values
   */
  public abstract static class CombineFn<
          InputT extends @Nullable Object,
          AccumT extends @Nullable Object,
          OutputT extends @Nullable Object>
      extends AbstractGlobalCombineFn<InputT, AccumT, OutputT> {

    /**
     * Returns a new, mutable accumulator value, representing the accumulation of zero input values.
     */
    public abstract AccumT createAccumulator();

    /**
     * Adds the given input value to the given accumulator, returning the new accumulator value.
     *
     * @param mutableAccumulator may be modified and returned for efficiency
     * @param input should not be mutated
     */
    public abstract AccumT addInput(AccumT mutableAccumulator, InputT input);

    /**
     * Returns an accumulator representing the accumulation of all the input values accumulated in
     * the merging accumulators.
     *
     * @param accumulators only the first accumulator may be modified and returned for efficiency;
     *     the other accumulators should not be mutated, because they may be shared with other code
     *     and mutating them could lead to incorrect results or data corruption.
     */
    public abstract AccumT mergeAccumulators(Iterable<AccumT> accumulators);

    /**
     * Returns the output value that is the result of combining all the input values represented by
     * the given accumulator.
     *
     * @param accumulator can be modified for efficiency
     */
    public abstract OutputT extractOutput(AccumT accumulator);

    /**
     * Returns an accumulator that represents the same logical value as the input accumulator, but
     * may have a more compact representation.
     *
     * <p>For most CombineFns this would be a no-op, but should be overridden by CombineFns that
     * (for example) buffer up elements and combine them in batches.
     *
     * <p>For efficiency, the input accumulator may be modified and returned.
     *
     * <p>By default returns the original accumulator.
     */
    public AccumT compact(AccumT accumulator) {
      return accumulator;
    }

    /**
     * Applies this {@code CombineFn} to a collection of input values to produce a combined output
     * value.
     *
     * <p>Useful when using a {@code CombineFn} separately from a {@code Combine} transform. Does
     * not invoke the {@link #mergeAccumulators} operation.
     */
    public OutputT apply(Iterable<? extends InputT> inputs) {
      AccumT accum = createAccumulator();
      for (InputT input : inputs) {
        accum = addInput(accum, input);
      }
      return extractOutput(accum);
    }

    /**
     * {@inheritDoc}
     *
     * <p>By default returns the extract output of an empty accumulator.
     */
    @Override
    public OutputT defaultValue() {
      return extractOutput(createAccumulator());
    }

    /**
     * Returns a {@link TypeDescriptor} capturing what is known statically about the output type of
     * this {@code CombineFn} instance's most-derived class.
     *
     * <p>In the normal case of a concrete {@code CombineFn} subclass with no generic type
     * parameters of its own, this will be a complete non-generic type.
     */
    public TypeDescriptor<OutputT> getOutputType() {
      return new TypeDescriptor<OutputT>(getClass()) {};
    }

    /**
     * Returns a {@link TypeDescriptor} capturing what is known statically about the input type of
     * this {@code CombineFn} instance's most-derived class.
     *
     * <p>In the normal case of a concrete {@code CombineFn} subclass with no generic type
     * parameters of its own, this will be a complete non-generic type.
     */
    public TypeDescriptor<InputT> getInputType() {
      return new TypeDescriptor<InputT>(getClass()) {};
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * An abstract subclass of {@link CombineFn} for implementing combiners that are more easily
   * expressed as binary operations.
   */
  public abstract static class BinaryCombineFn<V> extends CombineFn<V, Holder<V>, V> {

    /**
     * Returns a {@code CombineFn} that uses the given {@code SerializableBiFunction} to combine
     * values.
     */
    public static <V> BinaryCombineFn<V> of(SerializableBiFunction<V, V, V> combiner) {
      return new BinaryCombineFn<V>() {
        @Override
        public V apply(V left, V right) {
          return combiner.apply(left, right);
        }
      };
    }

    /** Applies the binary operation to the two operands, returning the result. */
    public abstract V apply(V left, V right);

    /** Returns the value that should be used for the combine of the empty set. */
    public @Nullable V identity() {
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
      Iterator<Holder<V>> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator();
      } else {
        Holder<V> running = iter.next();
        while (iter.hasNext()) {
          Holder<V> accum = iter.next();
          if (accum.present) {
            if (running.present) {
              running.set(apply(running.value, accum.value));
            } else {
              running.set(accum.value);
            }
          }
        }
        return running;
      }
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
    public Coder<Holder<V>> getAccumulatorCoder(CoderRegistry registry, Coder<V> inputCoder) {
      return new HolderCoder<>(inputCoder);
    }

    @Override
    public Coder<V> getDefaultOutputCoder(CoderRegistry registry, Coder<V> inputCoder) {
      return inputCoder;
    }
  }

  /**
   * Holds a single value value of type {@code V} which may or may not be present.
   *
   * <p>Used only as a private accumulator class.
   */
  public static class Holder<V> {
    private @Nullable V value;
    private boolean present;

    private Holder() {}

    private Holder(V value) {
      set(value);
    }

    private void set(V value) {
      this.present = true;
      this.value = value;
    }

    @Override
    public String toString() {
      return "Combine.Holder(value=" + value + ", present=" + present + ")";
    }
  }

  /** A {@link Coder} for a {@link Holder}. */
  private static class HolderCoder<V> extends StructuredCoder<Holder<V>> {

    private Coder<V> valueCoder;

    public HolderCoder(Coder<V> valueCoder) {
      this.valueCoder = valueCoder;
    }

    @Override
    public void encode(Holder<V> accumulator, OutputStream outStream)
        throws CoderException, IOException {
      encode(accumulator, outStream, Coder.Context.NESTED);
    }

    @Override
    public void encode(Holder<V> accumulator, OutputStream outStream, Coder.Context context)
        throws CoderException, IOException {
      if (accumulator.present) {
        outStream.write(1);
        valueCoder.encode(accumulator.value, outStream, context);
      } else {
        outStream.write(0);
      }
    }

    @Override
    public Holder<V> decode(InputStream inStream) throws CoderException, IOException {
      return decode(inStream, Coder.Context.NESTED);
    }

    @Override
    public Holder<V> decode(InputStream inStream, Coder.Context context)
        throws CoderException, IOException {
      if (inStream.read() == 1) {
        return new Holder<>(valueCoder.decode(inStream, context));
      } else {
        return new Holder<>();
      }
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.singletonList(valueCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      valueCoder.verifyDeterministic();
    }
  }

  /**
   * An abstract subclass of {@link CombineFn} for implementing combiners that are more easily and
   * efficiently expressed as binary operations on <code>int</code>s
   *
   * <p>It uses {@code int[0]} as the mutable accumulator.
   */
  public abstract static class BinaryCombineIntegerFn extends CombineFn<Integer, int[], Integer> {

    /** Applies the binary operation to the two operands, returning the result. */
    public abstract int apply(int left, int right);

    /**
     * Returns the identity element of this operation, i.e. an element {@code e} such that {@code
     * apply(e, x) == apply(x, e) == x} for all values of {@code x}.
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
        int[] running = iter.next();
        while (iter.hasNext()) {
          running[0] = apply(running[0], iter.next()[0]);
        }
        return running;
      }
    }

    @Override
    public Integer extractOutput(int[] accumulator) {
      return accumulator[0];
    }

    @Override
    public Coder<int[]> getAccumulatorCoder(CoderRegistry registry, Coder<Integer> inputCoder) {
      return DelegateCoder.of(
          inputCoder, new ToIntegerCodingFunction(), new FromIntegerCodingFunction());
    }

    @Override
    public Coder<Integer> getDefaultOutputCoder(CoderRegistry registry, Coder<Integer> inputCoder) {
      return inputCoder;
    }

    private static int[] wrap(int value) {
      return new int[] {value};
    }

    private static final class ToIntegerCodingFunction
        implements DelegateCoder.CodingFunction<int[], Integer> {
      @Override
      public Integer apply(int[] accumulator) {
        return accumulator[0];
      }

      @Override
      public boolean equals(@Nullable Object o) {
        return o instanceof ToIntegerCodingFunction;
      }

      @Override
      public int hashCode() {
        return this.getClass().hashCode();
      }
    }

    private static final class FromIntegerCodingFunction
        implements DelegateCoder.CodingFunction<Integer, int[]> {
      @Override
      public int[] apply(Integer value) {
        return wrap(value);
      }

      @Override
      public boolean equals(@Nullable Object o) {
        return o instanceof FromIntegerCodingFunction;
      }

      @Override
      public int hashCode() {
        return this.getClass().hashCode();
      }
    }
  }

  /**
   * An abstract subclass of {@link CombineFn} for implementing combiners that are more easily and
   * efficiently expressed as binary operations on <code>long</code>s.
   *
   * <p>It uses {@code long[0]} as the mutable accumulator.
   */
  public abstract static class BinaryCombineLongFn extends CombineFn<Long, long[], Long> {
    /** Applies the binary operation to the two operands, returning the result. */
    public abstract long apply(long left, long right);

    /**
     * Returns the identity element of this operation, i.e. an element {@code e} such that {@code
     * apply(e, x) == apply(x, e) == x} for all values of {@code x}.
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
        long[] running = iter.next();
        while (iter.hasNext()) {
          running[0] = apply(running[0], iter.next()[0]);
        }
        return running;
      }
    }

    @Override
    public Long extractOutput(long[] accumulator) {
      return accumulator[0];
    }

    @Override
    public Coder<long[]> getAccumulatorCoder(CoderRegistry registry, Coder<Long> inputCoder) {
      return DelegateCoder.of(inputCoder, new ToLongCodingFunction(), new FromLongCodingFunction());
    }

    @Override
    public Coder<Long> getDefaultOutputCoder(CoderRegistry registry, Coder<Long> inputCoder) {
      return inputCoder;
    }

    private static long[] wrap(long value) {
      return new long[] {value};
    }

    private static final class ToLongCodingFunction
        implements DelegateCoder.CodingFunction<long[], Long> {
      @Override
      public Long apply(long[] accumulator) {
        return accumulator[0];
      }

      @Override
      public boolean equals(@Nullable Object o) {
        return o instanceof ToLongCodingFunction;
      }

      @Override
      public int hashCode() {
        return this.getClass().hashCode();
      }
    }

    private static final class FromLongCodingFunction
        implements DelegateCoder.CodingFunction<Long, long[]> {
      @Override
      public long[] apply(Long value) {
        return wrap(value);
      }

      @Override
      public boolean equals(@Nullable Object o) {
        return o instanceof FromLongCodingFunction;
      }

      @Override
      public int hashCode() {
        return this.getClass().hashCode();
      }
    }
  }

  /**
   * An abstract subclass of {@link CombineFn} for implementing combiners that are more easily and
   * efficiently expressed as binary operations on <code>double</code>s.
   *
   * <p>It uses {@code double[0]} as the mutable accumulator.
   */
  public abstract static class BinaryCombineDoubleFn extends CombineFn<Double, double[], Double> {

    /** Applies the binary operation to the two operands, returning the result. */
    public abstract double apply(double left, double right);

    /**
     * Returns the identity element of this operation, i.e. an element {@code e} such that {@code
     * apply(e, x) == apply(x, e) == x} for all values of {@code x}.
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
        double[] running = iter.next();
        while (iter.hasNext()) {
          running[0] = apply(running[0], iter.next()[0]);
        }
        return running;
      }
    }

    @Override
    public Double extractOutput(double[] accumulator) {
      return accumulator[0];
    }

    @Override
    public Coder<double[]> getAccumulatorCoder(CoderRegistry registry, Coder<Double> inputCoder) {
      return DelegateCoder.of(
          inputCoder, new ToDoubleCodingFunction(), new FromDoubleCodingFunction());
    }

    @Override
    public Coder<Double> getDefaultOutputCoder(CoderRegistry registry, Coder<Double> inputCoder) {
      return inputCoder;
    }

    private static double[] wrap(double value) {
      return new double[] {value};
    }

    private static final class ToDoubleCodingFunction
        implements DelegateCoder.CodingFunction<double[], Double> {
      @Override
      public Double apply(double[] accumulator) {
        return accumulator[0];
      }

      @Override
      public boolean equals(@Nullable Object o) {
        return o instanceof ToDoubleCodingFunction;
      }

      @Override
      public int hashCode() {
        return this.getClass().hashCode();
      }
    }

    private static final class FromDoubleCodingFunction
        implements DelegateCoder.CodingFunction<Double, double[]> {
      @Override
      public double[] apply(Double value) {
        return wrap(value);
      }

      @Override
      public boolean equals(@Nullable Object o) {
        return o instanceof FromDoubleCodingFunction;
      }

      @Override
      public int hashCode() {
        return this.getClass().hashCode();
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@code CombineFn} that uses a subclass of {@link AccumulatingCombineFn.Accumulator} as its
   * accumulator type. By defining the operations of the {@code Accumulator} helper class, the
   * operations of the enclosing {@code CombineFn} are automatically provided. This can reduce the
   * code required to implement a {@code CombineFn}.
   *
   * <p>For example, the example from {@link CombineFn} above can be expressed using {@code
   * AccumulatingCombineFn} more concisely as follows:
   *
   * <pre>{@code
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
   * }</pre>
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

    /** The type of mutable accumulator values used by this {@code AccumulatingCombineFn}. */
    public interface Accumulator<InputT, AccumT, OutputT> {
      /** Adds the given input value to this accumulator, modifying this accumulator. */
      void addInput(InputT input);

      /** Adds the input values represented by the given accumulator into this accumulator. */
      void mergeAccumulator(AccumT other);

      /**
       * Returns the output value that is the result of combining all the input values represented
       * by this accumulator.
       */
      OutputT extractOutput();
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

  ////////////////////////////////////////////////////////////////////////////

  /**
   * {@code Combine.Globally<InputT, OutputT>} takes a {@code PCollection<InputT>} and returns a
   * {@code PCollection<OutputT>} whose elements are the result of combining all the elements in
   * each window of the input {@code PCollection}, using a specified {@link CombineFn
   * CombineFn&lt;InputT, AccumT, OutputT&gt;}. It is common for {@code InputT == OutputT}, but not
   * required. Common combining functions include sums, mins, maxes, and averages of numbers,
   * conjunctions and disjunctions of booleans, statistical aggregations, etc.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<Integer> pc = ...;
   * PCollection<Integer> sum = pc.apply(
   *     Combine.globally(new Sum.SumIntegerFn()));
   * }</pre>
   *
   * <p>Combining can happen in parallel, with different subsets of the input {@code PCollection}
   * being combined separately, and their intermediate results combined further, in an arbitrary
   * tree reduction pattern, until a single result value is produced.
   *
   * <p>If the input {@code PCollection} is windowed into {@link GlobalWindows}, a default value in
   * the {@link GlobalWindow} will be output if the input {@code PCollection} is empty. To use this
   * with inputs with other windowing, either {@link #withoutDefaults} or {@link #asSingletonView}
   * must be called, as the default value cannot be automatically assigned to any single window.
   *
   * <p>By default, the {@code Coder} of the output {@code PValue<OutputT>} is inferred from the
   * concrete type of the {@code CombineFn<InputT, AccumT, OutputT>}'s output type {@code OutputT}.
   *
   * <p>See also {@link #perKey}/{@link PerKey Combine.PerKey} and {@link #groupedValues}/{@link
   * GroupedValues Combine.GroupedValues}, which are useful for combining values associated with
   * each key in a {@code PCollection} of {@code KV}s.
   *
   * @param <InputT> type of input values
   * @param <OutputT> type of output values
   */
  public static class Globally<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollection<OutputT>> {

    private final GlobalCombineFn<? super InputT, ?, OutputT> fn;
    private final DisplayData.ItemSpec<? extends Class<?>> fnDisplayData;
    private final boolean insertDefault;
    private final int fanout;
    private final List<PCollectionView<?>> sideInputs;

    private Globally(
        GlobalCombineFn<? super InputT, ?, OutputT> fn,
        DisplayData.ItemSpec<? extends Class<?>> fnDisplayData,
        boolean insertDefault,
        int fanout,
        List<PCollectionView<?>> sideInputs) {
      this.fn = fn;
      this.fnDisplayData = fnDisplayData;
      this.insertDefault = insertDefault;
      this.fanout = fanout;
      this.sideInputs = sideInputs;
    }

    @Override
    protected String getKindString() {
      return String.format("Combine.globally(%s)", NameUtils.approximateSimpleName(fn));
    }

    /**
     * Returns a {@link PTransform} that produces a {@code PCollectionView} whose elements are the
     * result of combining elements per-window in the input {@code PCollection}. If a value is
     * requested from the view for a window that is not present, the result of applying the {@code
     * CombineFn} to an empty input set will be returned.
     */
    public GloballyAsSingletonView<InputT, OutputT> asSingletonView() {
      return new GloballyAsSingletonView<>(fn, fnDisplayData, insertDefault, fanout);
    }

    /**
     * Returns a {@link PTransform} identical to this, but that does not attempt to provide a
     * default value in the case of empty input. Required when the input is not globally windowed
     * and the output is not being used as a side input.
     */
    public Globally<InputT, OutputT> withoutDefaults() {
      return new Globally<>(fn, fnDisplayData, false, fanout, sideInputs);
    }

    /**
     * Returns a {@link PTransform} identical to this, but that uses an intermediate node to combine
     * parts of the data to reduce load on the final global combine step.
     *
     * <p>The {@code fanout} parameter determines the number of intermediate keys that will be used.
     */
    public Globally<InputT, OutputT> withFanout(int fanout) {
      return new Globally<>(fn, fnDisplayData, insertDefault, fanout, sideInputs);
    }

    /**
     * Returns a {@link PTransform} identical to this, but with the specified side inputs to use in
     * {@link CombineFnWithContext}.
     */
    public Globally<InputT, OutputT> withSideInputs(PCollectionView<?>... sideInputs) {
      return withSideInputs(Arrays.asList(sideInputs));
    }

    /**
     * Returns a {@link PTransform} identical to this, but with the specified side inputs to use in
     * {@link CombineFnWithContext}.
     */
    public Globally<InputT, OutputT> withSideInputs(
        Iterable<? extends PCollectionView<?>> sideInputs) {
      checkState(fn instanceof RequiresContextInternal);
      return new Globally<>(
          fn, fnDisplayData, insertDefault, fanout, ImmutableList.copyOf(sideInputs));
    }

    /** Returns the {@link GlobalCombineFn} used by this Combine operation. */
    public GlobalCombineFn<? super InputT, ?, OutputT> getFn() {
      return fn;
    }

    /** Returns the side inputs used by this Combine operation. */
    public List<PCollectionView<?>> getSideInputs() {
      return sideInputs;
    }

    /**
     * Returns the side inputs of this {@link Combine}, tagged with the tag of the {@link
     * PCollectionView}. The values of the returned map will be equal to the result of {@link
     * #getSideInputs()}.
     */
    @Override
    public Map<TupleTag<?>, PValue> getAdditionalInputs() {
      return PCollectionViews.toAdditionalInputs(sideInputs);
    }

    /** Returns whether or not this transformation applies a default value. */
    public boolean isInsertDefault() {
      return insertDefault;
    }

    @Override
    public PCollection<OutputT> expand(PCollection<InputT> input) {
      PCollection<KV<Void, InputT>> withKeys =
          input
              .apply(WithKeys.of((Void) null))
              .setCoder(KvCoder.of(VoidCoder.of(), input.getCoder()));

      Combine.PerKey<Void, InputT, OutputT> combine = Combine.fewKeys(fn, fnDisplayData);
      if (!sideInputs.isEmpty()) {
        combine = combine.withSideInputs(sideInputs);
      }

      PCollection<KV<Void, OutputT>> combined;
      if (fanout >= 2) {
        combined = withKeys.apply(combine.withHotKeyFanout(fanout));
      } else {
        combined = withKeys.apply(combine);
      }

      PCollection<OutputT> output = combined.apply(Values.create());

      if (insertDefault) {
        if (!output.getWindowingStrategy().getWindowFn().isCompatible(new GlobalWindows())) {
          throw new IllegalStateException(fn.getIncompatibleGlobalWindowErrorMessage());
        }
        return insertDefaultValueIfEmpty(output);
      } else {
        return output;
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      Combine.populateDisplayData(builder, fn, fnDisplayData);
      Combine.populateGlobalDisplayData(builder, fanout, insertDefault);
    }

    private PCollection<OutputT> insertDefaultValueIfEmpty(PCollection<OutputT> maybeEmpty) {
      final PCollectionView<Iterable<OutputT>> maybeEmptyView = maybeEmpty.apply(View.asIterable());

      final OutputT defaultValue = fn.defaultValue();
      PCollection<OutputT> defaultIfEmpty =
          maybeEmpty
              .getPipeline()
              .apply("CreateVoid", Create.of((Void) null).withCoder(VoidCoder.of()))
              .apply(
                  "ProduceDefault",
                  ParDo.of(
                          new DoFn<Void, OutputT>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                              Iterator<OutputT> combined = c.sideInput(maybeEmptyView).iterator();
                              if (!combined.hasNext()) {
                                c.output(defaultValue);
                              }
                            }
                          })
                      .withSideInputs(maybeEmptyView))
              .setCoder(maybeEmpty.getCoder())
              .setWindowingStrategyInternal(maybeEmpty.getWindowingStrategy());

      return PCollectionList.of(maybeEmpty).and(defaultIfEmpty).apply(Flatten.pCollections());
    }
  }

  private static void populateDisplayData(
      DisplayData.Builder builder,
      HasDisplayData fn,
      DisplayData.ItemSpec<? extends Class<?>> fnDisplayItem) {
    builder.include("combineFn", fn).add(fnDisplayItem);
  }

  private static void populateGlobalDisplayData(
      DisplayData.Builder builder, int fanout, boolean insertDefault) {
    builder
        .addIfNotDefault(DisplayData.item("fanout", fanout).withLabel("Key Fanout Size"), 0)
        .add(
            DisplayData.item("emitDefaultOnEmptyInput", insertDefault)
                .withLabel("Emit Default On Empty Input"));
  }

  /**
   * {@code Combine.GloballyAsSingletonView<InputT, OutputT>} takes a {@code PCollection<InputT>}
   * and returns a {@code PCollectionView<OutputT>} whose elements are the result of combining all
   * the elements in each window of the input {@code PCollection}, using a specified {@link
   * CombineFn CombineFn&lt;InputT, AccumT, OutputT&gt;}. It is common for {@code InputT ==
   * OutputT}, but not required. Common combining functions include sums, mins, maxes, and averages
   * of numbers, conjunctions and disjunctions of booleans, statistical aggregations, etc.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<Integer> pc = ...;
   * PCollection<Integer> sum = pc.apply(
   *     Combine.globally(new Sum.SumIntegerFn()));
   * }</pre>
   *
   * <p>Combining can happen in parallel, with different subsets of the input {@code PCollection}
   * being combined separately, and their intermediate results combined further, in an arbitrary
   * tree reduction pattern, until a single result value is produced.
   *
   * <p>If a value is requested from the view for a window that is not present and {@code
   * insertDefault} is true, the result of calling the {@code CombineFn} on empty input will
   * returned. If {@code insertDefault} is false, an exception will be thrown instead.
   *
   * <p>By default, the {@code Coder} of the output {@code PValue<OutputT>} is inferred from the
   * concrete type of the {@code CombineFn<InputT, AccumT, OutputT>}'s output type {@code OutputT}.
   *
   * <p>See also {@link #perKey}/{@link PerKey Combine.PerKey} and {@link #groupedValues}/{@link
   * GroupedValues Combine.GroupedValues}, which are useful for combining values associated with
   * each key in a {@code PCollection} of {@code KV}s.
   *
   * @param <InputT> type of input values
   * @param <OutputT> type of output values
   */
  public static class GloballyAsSingletonView<InputT, OutputT>
      extends PTransform<PCollection<InputT>, PCollectionView<OutputT>> {

    private final GlobalCombineFn<? super InputT, ?, OutputT> fn;
    private final DisplayData.ItemSpec<? extends Class<?>> fnDisplayData;
    private final boolean insertDefault;
    private final int fanout;

    private GloballyAsSingletonView(
        GlobalCombineFn<? super InputT, ?, OutputT> fn,
        DisplayData.ItemSpec<? extends Class<?>> fnDisplayData,
        boolean insertDefault,
        int fanout) {
      this.fn = fn;
      this.fnDisplayData = fnDisplayData;
      this.insertDefault = insertDefault;
      this.fanout = fanout;
    }

    @Override
    public PCollectionView<OutputT> expand(PCollection<InputT> input) {
      PCollection<OutputT> combined =
          input.apply(
              "CombineValues",
              Combine.<InputT, OutputT>globally(fn).withoutDefaults().withFanout(fanout));
      Coder<OutputT> outputCoder = combined.getCoder();
      PCollectionView<OutputT> view =
          PCollectionViews.singletonView(
              combined,
              (TypeDescriptorSupplier<OutputT>)
                  () -> outputCoder != null ? outputCoder.getEncodedTypeDescriptor() : null,
              input.getWindowingStrategy(),
              insertDefault,
              insertDefault ? fn.defaultValue() : null,
              combined.getCoder());
      combined.apply("CreatePCollectionView", CreatePCollectionView.of(view));
      return view;
    }

    public int getFanout() {
      return fanout;
    }

    public boolean getInsertDefault() {
      return insertDefault;
    }

    public GlobalCombineFn<? super InputT, ?, OutputT> getCombineFn() {
      return fn;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      Combine.populateDisplayData(builder, fn, fnDisplayData);
      Combine.populateGlobalDisplayData(builder, fanout, insertDefault);
    }
  }

  /**
   * Converts a {@link SerializableFunction} from {@code Iterable<V>}s to {@code V}s into a simple
   * {@link CombineFn} over {@code V}s.
   *
   * <p>Used in the implementation of convenience methods like {@link
   * #globally(SerializableFunction)}, {@link #perKey(SerializableFunction)}, and {@link
   * #groupedValues(SerializableFunction)}.
   */
  public static class IterableCombineFn<V> extends CombineFn<V, List<V>, V>
      implements NameOverride {
    /**
     * Returns a {@code CombineFn} that uses the given {@code SerializableFunction} to combine
     * values.
     */
    public static <V> IterableCombineFn<V> of(SerializableFunction<Iterable<V>, V> combiner) {
      return of(combiner, DEFAULT_BUFFER_SIZE);
    }

    /**
     * Returns a {@code CombineFn} that uses the given {@code SerializableFunction} to combine
     * values, attempting to buffer at least {@code bufferSize} values between invocations.
     */
    public static <V> IterableCombineFn<V> of(
        SerializableFunction<Iterable<V>, V> combiner, int bufferSize) {
      return new IterableCombineFn<>(combiner, bufferSize);
    }

    private static final int DEFAULT_BUFFER_SIZE = 20;

    /** The combiner function. */
    private final SerializableFunction<Iterable<V>, V> combiner;

    /** The number of values to accumulate before invoking the combiner function to combine them. */
    private final int bufferSize;

    private IterableCombineFn(SerializableFunction<Iterable<V>, V> combiner, int bufferSize) {
      this.combiner = combiner;
      this.bufferSize = bufferSize;
    }

    @Override
    public List<V> createAccumulator() {
      return new ArrayList<>();
    }

    @Override
    public List<V> addInput(List<V> accumulator, V input) {
      accumulator.add(input);
      if (accumulator.size() > bufferSize) {
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

    @Override
    public List<V> compact(List<V> accumulator) {
      return accumulator.size() > 1 ? mergeToSingleton(accumulator) : accumulator;
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder.add(DisplayData.item("combineFn", combiner.getClass()).withLabel("Combiner"));
    }

    private List<V> mergeToSingleton(Iterable<V> values) {
      List<V> singleton = new ArrayList<>();
      singleton.add(combiner.apply(values));
      return singleton;
    }

    @Override
    public String getNameOverride() {
      return NameUtils.approximateSimpleName(combiner);
    }
  }

  /**
   * Converts a {@link SerializableFunction} from {@code Iterable<V>}s to {@code V}s into a simple
   * {@link CombineFn} over {@code V}s.
   *
   * <p>@deprecated Use {@link IterableCombineFn} or the more space efficient {@link
   * BinaryCombineFn} instead (which avoids buffering values).
   */
  @Deprecated
  public static class SimpleCombineFn<V> extends IterableCombineFn<V> {

    /**
     * Returns a {@code CombineFn} that uses the given {@code SerializableFunction} to combine
     * values.
     */
    public static <V> SimpleCombineFn<V> of(SerializableFunction<Iterable<V>, V> combiner) {
      return new SimpleCombineFn<>(combiner);
    }

    protected SimpleCombineFn(SerializableFunction<Iterable<V>, V> combiner) {
      super(combiner, IterableCombineFn.DEFAULT_BUFFER_SIZE);
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * {@code PerKey<K, InputT, OutputT>} takes a {@code PCollection<KV<K, InputT>>}, groups it by
   * key, applies a combining function to the {@code InputT} values associated with each key to
   * produce a combined {@code OutputT} value, and returns a {@code PCollection<KV<K, OutputT>>}
   * representing a map from each distinct key of the input {@code PCollection} to the corresponding
   * combined value. {@code InputT} and {@code OutputT} are often the same.
   *
   * <p>This is a concise shorthand for an application of {@link GroupByKey} followed by an
   * application of {@link GroupedValues Combine.GroupedValues}. See those operations for more
   * details on how keys are compared for equality and on the default {@code Coder} for the output.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<KV<String, Double>> salesRecords = ...;
   * PCollection<KV<String, Double>> totalSalesPerPerson =
   *     salesRecords.apply(Combine.<String, Double, Double>perKey(
   *         Sum.ofDoubles()));
   * }</pre>
   *
   * <p>Each output element is in the window by which its corresponding input was grouped, and has
   * the timestamp of the end of that window. The output {@code PCollection} has the same {@link
   * org.apache.beam.sdk.transforms.windowing.WindowFn} as the input.
   *
   * @param <K> the type of the keys of the input and output {@code PCollection}s
   * @param <InputT> the type of the values of the input {@code PCollection}
   * @param <OutputT> the type of the values of the output {@code PCollection}
   */
  public static class PerKey<K, InputT, OutputT>
      extends PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> {

    private final GlobalCombineFn<? super InputT, ?, OutputT> fn;
    private final DisplayData.ItemSpec<? extends Class<?>> fnDisplayData;
    private final boolean fewKeys;
    private final List<PCollectionView<?>> sideInputs;

    private PerKey(
        GlobalCombineFn<? super InputT, ?, OutputT> fn,
        DisplayData.ItemSpec<? extends Class<?>> fnDisplayData,
        boolean fewKeys) {
      this.fn = fn;
      this.fnDisplayData = fnDisplayData;
      this.fewKeys = fewKeys;
      this.sideInputs = ImmutableList.of();
    }

    private PerKey(
        GlobalCombineFn<? super InputT, ?, OutputT> fn,
        DisplayData.ItemSpec<? extends Class<?>> fnDisplayData,
        boolean fewKeys,
        List<PCollectionView<?>> sideInputs) {
      this.fn = fn;
      this.fnDisplayData = fnDisplayData;
      this.fewKeys = fewKeys;
      this.sideInputs = sideInputs;
    }

    @Override
    protected String getKindString() {
      return String.format("Combine.perKey(%s)", NameUtils.approximateSimpleName(fn));
    }

    /**
     * Returns a {@link PTransform} identical to this, but with the specified side inputs to use in
     * {@link CombineFnWithContext}.
     */
    public PerKey<K, InputT, OutputT> withSideInputs(PCollectionView<?>... sideInputs) {
      return withSideInputs(Arrays.asList(sideInputs));
    }

    /**
     * Returns a {@link PTransform} identical to this, but with the specified side inputs to use in
     * {@link CombineFnWithContext}.
     */
    public PerKey<K, InputT, OutputT> withSideInputs(
        Iterable<? extends PCollectionView<?>> sideInputs) {
      checkState(fn instanceof RequiresContextInternal);
      return new PerKey<>(fn, fnDisplayData, fewKeys, ImmutableList.copyOf(sideInputs));
    }

    /**
     * If a single key has disproportionately many values, it may become a bottleneck, especially in
     * streaming mode. This returns a new per-key combining transform that inserts an intermediate
     * node to combine "hot" keys partially before performing the full combine.
     *
     * @param hotKeyFanout a function from keys to an integer N, where the key will be spread among
     *     N intermediate nodes for partial combining. If N is less than or equal to 1, this key
     *     will not be sent through an intermediate node.
     */
    public PerKeyWithHotKeyFanout<K, InputT, OutputT> withHotKeyFanout(
        SerializableFunction<? super K, Integer> hotKeyFanout) {
      return new PerKeyWithHotKeyFanout<>(fn, fnDisplayData, hotKeyFanout, fewKeys, sideInputs);
    }

    /**
     * Like {@link #withHotKeyFanout(SerializableFunction)}, but returning the given constant value
     * for every key.
     */
    public PerKeyWithHotKeyFanout<K, InputT, OutputT> withHotKeyFanout(final int hotKeyFanout) {
      return new PerKeyWithHotKeyFanout<>(
          fn,
          fnDisplayData,
          new SimpleFunction<K, Integer>() {
            @Override
            public void populateDisplayData(DisplayData.Builder builder) {
              super.populateDisplayData(builder);
              builder.add(DisplayData.item("fanout", hotKeyFanout).withLabel("Key Fanout Size"));
            }

            @Override
            public Integer apply(K unused) {
              return hotKeyFanout;
            }
          },
          fewKeys,
          sideInputs);
    }

    /** Returns the {@link GlobalCombineFn} used by this Combine operation. */
    public GlobalCombineFn<? super InputT, ?, OutputT> getFn() {
      return fn;
    }

    /** Returns the side inputs used by this Combine operation. */
    public List<PCollectionView<?>> getSideInputs() {
      return sideInputs;
    }

    /**
     * Returns the side inputs of this {@link Combine}, tagged with the tag of the {@link
     * PCollectionView}. The values of the returned map will be equal to the result of {@link
     * #getSideInputs()}.
     */
    @Override
    public Map<TupleTag<?>, PValue> getAdditionalInputs() {
      return PCollectionViews.toAdditionalInputs(sideInputs);
    }

    @Override
    public PCollection<KV<K, OutputT>> expand(PCollection<KV<K, InputT>> input) {
      return input
          .apply(fewKeys ? GroupByKey.createWithFewKeys() : GroupByKey.create())
          .apply(
              Combine.<K, InputT, OutputT>groupedValues(fn, fnDisplayData)
                  .withSideInputs(sideInputs));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      Combine.populateDisplayData(builder, fn, fnDisplayData);
    }
  }


  // Combiner for reducing key cardinality. Applies the child combiner
  // using numBuckets number of intermediate keys.
  public static class PerKeyWithBucketing<K, InputT, AccumT, OutputT>
      extends PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> {

    private final CombineFn<InputT, AccumT, OutputT> fn;
    // Actual Keys are mapped to one of the bucket and state is stored on the
    // bucket. Total number of buckets.
    private final int numBuckets;

    private PerKeyWithBucketing(CombineFn<InputT, AccumT, OutputT> fn, int numBuckets) {
      this.fn = fn;
      this.numBuckets = numBuckets;
    }

    @Override
    public PCollection<KV<K, OutputT>> expand(PCollection<KV<K, InputT>> input) {
      Coder<K> keyCoder = ((KvCoder<K, InputT>) input.getCoder()).getKeyCoder();
      CombineFn<KV<byte[], InputT>, Map<ByteBuffer, AccumT>, Map<ByteBuffer, OutputT>> combineFn =
          new CombineFn<KV<byte[], InputT>, Map<ByteBuffer, AccumT>, Map<ByteBuffer, OutputT>>() {
            @Override
            public Map<ByteBuffer, AccumT> createAccumulator() {
              return new HashMap<>();
            }

            @Override
            public Map<ByteBuffer, AccumT> addInput(
                Map<ByteBuffer, AccumT> mutableAccumulator, KV<byte[], InputT> input) {
              ByteBuffer key = ByteBuffer.wrap(input.getKey());
              AccumT accumT = mutableAccumulator.get(key);
              if (accumT == null) {
                accumT = fn.createAccumulator();
                mutableAccumulator.put(key, accumT);
              }
              fn.addInput(accumT, input.getValue());
              return mutableAccumulator;
            }

            @Override
            public Map<ByteBuffer, AccumT> mergeAccumulators(
                Iterable<Map<ByteBuffer, AccumT>> mapAccumulators) {
              HashMap<ByteBuffer, AccumT> mergedMap = new HashMap<>();
              for (Map<ByteBuffer, AccumT> mapAccumulator : mapAccumulators) {
                for (Entry<ByteBuffer, AccumT> entry : mapAccumulator.entrySet()) {
                  mergedMap.merge(
                      entry.getKey(),
                      entry.getValue(),
                      (accum1, accum2) -> fn.mergeAccumulators(Arrays.asList(accum1, accum2)));
                }
              }
              return mergedMap;
            }

            @Override
            public Map<ByteBuffer, OutputT> extractOutput(Map<ByteBuffer, AccumT> accumulator) {
              HashMap<ByteBuffer, OutputT> output = new HashMap<>();
              for (Entry<ByteBuffer, AccumT> entry : accumulator.entrySet()) {
                output.put(entry.getKey(), fn.extractOutput(entry.getValue()));
              }
              return output;
            }

            class ByteByfferCoder extends Coder<ByteBuffer> {

              private final ByteArrayCoder coder = ByteArrayCoder.of();

              @Override
              public void encode(ByteBuffer buffer, OutputStream outStream)
                  throws CoderException, IOException {
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                buffer.position(buffer.position() - bytes.length);
                coder.encode(bytes, outStream);
              }

              @Override
              public ByteBuffer decode(InputStream inStream) throws CoderException, IOException {
                return ByteBuffer.wrap(coder.decode(inStream));
              }

              @Override
              public List<? extends Coder<?>> getCoderArguments() {
                return coder.getCoderArguments();
              }

              @Override
              public void verifyDeterministic() throws NonDeterministicException {
                coder.verifyDeterministic();
              }
            }

            @Override
            public Coder<Map<ByteBuffer, AccumT>> getAccumulatorCoder(
                CoderRegistry registry, Coder<KV<byte[], InputT>> inputCoder)
                throws CannotProvideCoderException {
              return MapCoder.of(
                  new ByteByfferCoder(),
                  fn.getAccumulatorCoder(
                      registry, ((KvCoder<byte[], InputT>) inputCoder).getValueCoder()));
            }

            @Override
            public Coder<Map<ByteBuffer, OutputT>> getDefaultOutputCoder(
                CoderRegistry registry, Coder<KV<byte[], InputT>> inputCoder)
                throws CannotProvideCoderException {
              return MapCoder.of(
                  new ByteByfferCoder(),
                  fn.getDefaultOutputCoder(
                      registry, ((KvCoder<byte[], InputT>) inputCoder).getValueCoder()));
            }
          };
      try {
        return input
            .apply(
                MapElements.via(
                    new SimpleFunction<KV<K, InputT>, KV<Integer, KV<byte[], InputT>>>() {
                      @Override
                      public KV<Integer, KV<byte[], InputT>> apply(KV<K, InputT> input1) {
                        int bucket;
                        try {
                          // TODO: Find a better way to get fingerprint that doesn't require
                          // encoding key
                          ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                          keyCoder.encode(input1.getKey(), byteArrayOutputStream);
                          byte[] keyBytes = byteArrayOutputStream.toByteArray();
                          byteArrayOutputStream.reset();
                          bucket =
                              Hashing.consistentHash(
                                  Hashing.fingerprint2011().hashBytes(keyBytes), numBuckets);
                          return KV.of(bucket, KV.of(keyBytes, input1.getValue()));
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      }
                    }))
            .apply(Combine.perKey(combineFn))
            .apply(
                ParDo.of(
                    new DoFn<KV<Integer, Map<ByteBuffer, OutputT>>, KV<K, OutputT>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        try {
                          for (Entry<ByteBuffer, OutputT> entry :
                              c.element().getValue().entrySet()) {
                            c.output(
                                KV.of(
                                    keyCoder.decode(
                                        new ByteArrayInputStream(entry.getKey().array())),
                                    entry.getValue()));
                          }
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      }
                    }))
            .setCoder(
                KvCoder.of(
                    keyCoder,
                    fn.getDefaultOutputCoder(
                        CoderRegistry.createDefault(),
                        ((KvCoder<K, InputT>) input.getCoder()).getValueCoder())));
      } catch (CannotProvideCoderException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Like {@link PerKey}, but sharding the combining of hot keys. */
  public static class PerKeyWithHotKeyFanout<K, InputT, OutputT>
      extends PTransform<PCollection<KV<K, InputT>>, PCollection<KV<K, OutputT>>> {

    private final GlobalCombineFn<? super InputT, ?, OutputT> fn;
    private final DisplayData.ItemSpec<? extends Class<?>> fnDisplayData;
    private final SerializableFunction<? super K, Integer> hotKeyFanout;
    private final boolean fewKeys;
    private final List<PCollectionView<?>> sideInputs;

    private PerKeyWithHotKeyFanout(
        GlobalCombineFn<? super InputT, ?, OutputT> fn,
        DisplayData.ItemSpec<? extends Class<?>> fnDisplayData,
        SerializableFunction<? super K, Integer> hotKeyFanout,
        boolean fewKeys,
        List<PCollectionView<?>> sideInputs) {
      this.fn = fn;
      this.fnDisplayData = fnDisplayData;
      this.hotKeyFanout = hotKeyFanout;
      this.fewKeys = fewKeys;
      this.sideInputs = sideInputs;
    }

    @Override
    protected String getKindString() {
      return String.format("Combine.perKeyWithFanout(%s)", NameUtils.approximateSimpleName(fn));
    }

    @Override
    public PCollection<KV<K, OutputT>> expand(PCollection<KV<K, InputT>> input) {
      return applyHelper(input);
    }

    private <AccumT> PCollection<KV<K, OutputT>> applyHelper(PCollection<KV<K, InputT>> input) {

      // Name the accumulator type.
      @SuppressWarnings("unchecked")
      final GlobalCombineFn<InputT, AccumT, OutputT> typedFn =
          (GlobalCombineFn<InputT, AccumT, OutputT>) this.fn;

      if (!(input.getCoder() instanceof KvCoder)) {
        throw new IllegalStateException(
            "Expected input coder to be KvCoder, but was " + input.getCoder());
      }

      @SuppressWarnings("unchecked")
      final KvCoder<K, InputT> inputCoder = (KvCoder<K, InputT>) input.getCoder();
      final Coder<AccumT> accumCoder;

      try {
        accumCoder =
            typedFn.getAccumulatorCoder(
                input.getPipeline().getCoderRegistry(), inputCoder.getValueCoder());
      } catch (CannotProvideCoderException e) {
        throw new IllegalStateException("Unable to determine accumulator coder.", e);
      }
      Coder<InputOrAccum<InputT, AccumT>> inputOrAccumCoder =
          new InputOrAccum.InputOrAccumCoder<>(inputCoder.getValueCoder(), accumCoder);

      // A CombineFn's mergeAccumulator can be applied in a tree-like fashion.
      // Here we shard the key using an integer nonce, combine on that partial
      // set of values, then drop the nonce and do a final combine of the
      // aggregates.  We do this by splitting the original CombineFn into two,
      // on that does addInput + merge and another that does merge + extract.
      GlobalCombineFn<InputT, AccumT, AccumT> hotPreCombine;
      GlobalCombineFn<InputOrAccum<InputT, AccumT>, AccumT, OutputT> postCombine;
      if (typedFn instanceof CombineFn) {
        final CombineFn<InputT, AccumT, OutputT> fn = (CombineFn<InputT, AccumT, OutputT>) typedFn;
        hotPreCombine =
            new CombineFn<InputT, AccumT, AccumT>() {
              @Override
              public AccumT createAccumulator() {
                return fn.createAccumulator();
              }

              @Override
              public AccumT addInput(AccumT accumulator, InputT value) {
                return fn.addInput(accumulator, value);
              }

              @Override
              public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
                return fn.mergeAccumulators(accumulators);
              }

              @Override
              public AccumT compact(AccumT accumulator) {
                return fn.compact(accumulator);
              }

              @Override
              public AccumT extractOutput(AccumT accumulator) {
                return accumulator;
              }

              @Override
              @SuppressWarnings("unchecked")
              public Coder<AccumT> getAccumulatorCoder(
                  CoderRegistry registry, Coder<InputT> inputCoder)
                  throws CannotProvideCoderException {
                return accumCoder;
              }

              @Override
              public void populateDisplayData(DisplayData.Builder builder) {
                builder.delegate(PerKeyWithHotKeyFanout.this);
              }
            };

        postCombine =
            new CombineFn<InputOrAccum<InputT, AccumT>, AccumT, OutputT>() {
              @Override
              public AccumT createAccumulator() {
                return fn.createAccumulator();
              }

              @Override
              public AccumT addInput(AccumT accumulator, InputOrAccum<InputT, AccumT> value) {
                if (value.accum == null) {
                  return fn.addInput(accumulator, value.input);
                } else {
                  return fn.mergeAccumulators(ImmutableList.of(accumulator, value.accum));
                }
              }

              @Override
              public AccumT mergeAccumulators(Iterable<AccumT> accumulators) {
                return fn.mergeAccumulators(accumulators);
              }

              @Override
              public AccumT compact(AccumT accumulator) {
                return fn.compact(accumulator);
              }

              @Override
              public OutputT extractOutput(AccumT accumulator) {
                return fn.extractOutput(accumulator);
              }

              @Override
              public Coder<OutputT> getDefaultOutputCoder(
                  CoderRegistry registry, Coder<InputOrAccum<InputT, AccumT>> accumulatorCoder)
                  throws CannotProvideCoderException {
                return fn.getDefaultOutputCoder(registry, inputCoder.getValueCoder());
              }

              @Override
              public Coder<AccumT> getAccumulatorCoder(
                  CoderRegistry registry, Coder<InputOrAccum<InputT, AccumT>> inputCoder)
                  throws CannotProvideCoderException {
                return accumCoder;
              }

              @Override
              public void populateDisplayData(DisplayData.Builder builder) {
                builder.delegate(PerKeyWithHotKeyFanout.this);
              }
            };
      } else if (typedFn instanceof CombineFnWithContext) {
        final CombineFnWithContext<InputT, AccumT, OutputT> fnWithContext =
            (CombineFnWithContext<InputT, AccumT, OutputT>) typedFn;
        hotPreCombine =
            new CombineFnWithContext<InputT, AccumT, AccumT>() {
              @Override
              public AccumT createAccumulator(CombineWithContext.Context c) {
                return fnWithContext.createAccumulator(c);
              }

              @Override
              public AccumT addInput(
                  AccumT accumulator, InputT value, CombineWithContext.Context c) {
                return fnWithContext.addInput(accumulator, value, c);
              }

              @Override
              public AccumT mergeAccumulators(
                  Iterable<AccumT> accumulators, CombineWithContext.Context c) {
                return fnWithContext.mergeAccumulators(accumulators, c);
              }

              @Override
              public AccumT compact(AccumT accumulator, CombineWithContext.Context c) {
                return fnWithContext.compact(accumulator, c);
              }

              @Override
              public AccumT extractOutput(AccumT accumulator, CombineWithContext.Context c) {
                return accumulator;
              }

              @Override
              @SuppressWarnings("unchecked")
              public Coder<AccumT> getAccumulatorCoder(
                  CoderRegistry registry, Coder<InputT> inputCoder)
                  throws CannotProvideCoderException {
                return accumCoder;
              }

              @Override
              public void populateDisplayData(DisplayData.Builder builder) {
                builder.delegate(PerKeyWithHotKeyFanout.this);
              }
            };
        postCombine =
            new CombineFnWithContext<InputOrAccum<InputT, AccumT>, AccumT, OutputT>() {
              @Override
              public AccumT createAccumulator(CombineWithContext.Context c) {
                return fnWithContext.createAccumulator(c);
              }

              @Override
              public AccumT addInput(
                  AccumT accumulator,
                  InputOrAccum<InputT, AccumT> value,
                  CombineWithContext.Context c) {
                if (value.accum == null) {
                  return fnWithContext.addInput(accumulator, value.input, c);
                } else {
                  return fnWithContext.mergeAccumulators(
                      ImmutableList.of(accumulator, value.accum), c);
                }
              }

              @Override
              public AccumT mergeAccumulators(
                  Iterable<AccumT> accumulators, CombineWithContext.Context c) {
                return fnWithContext.mergeAccumulators(accumulators, c);
              }

              @Override
              public AccumT compact(AccumT accumulator, CombineWithContext.Context c) {
                return fnWithContext.compact(accumulator, c);
              }

              @Override
              public OutputT extractOutput(AccumT accumulator, CombineWithContext.Context c) {
                return fnWithContext.extractOutput(accumulator, c);
              }

              @Override
              public Coder<OutputT> getDefaultOutputCoder(
                  CoderRegistry registry, Coder<InputOrAccum<InputT, AccumT>> accumulatorCoder)
                  throws CannotProvideCoderException {
                return fnWithContext.getDefaultOutputCoder(registry, inputCoder.getValueCoder());
              }

              @Override
              public Coder<AccumT> getAccumulatorCoder(
                  CoderRegistry registry, Coder<InputOrAccum<InputT, AccumT>> inputCoder)
                  throws CannotProvideCoderException {
                return accumCoder;
              }

              @Override
              public void populateDisplayData(DisplayData.Builder builder) {
                builder.delegate(PerKeyWithHotKeyFanout.this);
              }
            };
      } else {
        throw new IllegalStateException(
            String.format("Unknown type of CombineFn: %s", typedFn.getClass()));
      }

      // Use the provided hotKeyFanout fn to split into "hot" and "cold" keys,
      // augmenting the hot keys with a nonce.
      final TupleTag<KV<KV<K, Integer>, InputT>> hot = new TupleTag<>();
      final TupleTag<KV<K, InputT>> cold = new TupleTag<>();
      PCollectionTuple split =
          input.apply(
              "AddNonce",
              ParDo.of(
                      new DoFn<KV<K, InputT>, KV<K, InputT>>() {
                        transient int nonce;

                        @StartBundle
                        public void startBundle() {
                          // Spreading a hot key across all possible sub-keys for all bundles
                          // would defeat the goal of not overwhelming downstream reducers
                          // (as well as making less efficient use of PGBK combining tables).
                          // Instead, each bundle independently makes a consistent choice about
                          // which "shard" of a key to send its intermediate results.
                          nonce = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
                        }

                        @ProcessElement
                        public void processElement(
                            @Element KV<K, InputT> kv, MultiOutputReceiver receiver) {
                          int spread = hotKeyFanout.apply(kv.getKey());
                          if (spread <= 1) {
                            receiver.get(cold).output(kv);
                          } else {
                            receiver
                                .get(hot)
                                .output(KV.of(KV.of(kv.getKey(), nonce % spread), kv.getValue()));
                          }
                        }
                      })
                  .withOutputTags(cold, TupleTagList.of(hot)));

      // The first level of combine should never use accumulating mode.
      WindowingStrategy<?, ?> preCombineStrategy = input.getWindowingStrategy();
      if (preCombineStrategy.getMode()
          == WindowingStrategy.AccumulationMode.ACCUMULATING_FIRED_PANES) {
        preCombineStrategy =
            preCombineStrategy.withMode(WindowingStrategy.AccumulationMode.DISCARDING_FIRED_PANES);
      }

      // Combine the hot and cold keys separately.
      Combine.PerKey<KV<K, Integer>, InputT, AccumT> hotPreCombineTransform =
          fewKeys
              ? Combine.fewKeys(hotPreCombine, fnDisplayData)
              : Combine.perKey(hotPreCombine, fnDisplayData);
      if (!sideInputs.isEmpty()) {
        hotPreCombineTransform = hotPreCombineTransform.withSideInputs(sideInputs);
      }

      PCollection<KV<K, InputOrAccum<InputT, AccumT>>> precombinedHot =
          split
              .get(hot)
              .setCoder(
                  KvCoder.of(
                      KvCoder.of(inputCoder.getKeyCoder(), VarIntCoder.of()),
                      inputCoder.getValueCoder()))
              .setWindowingStrategyInternal(preCombineStrategy)
              .apply("PreCombineHot", hotPreCombineTransform)
              .apply(
                  "StripNonce",
                  MapElements.via(
                      new SimpleFunction<
                          KV<KV<K, Integer>, AccumT>, KV<K, InputOrAccum<InputT, AccumT>>>() {
                        @Override
                        public KV<K, InputOrAccum<InputT, AccumT>> apply(
                            KV<KV<K, Integer>, AccumT> elem) {
                          return KV.of(
                              elem.getKey().getKey(),
                              InputOrAccum.<InputT, AccumT>accum(elem.getValue()));
                        }
                      }))
              .setCoder(KvCoder.of(inputCoder.getKeyCoder(), inputOrAccumCoder))
              .apply(Window.remerge())
              .setWindowingStrategyInternal(input.getWindowingStrategy());
      PCollection<KV<K, InputOrAccum<InputT, AccumT>>> preprocessedCold =
          split
              .get(cold)
              .setCoder(inputCoder)
              .apply(
                  "PrepareCold",
                  MapElements.via(
                      new SimpleFunction<KV<K, InputT>, KV<K, InputOrAccum<InputT, AccumT>>>() {
                        @Override
                        public KV<K, InputOrAccum<InputT, AccumT>> apply(KV<K, InputT> element) {
                          return KV.of(
                              element.getKey(),
                              InputOrAccum.<InputT, AccumT>input(element.getValue()));
                        }
                      }))
              .setCoder(KvCoder.of(inputCoder.getKeyCoder(), inputOrAccumCoder));

      // Combine the union of the pre-processed hot and cold key results.
      Combine.PerKey<K, InputOrAccum<InputT, AccumT>, OutputT> postCombineTransform =
          fewKeys
              ? Combine.fewKeys(postCombine, fnDisplayData)
              : Combine.perKey(postCombine, fnDisplayData);
      if (!sideInputs.isEmpty()) {
        postCombineTransform = postCombineTransform.withSideInputs(sideInputs);
      }

      return PCollectionList.of(precombinedHot)
          .and(preprocessedCold)
          .apply(Flatten.pCollections())
          .apply("PostCombine", postCombineTransform);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);

      Combine.populateDisplayData(builder, fn, fnDisplayData);
      if (hotKeyFanout instanceof HasDisplayData) {
        builder.include("hotKeyFanout", (HasDisplayData) hotKeyFanout);
      }
      builder.add(
          DisplayData.item("fanoutFn", hotKeyFanout.getClass()).withLabel("Fanout Function"));
    }

    /** Returns the side inputs used by this Combine operation. */
    public List<PCollectionView<?>> getSideInputs() {
      return sideInputs;
    }

    /**
     * Used to store either an input or accumulator value, for flattening the hot and cold key
     * paths.
     */
    private static class InputOrAccum<InputT, AccumT> {
      public final @Nullable InputT input;
      public final @Nullable AccumT accum;

      private InputOrAccum(@Nullable InputT input, @Nullable AccumT aggr) {
        this.input = input;
        this.accum = aggr;
      }

      public static <InputT, AccumT> InputOrAccum<InputT, AccumT> input(InputT input) {
        return new InputOrAccum<>(input, null);
      }

      public static <InputT, AccumT> InputOrAccum<InputT, AccumT> accum(AccumT aggr) {
        return new InputOrAccum<>(null, aggr);
      }

      private static class InputOrAccumCoder<InputT, AccumT>
          extends StructuredCoder<InputOrAccum<InputT, AccumT>> {

        private final Coder<InputT> inputCoder;
        private final Coder<AccumT> accumCoder;

        public InputOrAccumCoder(Coder<InputT> inputCoder, Coder<AccumT> accumCoder) {
          this.inputCoder = inputCoder;
          this.accumCoder = accumCoder;
        }

        @Override
        public void encode(InputOrAccum<InputT, AccumT> value, OutputStream outStream)
            throws CoderException, IOException {
          encode(value, outStream, Coder.Context.NESTED);
        }

        @Override
        public void encode(
            InputOrAccum<InputT, AccumT> value, OutputStream outStream, Coder.Context context)
            throws CoderException, IOException {
          if (value.input != null) {
            outStream.write(0);
            inputCoder.encode(value.input, outStream, context);
          } else {
            outStream.write(1);
            accumCoder.encode(value.accum, outStream, context);
          }
        }

        @Override
        public InputOrAccum<InputT, AccumT> decode(InputStream inStream)
            throws CoderException, IOException {
          return decode(inStream, Coder.Context.NESTED);
        }

        @Override
        public InputOrAccum<InputT, AccumT> decode(InputStream inStream, Coder.Context context)
            throws CoderException, IOException {
          if (inStream.read() == 0) {
            return InputOrAccum.input(inputCoder.decode(inStream, context));
          } else {
            return InputOrAccum.accum(accumCoder.decode(inStream, context));
          }
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
          return ImmutableList.of(inputCoder, accumCoder);
        }

        @Override
        public void verifyDeterministic() throws Coder.NonDeterministicException {
          inputCoder.verifyDeterministic();
          accumCoder.verifyDeterministic();
        }
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * {@code GroupedValues<K, InputT, OutputT>} takes a {@code PCollection<KV<K, Iterable<InputT>>>},
   * such as the result of {@link GroupByKey}, applies a specified {@link CombineFn
   * CombineFn&lt;InputT, AccumT, OutputT&gt;} to each of the input {@code KV<K, Iterable<InputT>>}
   * elements to produce a combined output {@code KV<K, OutputT>} element, and returns a {@code
   * PCollection<KV<K, OutputT>>} containing all the combined output elements. It is common for
   * {@code InputT == OutputT}, but not required. Common combining functions include sums, mins,
   * maxes, and averages of numbers, conjunctions and disjunctions of booleans, statistical
   * aggregations, etc.
   *
   * <p>Example of use:
   *
   * <pre>{@code
   * PCollection<KV<String, Integer>> pc = ...;
   * PCollection<KV<String, Iterable<Integer>>> groupedByKey = pc.apply(
   *     GroupByKey.create());
   * PCollection<KV<String, Integer>> sumByKey =
   *     groupedByKey.apply(Combine.groupedValues(
   *         Sum.ofIntegers()));
   * }</pre>
   *
   * <p>See also {@link #perKey}/{@link PerKey Combine.PerKey}, which captures the common pattern of
   * "combining by key" in a single easy-to-use {@code PTransform}.
   *
   * <p>Combining for different keys can happen in parallel. Moreover, combining of the {@code
   * Iterable<InputT>} values associated a single key can happen in parallel, with different subsets
   * of the values being combined separately, and their intermediate results combined further, in an
   * arbitrary tree reduction pattern, until a single result value is produced for each key.
   *
   * <p>By default, the {@code Coder} of the keys of the output {@code PCollection<KV<K, OutputT>>}
   * is that of the keys of the input {@code PCollection<KV<K, InputT>>}, and the {@code Coder} of
   * the values of the output {@code PCollection<KV<K, OutputT>>} is inferred from the concrete type
   * of the {@code CombineFn<InputT, AccumT, OutputT>}'s output type {@code OutputT}.
   *
   * <p>Each output element has the same timestamp and is in the same window as its corresponding
   * input element, and the output {@code PCollection} has the same {@link
   * org.apache.beam.sdk.transforms.windowing.WindowFn} associated with it as the input.
   *
   * <p>See also {@link #globally}/{@link Globally Combine.Globally}, which combines all the values
   * in a {@code PCollection} into a single value in a {@code PCollection}.
   *
   * @param <K> type of input and output keys
   * @param <InputT> type of input values
   * @param <OutputT> type of output values
   */
  public static class GroupedValues<K, InputT, OutputT>
      extends PTransform<
          PCollection<? extends KV<K, ? extends Iterable<InputT>>>, PCollection<KV<K, OutputT>>> {

    private final GlobalCombineFn<? super InputT, ?, OutputT> fn;
    private final DisplayData.ItemSpec<? extends Class<?>> fnDisplayData;
    private final List<PCollectionView<?>> sideInputs;

    private GroupedValues(
        GlobalCombineFn<? super InputT, ?, OutputT> fn,
        DisplayData.ItemSpec<? extends Class<?>> fnDisplayData) {
      this.fn = SerializableUtils.clone(fn);
      this.fnDisplayData = fnDisplayData;
      this.sideInputs = ImmutableList.of();
    }

    private GroupedValues(
        GlobalCombineFn<? super InputT, ?, OutputT> fn,
        DisplayData.ItemSpec<? extends Class<?>> fnDisplayData,
        List<PCollectionView<?>> sideInputs) {
      this.fn = SerializableUtils.clone(fn);
      this.fnDisplayData = fnDisplayData;
      this.sideInputs = sideInputs;
    }

    public GroupedValues<K, InputT, OutputT> withSideInputs(PCollectionView<?>... sideInputs) {
      return withSideInputs(Arrays.asList(sideInputs));
    }

    public GroupedValues<K, InputT, OutputT> withSideInputs(
        Iterable<? extends PCollectionView<?>> sideInputs) {
      return new GroupedValues<>(fn, fnDisplayData, ImmutableList.copyOf(sideInputs));
    }

    /** Returns the {@link GlobalCombineFn} used by this Combine operation. */
    public GlobalCombineFn<? super InputT, ?, OutputT> getFn() {
      return fn;
    }

    public List<PCollectionView<?>> getSideInputs() {
      return sideInputs;
    }

    @Override
    public PCollection<KV<K, OutputT>> expand(
        PCollection<? extends KV<K, ? extends Iterable<InputT>>> input) {

      PCollection<KV<K, OutputT>> output =
          input.apply(
              ParDo.of(
                      new DoFn<KV<K, ? extends Iterable<InputT>>, KV<K, OutputT>>() {
                        @ProcessElement
                        public void processElement(final ProcessContext c) {
                          K key = c.element().getKey();

                          OutputT output;
                          if (fn instanceof CombineFnWithContext) {
                            output =
                                ((CombineFnWithContext<? super InputT, ?, OutputT>) fn)
                                    .apply(
                                        c.element().getValue(),
                                        new CombineWithContext.Context() {
                                          @Override
                                          public PipelineOptions getPipelineOptions() {
                                            return c.getPipelineOptions();
                                          }

                                          @Override
                                          public <T> T sideInput(PCollectionView<T> view) {
                                            return c.sideInput(view);
                                          }
                                        });
                          } else if (fn instanceof CombineFn) {
                            output =
                                ((CombineFn<? super InputT, ?, OutputT>) fn)
                                    .apply(c.element().getValue());
                          } else {
                            throw new IllegalStateException(
                                String.format("Unknown type of CombineFn: %s", fn.getClass()));
                          }
                          c.output(KV.of(key, output));
                        }

                        @Override
                        public void populateDisplayData(DisplayData.Builder builder) {
                          builder.delegate(Combine.GroupedValues.this);
                        }
                      })
                  .withSideInputs(sideInputs));

      try {
        KvCoder<K, InputT> kvCoder = getKvCoder(input.getCoder());
        @SuppressWarnings("unchecked")
        Coder<OutputT> outputValueCoder =
            ((GlobalCombineFn<InputT, ?, OutputT>) fn)
                .getDefaultOutputCoder(
                    input.getPipeline().getCoderRegistry(), kvCoder.getValueCoder());
        output.setCoder(KvCoder.of(kvCoder.getKeyCoder(), outputValueCoder));
      } catch (CannotProvideCoderException exc) {
        // let coder inference happen later, if it can
      }

      return output;
    }

    /**
     * Returns the {@link CombineFn} bound to its coders.
     *
     * <p>For internal use.
     */
    public AppliedCombineFn<? super K, ? super InputT, ?, OutputT> getAppliedFn(
        CoderRegistry registry,
        Coder<? extends KV<K, ? extends Iterable<InputT>>> inputCoder,
        WindowingStrategy<?, ?> windowingStrategy) {
      KvCoder<K, InputT> kvCoder = getKvCoder(inputCoder);
      return AppliedCombineFn.withInputCoder(fn, registry, kvCoder, sideInputs, windowingStrategy);
    }

    private KvCoder<K, InputT> getKvCoder(
        Coder<? extends KV<K, ? extends Iterable<InputT>>> inputCoder) {
      if (!(inputCoder instanceof KvCoder)) {
        throw new IllegalStateException("Combine.GroupedValues requires its input to use KvCoder");
      }
      @SuppressWarnings({"unchecked", "rawtypes"})
      KvCoder<K, ? extends Iterable<InputT>> kvCoder = (KvCoder) inputCoder;
      Coder<K> keyCoder = kvCoder.getKeyCoder();
      Coder<? extends Iterable<InputT>> kvValueCoder = kvCoder.getValueCoder();
      if (!(kvValueCoder instanceof IterableCoder)) {
        throw new IllegalStateException(
            "Combine.GroupedValues requires its input values to use " + "IterableCoder");
      }
      @SuppressWarnings("unchecked")
      IterableCoder<InputT> inputValuesCoder = (IterableCoder<InputT>) kvValueCoder;
      Coder<InputT> inputValueCoder = inputValuesCoder.getElemCoder();
      return KvCoder.of(keyCoder, inputValueCoder);
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      Combine.populateDisplayData(builder, fn, fnDisplayData);
    }
  }
}
