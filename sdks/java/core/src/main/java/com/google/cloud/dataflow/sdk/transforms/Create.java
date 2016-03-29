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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollection.IsBounded;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import com.google.cloud.dataflow.sdk.values.TimestampedValue.TimestampedValueCoder;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import org.joda.time.Instant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@code Create<T>} takes a collection of elements of type {@code T}
 * known when the pipeline is constructed and returns a
 * {@code PCollection<T>} containing the elements.
 *
 * <p>Example of use:
 * <pre> {@code
 * Pipeline p = ...;
 *
 * PCollection<Integer> pc = p.apply(Create.of(3, 4, 5).withCoder(BigEndianIntegerCoder.of()));
 *
 * Map<String, Integer> map = ...;
 * PCollection<KV<String, Integer>> pt =
 *     p.apply(Create.of(map)
 *      .withCoder(KvCoder.of(StringUtf8Coder.of(),
 *                            BigEndianIntegerCoder.of())));
 * } </pre>
 *
 * <p>{@code Create} can automatically determine the {@code Coder} to use
 * if all elements have the same run-time class, and a default coder is registered for that
 * class. See {@link CoderRegistry} for details on how defaults are determined.
 *
 * <p>If a coder can not be inferred, {@link Create.Values#withCoder} must be called
 * explicitly to set the encoding of the resulting
 * {@code PCollection}.
 *
 * <p>A good use for {@code Create} is when a {@code PCollection}
 * needs to be created without dependencies on files or other external
 * entities.  This is especially useful during testing.
 *
 * <p>Caveat: {@code Create} only supports small in-memory datasets,
 * particularly when submitting jobs to the Google Cloud Dataflow
 * service.
 *
 * @param <T> the type of the elements of the resulting {@code PCollection}
 */
public class Create<T> {
  /**
   * Returns a new {@code Create.Values} transform that produces a
   * {@link PCollection} containing elements of the provided
   * {@code Iterable}.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>The elements of the output {@link PCollection} will have a timestamp of negative infinity,
   * see {@link Create#timestamped} for a way of creating a {@code PCollection} with timestamped
   * elements.
   *
   * <p>By default, {@code Create.Values} can automatically determine the {@code Coder} to use
   * if all elements have the same non-parameterized run-time class, and a default coder is
   * registered for that class. See {@link CoderRegistry} for details on how defaults are
   * determined.
   * Otherwise, use {@link Create.Values#withCoder} to set the coder explicitly.
   */
  public static <T> Values<T> of(Iterable<T> elems) {
    return new Values<>(elems, Optional.<Coder<T>>absent());
  }

  /**
   * Returns a new {@code Create.Values} transform that produces a
   * {@link PCollection} containing the specified elements.
   *
   * <p>The elements will have a timestamp of negative infinity, see
   * {@link Create#timestamped} for a way of creating a {@code PCollection}
   * with timestamped elements.
   *
   * <p>The arguments should not be modified after this is called.
   *
   * <p>By default, {@code Create.Values} can automatically determine the {@code Coder} to use
   * if all elements have the same non-parameterized run-time class, and a default coder is
   * registered for that class. See {@link CoderRegistry} for details on how defaults are
   * determined.
   * Otherwise, use {@link Create.Values#withCoder} to set the coder explicitly.
   */
  @SafeVarargs
  public static <T> Values<T> of(T... elems) {
    return of(Arrays.asList(elems));
  }

  /**
   * Returns a new {@code Create.Values} transform that produces a
   * {@link PCollection} of {@link KV}s corresponding to the keys and
   * values of the specified {@code Map}.
   *
   * <p>The elements will have a timestamp of negative infinity, see
   * {@link Create#timestamped} for a way of creating a {@code PCollection}
   * with timestamped elements.
   *
   * <p>By default, {@code Create.Values} can automatically determine the {@code Coder} to use
   * if all elements have the same non-parameterized run-time class, and a default coder is
   * registered for that class. See {@link CoderRegistry} for details on how defaults are
   * determined.
   * Otherwise, use {@link Create.Values#withCoder} to set the coder explicitly.
   */
  public static <K, V> Values<KV<K, V>> of(Map<K, V> elems) {
    List<KV<K, V>> kvs = new ArrayList<>(elems.size());
    for (Map.Entry<K, V> entry : elems.entrySet()) {
      kvs.add(KV.of(entry.getKey(), entry.getValue()));
    }
    return of(kvs);
  }

  /**
   * Returns a new {@link Create.TimestampedValues} transform that produces a
   * {@link PCollection} containing the elements of the provided {@code Iterable}
   * with the specified timestamps.
   *
   * <p>The argument should not be modified after this is called.
   *
   * <p>By default, {@code Create.TimestampedValues} can automatically determine the {@code Coder}
   * to use if all elements have the same non-parameterized run-time class, and a default coder is
   * registered for that class. See {@link CoderRegistry} for details on how defaults are
   * determined.
   * Otherwise, use {@link Create.TimestampedValues#withCoder} to set the coder explicitly.
   */
  public static <T> TimestampedValues<T> timestamped(Iterable<TimestampedValue<T>> elems) {
    return new TimestampedValues<>(elems, Optional.<Coder<T>>absent());
  }

  /**
   * Returns a new {@link Create.TimestampedValues} transform that produces a {@link PCollection}
   * containing the specified elements with the specified timestamps.
   *
   * <p>The arguments should not be modified after this is called.
   */
  @SafeVarargs
  public static <T> TimestampedValues<T> timestamped(
      @SuppressWarnings("unchecked") TimestampedValue<T>... elems) {
    return timestamped(Arrays.asList(elems));
  }

  /**
   * Returns a new root transform that produces a {@link PCollection} containing
   * the specified elements with the specified timestamps.
   *
   * <p>The arguments should not be modified after this is called.
   *
   * <p>By default, {@code Create.TimestampedValues} can automatically determine the {@code Coder}
   * to use if all elements have the same non-parameterized run-time class, and a default coder
   * is registered for that class. See {@link CoderRegistry} for details on how defaults are
   * determined.
   * Otherwise, use {@link Create.TimestampedValues#withCoder} to set the coder explicitly.

   * @throws IllegalArgumentException if there are a different number of values
   * and timestamps
   */
  public static <T> TimestampedValues<T> timestamped(
      Iterable<T> values, Iterable<Long> timestamps) {
    List<TimestampedValue<T>> elems = new ArrayList<>();
    Iterator<T> valueIter = values.iterator();
    Iterator<Long> timestampIter = timestamps.iterator();
    while (valueIter.hasNext() && timestampIter.hasNext()) {
      elems.add(TimestampedValue.of(valueIter.next(), new Instant(timestampIter.next())));
    }
    Preconditions.checkArgument(
        !valueIter.hasNext() && !timestampIter.hasNext(),
        "Expect sizes of values and timestamps are same.");
    return timestamped(elems);
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@code PTransform} that creates a {@code PCollection} from a set of in-memory objects.
   */
  public static class Values<T> extends PTransform<PInput, PCollection<T>> {
    /**
     * Returns a {@link Create.Values} PTransform like this one that uses the given
     * {@code Coder<T>} to decode each of the objects into a
     * value of type {@code T}.
     *
     * <p>By default, {@code Create.Values} can automatically determine the {@code Coder} to use
     * if all elements have the same non-parameterized run-time class, and a default coder is
     * registered for that class. See {@link CoderRegistry} for details on how defaults are
     * determined.
     *
     * <p>Note that for {@link Create.Values} with no elements, the {@link VoidCoder} is used.
     */
    public Values<T> withCoder(Coder<T> coder) {
      return new Values<>(elems, Optional.of(coder));
    }

    public Iterable<T> getElements() {
      return elems;
    }

    @Override
    public PCollection<T> apply(PInput input) {
      try {
        Coder<T> coder = getDefaultOutputCoder(input);
        return PCollection
            .<T>createPrimitiveOutputInternal(
                input.getPipeline(),
                WindowingStrategy.globalDefault(),
                IsBounded.BOUNDED)
            .setCoder(coder);
      } catch (CannotProvideCoderException e) {
        throw new IllegalArgumentException("Unable to infer a coder and no Coder was specified. "
            + "Please set a coder by invoking Create.withCoder() explicitly.", e);
      }
    }

    @Override
    public Coder<T> getDefaultOutputCoder(PInput input) throws CannotProvideCoderException {
      if (coder.isPresent()) {
        return coder.get();
      }
      // First try to deduce a coder using the types of the elements.
      Class<?> elementClazz = Void.class;
      for (T elem : elems) {
        if (elem == null) {
          continue;
        }
        Class<?> clazz = elem.getClass();
        if (elementClazz.equals(Void.class)) {
          elementClazz = clazz;
        } else if (!elementClazz.equals(clazz)) {
          // Elements are not the same type, require a user-specified coder.
          throw new CannotProvideCoderException(
              "Cannot provide coder for Create: The elements are not all of the same class.");
        }
      }

      if (elementClazz.getTypeParameters().length == 0) {
        try {
          @SuppressWarnings("unchecked") // elementClazz is a wildcard type
          Coder<T> coder = (Coder<T>) input.getPipeline().getCoderRegistry()
              .getDefaultCoder(TypeDescriptor.of(elementClazz));
          return coder;
        } catch (CannotProvideCoderException exc) {
          // let the next stage try
        }
      }

      // If that fails, try to deduce a coder using the elements themselves
      Optional<Coder<T>> coder = Optional.absent();
      for (T elem : elems) {
        Coder<T> c = input.getPipeline().getCoderRegistry().getDefaultCoder(elem);
        if (!coder.isPresent()) {
          coder = Optional.of(c);
        } else if (!Objects.equals(c, coder.get())) {
          throw new CannotProvideCoderException(
              "Cannot provide coder for elements of " + Create.class.getSimpleName() + ":"
              + " For their common class, no coder could be provided."
              + " Based on their values, they do not all default to the same Coder.");
        }
      }

      if (!coder.isPresent()) {
        throw new CannotProvideCoderException("Unable to infer a coder. Please register "
            + "a coder for ");
      }
      return coder.get();
    }

    /////////////////////////////////////////////////////////////////////////////

    /** The elements of the resulting PCollection. */
    private final transient Iterable<T> elems;

    /** The coder used to encode the values to and from a binary representation. */
    private final transient Optional<Coder<T>> coder;

    /**
     * Constructs a {@code Create.Values} transform that produces a
     * {@link PCollection} containing the specified elements.
     *
     * <p>The arguments should not be modified after this is called.
     */
    private Values(Iterable<T> elems, Optional<Coder<T>> coder) {
      this.elems = elems;
      this.coder = coder;
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /**
   * A {@code PTransform} that creates a {@code PCollection} whose elements have
   * associated timestamps.
   */
  public static class TimestampedValues<T> extends Values<T> {
    /**
     * Returns a {@link Create.TimestampedValues} PTransform like this one that uses the given
     * {@code Coder<T>} to decode each of the objects into a
     * value of type {@code T}.
     *
     * <p>By default, {@code Create.TimestampedValues} can automatically determine the
     * {@code Coder} to use if all elements have the same non-parameterized run-time class,
     * and a default coder is registered for that class. See {@link CoderRegistry} for details
     * on how defaults are determined.
     *
     * <p>Note that for {@link Create.TimestampedValues with no elements}, the {@link VoidCoder}
     * is used.
     */
    @Override
    public TimestampedValues<T> withCoder(Coder<T> coder) {
      return new TimestampedValues<>(elems, Optional.<Coder<T>>of(coder));
    }

    @Override
    public PCollection<T> apply(PInput input) {
      try {
        Coder<T> coder = getDefaultOutputCoder(input);
        PCollection<TimestampedValue<T>> intermediate = Pipeline.applyTransform(input,
            Create.of(elems).withCoder(TimestampedValueCoder.of(coder)));

        PCollection<T> output = intermediate.apply(ParDo.of(new ConvertTimestamps<T>()));
        output.setCoder(coder);
        return output;
      } catch (CannotProvideCoderException e) {
        throw new IllegalArgumentException("Unable to infer a coder and no Coder was specified. "
            + "Please set a coder by invoking CreateTimestamped.withCoder() explicitly.", e);
      }
    }

    /////////////////////////////////////////////////////////////////////////////

    /** The timestamped elements of the resulting PCollection. */
    private final transient Iterable<TimestampedValue<T>> elems;

    private TimestampedValues(Iterable<TimestampedValue<T>> elems,
        Optional<Coder<T>> coder) {
      super(
          Iterables.transform(elems, new Function<TimestampedValue<T>, T>() {
            @Override
            public T apply(TimestampedValue<T> input) {
              return input.getValue();
            }
          }), coder);
      this.elems = elems;
    }

    private static class ConvertTimestamps<T> extends DoFn<TimestampedValue<T>, T> {
      @Override
      public void processElement(ProcessContext c) {
        c.outputWithTimestamp(c.element().getValue(), c.element().getTimestamp());
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  static {
    registerDefaultTransformEvaluator();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void registerDefaultTransformEvaluator() {
    DirectPipelineRunner.registerDefaultTransformEvaluator(
        Create.Values.class,
        new DirectPipelineRunner.TransformEvaluator<Create.Values>() {
          @Override
          public void evaluate(
              Create.Values transform,
              DirectPipelineRunner.EvaluationContext context) {
            evaluateHelper(transform, context);
          }
        });
  }

  private static <T> void evaluateHelper(
      Create.Values<T> transform,
      DirectPipelineRunner.EvaluationContext context) {
    // Convert the Iterable of elems into a List of elems.
    List<T> listElems;
    if (transform.elems instanceof Collection) {
      Collection<T> collectionElems = (Collection<T>) transform.elems;
      listElems = new ArrayList<>(collectionElems.size());
    } else {
      listElems = new ArrayList<>();
    }
    for (T elem : transform.elems) {
      listElems.add(
          context.ensureElementEncodable(context.getOutput(transform), elem));
    }
    context.setPCollection(context.getOutput(transform), listElems);
  }
}
