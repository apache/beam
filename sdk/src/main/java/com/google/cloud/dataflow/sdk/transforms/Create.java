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

import com.google.api.client.util.Preconditions;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.TimestampedValue;
import com.google.cloud.dataflow.sdk.values.TimestampedValue.TimestampedValueCoder;
import com.google.common.reflect.TypeToken;

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
 * <p> Example of use:
 * <pre> {@code
 * Pipeline p = ...;
 *
 * PCollection<Integer> pc = p.apply(Create.of(3, 4, 5)).setCoder(BigEndianIntegerCoder.of());
 *
 * Map<String, Integer> map = ...;
 * PCollection<KV<String, Integer>> pt =
 *     p.apply(Create.of(map))
 *      .setCoder(KvCoder.of(StringUtf8Coder.of(),
 *                           BigEndianIntegerCoder.of()));
 * } </pre>
 *
 * <p> Note that {@link PCollection#setCoder} must be called
 * explicitly to set the encoding of the resulting
 * {@code PCollection}, since {@code Create} does not infer the
 * encoding.
 *
 * <p> A good use for {@code Create} is when a {@code PCollection}
 * needs to be created without dependencies on files or other external
 * entities.  This is especially useful during testing.
 *
 * <p> Caveat: {@code Create} only supports small in-memory datasets,
 * particularly when submitting jobs to the Google Cloud Dataflow
 * service.
 *
 * <p> {@code Create} can automatically determine the {@code Coder} to use
 * if all elements are the same type, and a default exists for that type.
 * See {@link com.google.cloud.dataflow.sdk.coders.CoderRegistry} for details
 * on how defaults are determined.
 *
 * @param <T> the type of the elements of the resulting {@code PCollection}
 */
@SuppressWarnings("serial")
public class Create<T> extends PTransform<PInput, PCollection<T>> {

  /**
   * Returns a new {@code Create} root transform that produces a
   * {@link PCollection} containing the specified elements.
   *
   * <p> The argument should not be modified after this is called.
   *
   * <p> The elements will have a timestamp of negative infinity, see
   * {@link Create#timestamped} for a way of creating a {@code PCollection}
   * with timestamped elements.
   *
   * <p> The result of applying this transform should have its
   * {@link Coder} specified explicitly, via a call to
   * {@link PCollection#setCoder}.
   */
  public static <T> Create<T> of(Iterable<T> elems) {
    return new Create<>(elems);
  }

  /**
   * Returns a new {@code Create} root transform that produces a
   * {@link PCollection} containing the specified elements.
   *
   * <p> The elements will have a timestamp of negative infinity, see
   * {@link Create#timestamped} for a way of creating a {@code PCollection}
   * with timestamped elements.
   *
   * <p> The argument should not be modified after this is called.
   *
   * <p> The result of applying this transform should have its
   * {@link Coder} specified explicitly, via a call to
   * {@link PCollection#setCoder}.
   */
  @SafeVarargs
  public static <T> Create<T> of(T... elems) {
    return of(Arrays.asList(elems));
  }

  /**
   * Returns a new {@code Create} root transform that produces a
   * {@link PCollection} of {@link KV}s corresponding to the keys and
   * values of the specified {@code Map}.
   *
   * <p> The elements will have a timestamp of negative infinity, see
   * {@link Create#timestamped} for a way of creating a {@code PCollection}
   * with timestamped elements.
   *
   * <p> The result of applying this transform should have its
   * {@link Coder} specified explicitly, via a call to
   * {@link PCollection#setCoder}.
   */
  public static <K, V> Create<KV<K, V>> of(Map<K, V> elems) {
    List<KV<K, V>> kvs = new ArrayList<>(elems.size());
    for (Map.Entry<K, V> entry : elems.entrySet()) {
      kvs.add(KV.of(entry.getKey(), entry.getValue()));
    }
    return of(kvs);
  }

  /**
   * Returns a new root transform that produces a {@link PCollection} containing
   * the specified elements with the specified timestamps.
   *
   * <p> The argument should not be modified after this is called.
   */
  public static <T> CreateTimestamped<T> timestamped(Iterable<TimestampedValue<T>> elems) {
    return new CreateTimestamped<>(elems);
  }

  /**
   * Returns a new root transform that produces a {@link PCollection} containing
   * the specified elements with the specified timestamps.
   *
   * <p> The argument should not be modified after this is called.
   */
  @SuppressWarnings("unchecked")
  public static <T> CreateTimestamped<T> timestamped(TimestampedValue<T>... elems) {
    return new CreateTimestamped<>(Arrays.asList(elems));
  }

  /**
   * Returns a new root transform that produces a {@link PCollection} containing
   * the specified elements with the specified timestamps.
   *
   * <p> The arguments should not be modified after this is called.
   *
   * @throws IllegalArgumentException if there are a different number of values
   * and timestamps
   */
  public static <T> CreateTimestamped<T> timestamped(
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
    return new CreateTimestamped<>(elems);
  }

  @Override
  public PCollection<T> apply(PInput input) {
    return PCollection.<T>createPrimitiveOutputInternal(new GlobalWindows());
  }


  /////////////////////////////////////////////////////////////////////////////

  /** The elements of the resulting PCollection. */
  private final Iterable<T> elems;

  /**
   * Constructs a {@code Create} transform that produces a
   * {@link PCollection} containing the specified elements.
   *
   * <p> The argument should not be modified after this is called.
   */
  private Create(Iterable<T> elems) {
    this.elems = elems;
  }

  public Iterable<T> getElements() {
    return elems;
  }

  @Override
  protected Coder<?> getDefaultOutputCoder() {
    // First try to deduce a coder using the types of the elements.
    Class<?> elementType = null;
    for (T elem : elems) {
      Class<?> type = elem.getClass();
      if (elementType == null) {
        elementType = type;
      } else if (!elementType.equals(type)) {
        // Elements are not the same type, require a user-specified coder.
        elementType = null;
        break;
      }
    }
    if (elementType == null) {
      return super.getDefaultOutputCoder();
    }
    if (elementType.getTypeParameters().length == 0) {
      Coder<?> candidate = getCoderRegistry().getDefaultCoder(TypeToken.of(elementType));
      if (candidate != null) {
        return candidate;
      }
    }

    // If that fails, try to deduce a coder using the elements themselves
    Coder<?> coder = null;
    for (T elem : elems) {
      Coder<?> c = getCoderRegistry().getDefaultCoder(elem);
      if (coder == null) {
        coder = c;
      } else if (!Objects.equals(c, coder)) {
        coder = null;
        break;
      }
    }
    if (coder != null) {
      return coder;
    }

    return super.getDefaultOutputCoder();
  }

  /**
   * A {@code PTransform} that creates a {@code PCollection} whose elements have
   * associated timestamps.
   */
  private static class CreateTimestamped<T> extends PTransform<PBegin, PCollection<T>> {
    /** The timestamped elements of the resulting PCollection. */
    private final Iterable<TimestampedValue<T>> elems;

    private CreateTimestamped(Iterable<TimestampedValue<T>> elems) {
      this.elems = elems;
    }

    @Override
    public PCollection<T> apply(PBegin input) {
      PCollection<TimestampedValue<T>> intermediate = input.apply(Create.of(elems));
      if (!elems.iterator().hasNext()) {
        // There aren't any elements, so we can provide a fake coder instance.
        // If we don't set a Coder here, users of CreateTimestamped have
        // no way to set the coder of the intermediate PCollection.
        @SuppressWarnings("unchecked")
        TimestampedValueCoder<T> fakeCoder =
            (TimestampedValueCoder<T>) TimestampedValue.TimestampedValueCoder.of(VoidCoder.of());
        intermediate.setCoder(fakeCoder);
      }

      return intermediate.apply(ParDo.of(new ConvertTimestamps<T>()));
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
        Create.class,
        new DirectPipelineRunner.TransformEvaluator<Create>() {
          @Override
          public void evaluate(
              Create transform,
              DirectPipelineRunner.EvaluationContext context) {
            evaluateHelper(transform, context);
          }
        });
  }

  private static <T> void evaluateHelper(
      Create<T> transform,
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
          context.ensureElementEncodable(transform.getOutput(), elem));
    }
    context.setPCollection(transform.getOutput(), listElems);
  }
}
