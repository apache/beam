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

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * {@code Distinct<T>} takes a {@code PCollection<T>} and returns a {@code PCollection<T>} that has
 * all distinct elements of the input. Thus, each element is unique within each window.
 *
 * <p>Two values of type {@code T} are compared for equality <b>not</b> by regular Java {@link
 * Object#equals}, but instead by first encoding each of the elements using the {@code
 * PCollection}'s {@code Coder}, and then comparing the encoded bytes. This admits efficient
 * parallel evaluation.
 *
 * <p>Optionally, a function may be provided that maps each element to a representative value. In
 * this case, two elements will be considered duplicates if they have equal representative values,
 * with equality being determined as above.
 *
 * <p>By default, the {@code Coder} of the output {@code PCollection} is the same as the {@code
 * Coder} of the input {@code PCollection}.
 *
 * <p>Each output element is in the same window as its corresponding input element, and has the
 * timestamp of the end of that window. The output {@code PCollection} has the same {@link
 * org.apache.beam.sdk.transforms.windowing.WindowFn} as the input.
 *
 * <p>Does not preserve any order the input PCollection might have had.
 *
 * <p>Example of use:
 *
 * <pre>{@code
 * PCollection<String> words = ...;
 * PCollection<String> uniqueWords =
 *     words.apply(Distinct.<String>create());
 * }</pre>
 *
 * @param <T> the type of the elements of the input and output {@code PCollection}s
 */
public class Distinct<T> extends PTransform<PCollection<T>, PCollection<T>> {

  /**
   * Returns a {@code Distinct<T>} {@code PTransform}.
   *
   * @param <T> the type of the elements of the input and output {@code PCollection}s
   */
  public static <T> Distinct<T> create() {
    return new Distinct<>();
  }

  /**
   * Returns a {@code Distinct<T, IdT>} {@code PTransform}.
   *
   * @param <T> the type of the elements of the input and output {@code PCollection}s
   * @param <IdT> the type of the representative value used to dedup
   */
  public static <T, IdT> WithRepresentativeValues<T, IdT> withRepresentativeValueFn(
      SerializableFunction<T, IdT> fn) {
    return new WithRepresentativeValues<>(fn, null);
  }

  private static <T, W extends BoundedWindow> void validateWindowStrategy(
      WindowingStrategy<T, W> strategy) {
    if (!strategy.getWindowFn().isNonMerging()
        && (!strategy.getTrigger().getClass().equals(DefaultTrigger.class)
            || strategy.getAllowedLateness().isLongerThan(Duration.ZERO))) {
      throw new UnsupportedOperationException(
          String.format(
              "%s does not support merging windowing strategies, except when using the default "
                  + "trigger and zero allowed lateness.",
              Distinct.class.getSimpleName()));
    }
  }

  @Override
  public PCollection<T> expand(PCollection<T> in) {
    validateWindowStrategy(in.getWindowingStrategy());
    PCollection<KV<T, Void>> combined =
        in.apply(
                "KeyByElement",
                MapElements.via(
                    new SimpleFunction<T, KV<T, Void>>() {
                      @Override
                      public KV<T, Void> apply(T element) {
                        return KV.of(element, (Void) null);
                      }
                    }))
            .apply(
                "DropValues",
                Combine.perKey(
                    new SerializableFunction<Iterable<Void>, Void>() {
                      @Override
                      public @Nullable Void apply(Iterable<Void> iter) {
                        return null; // ignore input
                      }
                    }));
    return combined.apply(
        "ExtractFirstKey",
        ParDo.of(
            new DoFn<KV<T, Void>, T>() {
              @ProcessElement
              public void processElement(
                  @Element KV<T, Void> element, PaneInfo pane, OutputReceiver<T> receiver) {
                if (pane.isFirst()) {
                  // Only output the key if it's the first time it's been seen.
                  receiver.output(element.getKey());
                }
              }
            }));
  }

  /**
   * A {@link Distinct} {@link PTransform} that uses a {@link SerializableFunction} to obtain a
   * representative value for each input element.
   *
   * <p>Construct via {@link Distinct#withRepresentativeValueFn(SerializableFunction)}.
   *
   * @param <T> the type of input and output element
   * @param <IdT> the type of representative values used to dedup
   */
  public static class WithRepresentativeValues<T, IdT>
      extends PTransform<PCollection<T>, PCollection<T>> {
    private final SerializableFunction<T, IdT> fn;
    private final TypeDescriptor<IdT> representativeType;

    private WithRepresentativeValues(
        SerializableFunction<T, IdT> fn, TypeDescriptor<IdT> representativeType) {
      this.fn = fn;
      this.representativeType = representativeType;
    }

    @Override
    public PCollection<T> expand(PCollection<T> in) {
      validateWindowStrategy(in.getWindowingStrategy());
      WithKeys<IdT, T> withKeys = WithKeys.of(fn);
      if (representativeType != null) {
        withKeys = withKeys.withKeyType(representativeType);
      }

      PCollection<KV<IdT, T>> keyed = in.apply("KeyByRepresentativeValue", withKeys);
      KvCoder<IdT, T> keyedCoder = (KvCoder<IdT, T>) keyed.getCoder();
      PCollection<KV<IdT, T>> combined =
          keyed
              .apply(
                  "OneValuePerKey",
                  Combine.perKey(
                      new Combine.BinaryCombineFn<T>() {
                        @Override
                        public T apply(T left, T right) {
                          return left;
                        }
                      }))
              // When there is no input, the combine outputs null. This can occur when the input
              // is in discarding mode with speculative triggers consuming all input prior to
              // on-time or GC firing. There is no reason that the input coder would necessarily
              // support nulls.
              .setCoder(
                  KvCoder.of(
                      keyedCoder.getKeyCoder(), NullableCoder.of(keyedCoder.getValueCoder())));

      return combined.apply(
          "KeepFirstPane",
          ParDo.of(
              new DoFn<KV<IdT, T>, T>() {
                @ProcessElement
                public void processElement(
                    @Element KV<IdT, T> element, PaneInfo pane, OutputReceiver<T> receiver) {
                  // Only output the value if it's the first time it's been seen.
                  if (pane.isFirst()) {
                    receiver.output(element.getValue());
                  }
                }
              }));
    }

    /**
     * Return a {@code WithRepresentativeValues} {@link PTransform} that is like this one, but with
     * the specified output type descriptor.
     *
     * <p>Required for use of {@link Distinct#withRepresentativeValueFn(SerializableFunction)} in
     * Java 8 with a lambda as the fn.
     *
     * @param type a {@link TypeDescriptor} describing the representative type of this {@code
     *     WithRepresentativeValues}
     * @return A {@code WithRepresentativeValues} {@link PTransform} that is like this one, but with
     *     the specified output type descriptor.
     */
    public WithRepresentativeValues<T, IdT> withRepresentativeType(TypeDescriptor<IdT> type) {
      return new WithRepresentativeValues<>(fn, type);
    }
  }
}
