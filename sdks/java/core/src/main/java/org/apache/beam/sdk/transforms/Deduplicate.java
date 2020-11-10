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

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * A set of {@link PTransform}s which deduplicate input records over a time domain and threshold.
 * Values in different windows will not be considered duplicates of each other. Deduplication is
 * best effort.
 *
 * <p>Two values of type {@code T} are compared for equality <b>not</b> by regular Java {@link
 * Object#equals}, but instead by first encoding each of the elements using the {@code
 * PCollection}'s {@code Coder}, and then comparing the encoded bytes. This admits efficient
 * parallel evaluation.
 *
 * <p>These PTransforms are different then {@link Distinct} since {@link Distinct} guarantees
 * uniqueness of values within a {@link PCollection} but may support a narrower set of {@link
 * org.apache.beam.sdk.values.WindowingStrategy windowing strategies} or may delay when output is
 * produced.
 *
 * <p>The durations specified may impose memory and/or storage requirements within a runner and care
 * might need to be used to ensure that the deduplication time limit is long enough to remove
 * duplicates but short enough to not cause performance problems within a runner. Each runner may
 * provide an optimized implementation of their choice using the deduplication time domain and
 * threshold specified.
 *
 * <p>Does not preserve any order the input PCollection might have had.
 *
 * <p>Example of use:
 *
 * <pre>{@code
 * PCollection<String> words = ...;
 * PCollection<String> deduplicatedWords =
 *     words.apply(Deduplicate.<String>values());
 * }</pre>
 */
@Experimental(Kind.SPLITTABLE_DO_FN)
public final class Deduplicate {
  /** The default is the {@link TimeDomain#PROCESSING_TIME processing time domain}. */
  public static final TimeDomain DEFAULT_TIME_DOMAIN = TimeDomain.PROCESSING_TIME;
  /** The default duration is 10 mins. */
  public static final Duration DEFAULT_DURATION = Duration.standardMinutes(10);

  /**
   * Deduplicates values over a specified time domain and threshold. Construct via {@link
   * Deduplicate#values()}.
   */
  public static final class Values<T> extends PTransform<PCollection<T>, PCollection<T>> {
    private final TimeDomain timeDomain;
    private final Duration duration;

    private Values(TimeDomain timeDomain, Duration duration) {
      this.timeDomain = timeDomain;
      this.duration = duration;
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      return input
          .apply(
              "KeyByElement",
              MapElements.via(
                  new SimpleFunction<T, KV<T, Void>>() {
                    @Override
                    public KV<T, Void> apply(T element) {
                      return KV.of(element, (Void) null);
                    }
                  }))
          .apply(new KeyedValues<>(timeDomain, duration))
          .apply(Keys.create());
    }

    /**
     * Returns a {@code Values} {@link PTransform} like this one but with the specified time domain.
     */
    public Values<T> withTimeDomain(TimeDomain timeDomain) {
      return new Values<T>(timeDomain, duration);
    }

    /**
     * Returns a {@code Values} {@link PTransform} like this one but with the specified duration.
     */
    public Values<T> withDuration(Duration duration) {
      return new Values<T>(timeDomain, duration);
    }
  }

  /**
   * A {@link PTransform} that uses a {@link SerializableFunction} to obtain a representative value
   * for each input element used for deduplication.
   *
   * <p>Construct via {@link Deduplicate#withRepresentativeValueFn}.
   *
   * @param <T> the type of input and output element
   * @param <IdT> the type of representative values used to dedup
   */
  public static final class WithRepresentativeValues<T, IdT>
      extends PTransform<PCollection<T>, PCollection<T>> {
    private final SerializableFunction<T, IdT> fn;
    private final @Nullable TypeDescriptor<IdT> type;
    private final @Nullable Coder<IdT> coder;
    private final TimeDomain timeDomain;
    private final Duration duration;

    private WithRepresentativeValues(
        TimeDomain timeDomain,
        Duration duration,
        SerializableFunction<T, IdT> fn,
        @Nullable TypeDescriptor<IdT> type,
        @Nullable Coder<IdT> coder) {
      this.timeDomain = timeDomain;
      this.duration = duration;
      this.fn = fn;
      this.type = type;
      this.coder = coder;
    }

    /**
     * Return a {@code WithRepresentativeValues} {@link PTransform} that is like this one, but with
     * the specified id type descriptor.
     *
     * <p>Either {@link #withRepresentativeCoder} or this method must be invoked if using {@link
     * Deduplicate#withRepresentativeValueFn} in Java 8 with a lambda as the fn.
     *
     * @param type a {@link TypeDescriptor} describing the representative type of this {@code
     *     WithRepresentativeValues}
     * @return A {@code WithRepresentativeValues} {@link PTransform} that is like this one, but with
     *     the specified representative value type descriptor. Any previously set representative
     *     value coder will be cleared.
     */
    public WithRepresentativeValues<T, IdT> withRepresentativeType(TypeDescriptor<IdT> type) {
      return new WithRepresentativeValues<>(timeDomain, duration, fn, type, null);
    }

    /**
     * Return a {@code WithRepresentativeValues} {@link PTransform} that is like this one, but with
     * the specified id type coder.
     *
     * <p>Required for use of {@link Deduplicate#withRepresentativeValueFn} in Java 8 with a lambda
     * as the fn.
     *
     * @param coder a {@link Coder} capable of encoding the representative type of this {@code
     *     WithRepresentativeValues}
     * @return A {@code WithRepresentativeValues} {@link PTransform} that is like this one, but with
     *     the specified representative value coder. Any previously set representative value type
     *     descriptor will be cleared.
     */
    public WithRepresentativeValues<T, IdT> withRepresentativeCoder(Coder<IdT> coder) {
      return new WithRepresentativeValues<>(timeDomain, duration, fn, null, coder);
    }

    /**
     * Returns a {@code WithRepresentativeValues} {@link PTransform} like this one but with the
     * specified time domain.
     */
    public WithRepresentativeValues<T, IdT> withTimeDomain(TimeDomain timeDomain) {
      return new WithRepresentativeValues<>(timeDomain, duration, fn, type, coder);
    }

    /**
     * Return a {@code WithRepresentativeValues} {@link PTransform} that is like this one, but with
     * the specified deduplication duration.
     */
    public WithRepresentativeValues<T, IdT> withDuration(Duration duration) {
      return new WithRepresentativeValues<>(timeDomain, duration, fn, type, coder);
    }

    @Override
    public PCollection<T> expand(PCollection<T> input) {
      WithKeys<IdT, T> withKeys = WithKeys.of(fn);
      if (type != null) {
        withKeys = withKeys.withKeyType(type);
      }
      PCollection<KV<IdT, T>> inputWithKey = input.apply(withKeys);
      if (coder != null) {
        inputWithKey.setCoder(KvCoder.of(coder, input.getCoder()));
      }
      return inputWithKey
          .apply(new KeyedValues<>(timeDomain, duration))
          .apply(org.apache.beam.sdk.transforms.Values.create());
    }
  }

  /**
   * Deduplicates keyed values using the key over a specified time domain and threshold. Construct
   * via {@link Deduplicate#keyedValues()} ()}.
   */
  public static final class KeyedValues<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, V>>> {
    private final TimeDomain timeDomain;
    private final Duration duration;

    private KeyedValues(TimeDomain timeDomain, Duration duration) {
      this.timeDomain = timeDomain;
      this.duration = duration;
    }

    @Override
    public PCollection<KV<K, V>> expand(PCollection<KV<K, V>> input) {
      return input.apply(ParDo.of(new DeduplicateFn<>(timeDomain, duration)));
    }

    /**
     * Returns a {@code KeyedValues} {@link PTransform} like this one but with the specified time
     * domain.
     */
    public KeyedValues<K, V> withTimeDomain(TimeDomain timeDomain) {
      return new KeyedValues<>(timeDomain, duration);
    }

    /**
     * Returns a {@code KeyedValues} {@link PTransform} like this one but with the specified
     * duration.
     */
    public KeyedValues<K, V> withDuration(Duration duration) {
      return new KeyedValues<>(timeDomain, duration);
    }
  }

  /**
   * Returns a deduplication transform that deduplicates values for up to 10 mins within the {@link
   * TimeDomain#PROCESSING_TIME processing time domain}.
   */
  public static <T> Deduplicate.Values<T> values() {
    return new Deduplicate.Values<>(DEFAULT_TIME_DOMAIN, DEFAULT_DURATION);
  }

  /**
   * Returns a deduplication transform that deduplicates keyed values using the key for up to 10
   * mins within the {@link TimeDomain#PROCESSING_TIME processing time domain}.
   */
  public static <K, V> Deduplicate.KeyedValues<K, V> keyedValues() {
    return new Deduplicate.KeyedValues<>(DEFAULT_TIME_DOMAIN, DEFAULT_DURATION);
  }

  /**
   * Returns a deduplication transform that deduplicates values using the supplied representative
   * value for up to 10 mins within the {@link TimeDomain#PROCESSING_TIME processing time domain}.
   */
  public static <T, IdT> Deduplicate.WithRepresentativeValues<T, IdT> withRepresentativeValueFn(
      SerializableFunction<T, IdT> representativeValueFn) {
    return new Deduplicate.WithRepresentativeValues<T, IdT>(
        DEFAULT_TIME_DOMAIN, DEFAULT_DURATION, representativeValueFn, null, null);
  }

  /////////////////////////////////////////////////////////////////////////////

  // prevent instantiation
  private Deduplicate() {}

  /**
   * A stateful {@link DoFn} that uses a {@link ValueState} to capture whether the value has ever
   * been seen.
   *
   * @param <K>
   * @param <V>
   */
  private static class DeduplicateFn<K, V> extends DoFn<KV<K, V>, KV<K, V>> {
    private static final String EXPIRY_TIMER = "expiryTimer";
    private static final String SEEN_STATE = "seen";

    @TimerId(EXPIRY_TIMER)
    private final TimerSpec expiryTimerSpec;

    @StateId(SEEN_STATE)
    private final StateSpec<ValueState<Boolean>> seenState = StateSpecs.value(BooleanCoder.of());

    private final Duration duration;

    private DeduplicateFn(TimeDomain timeDomain, Duration duration) {
      this.expiryTimerSpec = TimerSpecs.timer(timeDomain);
      this.duration = duration;
    }

    @ProcessElement
    public void processElement(
        @Element KV<K, V> element,
        OutputReceiver<KV<K, V>> receiver,
        @StateId(SEEN_STATE) ValueState<Boolean> seenState,
        @TimerId(EXPIRY_TIMER) Timer expiryTimer) {
      Boolean seen = seenState.read();
      // Seen state is either set or not set so if it has been set then it must be true.
      if (seen == null) {
        expiryTimer.offset(duration).setRelative();
        seenState.write(true);
        receiver.output(element);
      }
    }

    @OnTimer(EXPIRY_TIMER)
    public void onExpiry(
        OnTimerContext context, @StateId(SEEN_STATE) ValueState<Boolean> seenState) {
      seenState.clear();
    }
  }
}
