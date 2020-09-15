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

import static org.apache.beam.sdk.transforms.Contextful.Fn.Context.wrapProcessContext;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.resume;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.stop;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DurationCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SnappyCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.Contextful.Fn;
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.Watch.Growth.PollResult;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.VarInt;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.TypeDescriptors.TypeVariableExtractor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Ordering;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Funnel;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Funnels;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.HashCode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.Hashing;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.ReadableDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given a "poll function" that produces a potentially growing set of outputs for an input, this
 * transform simultaneously continuously watches the growth of output sets of all inputs, until a
 * per-input termination condition is reached.
 *
 * <p>The output is returned as an unbounded {@link PCollection} of {@code KV<InputT, OutputT>},
 * where each {@code OutputT} is associated with the {@code InputT} that produced it, and is
 * assigned with the timestamp that the poll function returned when this output was detected for the
 * first time.
 *
 * <p>Hypothetical usage example for watching new files in a collection of directories, where for
 * each directory we assume that new files will not appear if the directory contains a file named
 * ".complete":
 *
 * <pre>{@code
 * PCollection<String> directories = ...;  // E.g. Create.of(single directory)
 * PCollection<KV<String, String>> matches = filepatterns.apply(Watch.<String, String>growthOf(
 *   new PollFn<String, String>() {
 *     public PollResult<String> apply(TimestampedValue<String> input) {
 *       String directory = input.getValue();
 *       List<TimestampedValue<String>> outputs = new ArrayList<>();
 *       ... List the directory and get creation times of all files ...
 *       boolean isComplete = ... does a file ".complete" exist in the directory ...
 *       return isComplete ? PollResult.complete(outputs) : PollResult.incomplete(outputs);
 *     }
 *   })
 *   // Poll each directory every 5 seconds
 *   .withPollInterval(Duration.standardSeconds(5))
 *   // Stop watching each directory 12 hours after it's seen even if it's incomplete
 *   .withTerminationPerInput(afterTotalOf(Duration.standardHours(12)));
 * }</pre>
 *
 * <p>By default, the watermark for a particular input is computed from a poll result as "earliest
 * timestamp of new elements in this poll result". It can also be set explicitly via {@link
 * Growth.PollResult#withWatermark} if the {@link Growth.PollFn} can provide a more optimistic
 * estimate.
 *
 * <p>Note: This transform works only in runners supporting Splittable DoFn: see <a
 * href="https://beam.apache.org/documentation/runners/capability-matrix/">capability matrix</a>.
 */
@Experimental(Kind.SPLITTABLE_DO_FN)
public class Watch {
  private static final Logger LOG = LoggerFactory.getLogger(Watch.class);

  /** Watches the growth of the given poll function. See class documentation for more details. */
  public static <InputT, OutputT> Growth<InputT, OutputT, OutputT> growthOf(
      Growth.PollFn<InputT, OutputT> pollFn, Requirements requirements) {
    return new AutoValue_Watch_Growth.Builder<InputT, OutputT, OutputT>()
        .setTerminationPerInput(Growth.never())
        .setPollFn(Contextful.of(pollFn, requirements))
        // use null as a signal that this is the identity function and output coder can be
        // reused as key coder
        .setOutputKeyFn(null)
        .build();
  }

  /** Watches the growth of the given poll function. See class documentation for more details. */
  public static <InputT, OutputT> Growth<InputT, OutputT, OutputT> growthOf(
      Growth.PollFn<InputT, OutputT> pollFn) {
    return growthOf(pollFn, Requirements.empty());
  }

  /**
   * Watches the growth of the given poll function, using the given "key function" to deduplicate
   * outputs. For example, if OutputT is a filename + file size, this can be a function that returns
   * just the filename, so that if the same file is observed multiple times with different sizes,
   * only the first observation is emitted.
   *
   * <p>By default, this is the identity function, i.e. the output is used as its own key.
   */
  public static <InputT, OutputT, KeyT> Growth<InputT, OutputT, KeyT> growthOf(
      Contextful<Growth.PollFn<InputT, OutputT>> pollFn,
      SerializableFunction<OutputT, KeyT> outputKeyFn) {
    checkArgument(pollFn != null, "pollFn can not be null");
    checkArgument(outputKeyFn != null, "outputKeyFn can not be null");
    return new AutoValue_Watch_Growth.Builder<InputT, OutputT, KeyT>()
        .setTerminationPerInput(Watch.Growth.never())
        .setPollFn(pollFn)
        .setOutputKeyFn(outputKeyFn)
        .build();
  }

  /** Implementation of {@link #growthOf}. */
  @AutoValue
  public abstract static class Growth<InputT, OutputT, KeyT>
      extends PTransform<PCollection<InputT>, PCollection<KV<InputT, OutputT>>> {
    /** The result of a single invocation of a {@link PollFn}. */
    public static final class PollResult<OutputT> {
      private final List<TimestampedValue<OutputT>> outputs;
      // null means unspecified (infer automatically).
      private final @Nullable Instant watermark;

      private PollResult(List<TimestampedValue<OutputT>> outputs, @Nullable Instant watermark) {
        this.outputs = outputs;
        this.watermark = watermark;
      }

      List<TimestampedValue<OutputT>> getOutputs() {
        return outputs;
      }

      @Nullable
      Instant getWatermark() {
        return watermark;
      }

      /**
       * Returns a new {@link PollResult} like this one with the provided watermark. The watermark
       * represents an approximate lower bound on timestamps of future new outputs from the {@link
       * PollFn}.
       */
      public PollResult<OutputT> withWatermark(Instant watermark) {
        checkNotNull(watermark, "watermark");
        return new PollResult<>(outputs, watermark);
      }

      /** Returns a new {@link PollResult} like this one with the provided outputs. */
      public PollResult<OutputT> withOutputs(List<TimestampedValue<OutputT>> outputs) {
        checkNotNull(outputs);
        return new PollResult<>(outputs, watermark);
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("watermark", watermark)
            .add("outputs", outputs)
            .toString();
      }

      /**
       * Constructs a {@link PollResult} with the given outputs and declares that there will be no
       * new outputs for the current input. The {@link PollFn} will not be called again for this
       * input.
       */
      public static <OutputT> PollResult<OutputT> complete(
          List<TimestampedValue<OutputT>> outputs) {
        return new PollResult<>(outputs, BoundedWindow.TIMESTAMP_MAX_VALUE);
      }

      /** Like {@link #complete(List)}, but assigns the same timestamp to all new outputs. */
      public static <OutputT> PollResult<OutputT> complete(
          Instant timestamp, List<OutputT> outputs) {
        return new PollResult<>(
            addTimestamp(timestamp, outputs), BoundedWindow.TIMESTAMP_MAX_VALUE);
      }

      /**
       * Constructs a {@link PollResult} with the given outputs and declares that new outputs might
       * appear for the current input. By default, {@link Watch} will estimate the watermark for
       * future new outputs as equal to the earliest of the new outputs from this {@link
       * PollResult}. To specify a more exact watermark, use {@link #withWatermark(Instant)}.
       */
      public static <OutputT> PollResult<OutputT> incomplete(
          List<TimestampedValue<OutputT>> outputs) {
        return new PollResult<>(outputs, null);
      }

      /** Like {@link #incomplete(List)}, but assigns the same timestamp to all new outputs. */
      public static <OutputT> PollResult<OutputT> incomplete(
          Instant timestamp, List<OutputT> outputs) {
        return new PollResult<>(addTimestamp(timestamp, outputs), null);
      }

      private static <OutputT> List<TimestampedValue<OutputT>> addTimestamp(
          Instant timestamp, List<OutputT> outputs) {
        List<TimestampedValue<OutputT>> res = Lists.newArrayListWithExpectedSize(outputs.size());
        for (OutputT output : outputs) {
          res.add(TimestampedValue.of(output, timestamp));
        }
        return res;
      }

      @Override
      public boolean equals(@Nullable Object o) {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }
        PollResult<?> that = (PollResult<?>) o;
        return Objects.equals(outputs, that.outputs) && Objects.equals(watermark, that.watermark);
      }

      @Override
      public int hashCode() {
        return Objects.hash(outputs, watermark);
      }
    }

    /**
     * A function that computes the current set of outputs for the given input, in the form of a
     * {@link PollResult}.
     */
    public abstract static class PollFn<InputT, OutputT>
        implements Fn<InputT, PollResult<OutputT>> {}

    /**
     * A strategy for determining whether it is time to stop polling the current input regardless of
     * whether its output is complete or not.
     *
     * <p>Some built-in termination conditions are {@link #never}, {@link #afterTotalOf} and {@link
     * #afterTimeSinceNewOutput}. Conditions can be combined using {@link #eitherOf} and {@link
     * #allOf}. Users can also develop custom termination conditions, for example, one might imagine
     * a condition that terminates after a given time after the first output appears for the input
     * (unlike {@link #afterTotalOf} which operates relative to when the input itself arrives).
     *
     * <p>A {@link TerminationCondition} is provided to {@link
     * Growth#withTerminationPerInput(TerminationCondition)} and is used to maintain an independent
     * state of the termination condition for every input, represented as {@code StateT} which must
     * be immutable, non-null, and encodable via {@link #getStateCoder()}.
     *
     * <p>All functions take the wall-clock timestamp as {@link Instant} for convenience of
     * unit-testing custom termination conditions.
     */
    public interface TerminationCondition<InputT, StateT> extends Serializable {
      /** Used to encode the state of this {@link TerminationCondition}. */
      Coder<StateT> getStateCoder();

      /**
       * Called by the {@link Watch} transform to create a new independent termination state for a
       * newly arrived {@code InputT}.
       */
      StateT forNewInput(Instant now, @Nullable InputT input);

      /**
       * Called by the {@link Watch} transform to compute a new termination state, in case after
       * calling the {@link PollFn} for the current input, the {@link PollResult} included a
       * previously unseen {@code OutputT}.
       */
      StateT onSeenNewOutput(Instant now, StateT state);

      /**
       * Called by the {@link Watch} transform to determine whether the given termination state
       * signals that {@link Watch} should stop calling {@link PollFn} for the current input,
       * regardless of whether the last {@link PollResult} was complete or incomplete.
       */
      boolean canStopPolling(Instant now, StateT state);

      /** Creates a human-readable representation of the given state of this condition. */
      String toString(StateT state);
    }

    /**
     * Returns a {@link TerminationCondition} that never holds (i.e., poll each input until its
     * output is complete).
     */
    public static <InputT> Never<InputT> never() {
      return new Never<>();
    }

    /**
     * Wraps a given input-independent {@link TerminationCondition} as an equivalent condition with
     * a given input type, passing {@code null} to the original condition as input.
     */
    public static <InputT, StateT> TerminationCondition<InputT, StateT> ignoreInput(
        TerminationCondition<?, StateT> condition) {
      return new IgnoreInput<>(condition);
    }

    /**
     * Returns a {@link TerminationCondition} that holds after the given time has elapsed after the
     * current input was seen.
     */
    public static <InputT> AfterTotalOf<InputT> afterTotalOf(ReadableDuration timeSinceInput) {
      return afterTotalOf(SerializableFunctions.<InputT, ReadableDuration>constant(timeSinceInput));
    }

    /** Like {@link #afterTotalOf(ReadableDuration)}, but the duration is input-dependent. */
    public static <InputT> AfterTotalOf<InputT> afterTotalOf(
        SerializableFunction<InputT, ReadableDuration> timeSinceInput) {
      return new AfterTotalOf<>(timeSinceInput);
    }

    /**
     * Returns a {@link TerminationCondition} that holds after the given time has elapsed after the
     * last time the {@link PollResult} for the current input contained a previously unseen output.
     */
    public static <InputT> AfterTimeSinceNewOutput<InputT> afterTimeSinceNewOutput(
        ReadableDuration timeSinceNewOutput) {
      return afterTimeSinceNewOutput(
          SerializableFunctions.<InputT, ReadableDuration>constant(timeSinceNewOutput));
    }

    /**
     * Like {@link #afterTimeSinceNewOutput(ReadableDuration)}, but the duration is input-dependent.
     */
    public static <InputT> AfterTimeSinceNewOutput<InputT> afterTimeSinceNewOutput(
        SerializableFunction<InputT, ReadableDuration> timeSinceNewOutput) {
      return new AfterTimeSinceNewOutput<>(timeSinceNewOutput);
    }

    /**
     * Returns a {@link TerminationCondition} that holds when at least one of the given two
     * conditions holds.
     */
    public static <InputT, FirstStateT, SecondStateT>
        BinaryCombined<InputT, FirstStateT, SecondStateT> eitherOf(
            TerminationCondition<InputT, FirstStateT> first,
            TerminationCondition<InputT, SecondStateT> second) {
      return new BinaryCombined<>(BinaryCombined.Operation.OR, first, second);
    }

    /**
     * Returns a {@link TerminationCondition} that holds when both of the given two conditions hold.
     */
    public static <InputT, FirstStateT, SecondStateT>
        BinaryCombined<InputT, FirstStateT, SecondStateT> allOf(
            TerminationCondition<InputT, FirstStateT> first,
            TerminationCondition<InputT, SecondStateT> second) {
      return new BinaryCombined<>(BinaryCombined.Operation.AND, first, second);
    }

    // Uses Integer rather than Void for state, because termination state must be non-null.
    static class Never<InputT> implements TerminationCondition<InputT, Integer> {
      @Override
      public Coder<Integer> getStateCoder() {
        return VarIntCoder.of();
      }

      @Override
      public Integer forNewInput(Instant now, InputT input) {
        return 0;
      }

      @Override
      public Integer onSeenNewOutput(Instant now, Integer state) {
        return state;
      }

      @Override
      public boolean canStopPolling(Instant now, Integer state) {
        return false;
      }

      @Override
      public String toString(Integer state) {
        return "Never";
      }
    }

    static class IgnoreInput<InputT, StateT> implements TerminationCondition<InputT, StateT> {
      private final TerminationCondition<?, StateT> wrapped;

      IgnoreInput(TerminationCondition<?, StateT> wrapped) {
        this.wrapped = wrapped;
      }

      @Override
      public Coder<StateT> getStateCoder() {
        return wrapped.getStateCoder();
      }

      @Override
      public StateT forNewInput(Instant now, InputT input) {
        return wrapped.forNewInput(now, null);
      }

      @Override
      public StateT onSeenNewOutput(Instant now, StateT state) {
        return wrapped.onSeenNewOutput(now, state);
      }

      @Override
      public boolean canStopPolling(Instant now, StateT state) {
        return wrapped.canStopPolling(now, state);
      }

      @Override
      public String toString(StateT state) {
        return wrapped.toString(state);
      }
    }

    static class AfterTotalOf<InputT>
        implements TerminationCondition<
            InputT, KV<Instant /* timeStarted */, ReadableDuration /* maxTimeSinceInput */>> {
      private final SerializableFunction<InputT, ReadableDuration> maxTimeSinceInput;

      private AfterTotalOf(SerializableFunction<InputT, ReadableDuration> maxTimeSinceInput) {
        this.maxTimeSinceInput = maxTimeSinceInput;
      }

      @Override
      public Coder<KV<Instant, ReadableDuration>> getStateCoder() {
        return KvCoder.of(InstantCoder.of(), DurationCoder.of());
      }

      @Override
      public KV<Instant, ReadableDuration> forNewInput(Instant now, InputT input) {
        return KV.of(now, maxTimeSinceInput.apply(input));
      }

      @Override
      public KV<Instant, ReadableDuration> onSeenNewOutput(
          Instant now, KV<Instant, ReadableDuration> state) {
        return state;
      }

      @Override
      public boolean canStopPolling(Instant now, KV<Instant, ReadableDuration> state) {
        return new Duration(state.getKey(), now).isLongerThan(state.getValue());
      }

      @Override
      public String toString(KV<Instant, ReadableDuration> state) {
        return "AfterTotalOf{"
            + "timeStarted="
            + state.getKey()
            + ", maxTimeSinceInput="
            + state.getValue()
            + '}';
      }
    }

    static class AfterTimeSinceNewOutput<InputT>
        implements TerminationCondition<
            InputT,
            KV<Instant /* timeOfLastNewOutput */, ReadableDuration /* maxTimeSinceNewOutput */>> {
      private final SerializableFunction<InputT, ReadableDuration> maxTimeSinceNewOutput;

      private AfterTimeSinceNewOutput(
          SerializableFunction<InputT, ReadableDuration> maxTimeSinceNewOutput) {
        this.maxTimeSinceNewOutput = maxTimeSinceNewOutput;
      }

      @Override
      public Coder<KV<Instant, ReadableDuration>> getStateCoder() {
        return KvCoder.of(NullableCoder.of(InstantCoder.of()), DurationCoder.of());
      }

      @Override
      public KV<Instant, ReadableDuration> forNewInput(Instant now, InputT input) {
        return KV.of(null, maxTimeSinceNewOutput.apply(input));
      }

      @Override
      public KV<Instant, ReadableDuration> onSeenNewOutput(
          Instant now, KV<Instant, ReadableDuration> state) {
        return KV.of(now, state.getValue());
      }

      @Override
      public boolean canStopPolling(Instant now, KV<Instant, ReadableDuration> state) {
        Instant timeOfLastNewOutput = state.getKey();
        ReadableDuration maxTimeSinceNewOutput = state.getValue();
        return timeOfLastNewOutput != null
            && new Duration(timeOfLastNewOutput, now).isLongerThan(maxTimeSinceNewOutput);
      }

      @Override
      public String toString(KV<Instant, ReadableDuration> state) {
        return "AfterTimeSinceNewOutput{"
            + "timeOfLastNewOutput="
            + state.getKey()
            + ", maxTimeSinceNewOutput="
            + state.getValue()
            + '}';
      }
    }

    static class BinaryCombined<InputT, FirstStateT, SecondStateT>
        implements TerminationCondition<InputT, KV<FirstStateT, SecondStateT>> {
      private enum Operation {
        OR,
        AND
      }

      private final Operation operation;
      private final TerminationCondition<InputT, FirstStateT> first;
      private final TerminationCondition<InputT, SecondStateT> second;

      public BinaryCombined(
          Operation operation,
          TerminationCondition<InputT, FirstStateT> first,
          TerminationCondition<InputT, SecondStateT> second) {
        this.operation = operation;
        this.first = first;
        this.second = second;
      }

      @Override
      public Coder<KV<FirstStateT, SecondStateT>> getStateCoder() {
        return KvCoder.of(first.getStateCoder(), second.getStateCoder());
      }

      @Override
      public KV<FirstStateT, SecondStateT> forNewInput(Instant now, InputT input) {
        return KV.of(first.forNewInput(now, input), second.forNewInput(now, input));
      }

      @Override
      public KV<FirstStateT, SecondStateT> onSeenNewOutput(
          Instant now, KV<FirstStateT, SecondStateT> state) {
        return KV.of(
            first.onSeenNewOutput(now, state.getKey()),
            second.onSeenNewOutput(now, state.getValue()));
      }

      @Override
      public boolean canStopPolling(Instant now, KV<FirstStateT, SecondStateT> state) {
        switch (operation) {
          case OR:
            return first.canStopPolling(now, state.getKey())
                || second.canStopPolling(now, state.getValue());
          case AND:
            return first.canStopPolling(now, state.getKey())
                && second.canStopPolling(now, state.getValue());
          default:
            throw new UnsupportedOperationException("Unexpected operation " + operation);
        }
      }

      @Override
      public String toString(KV<FirstStateT, SecondStateT> state) {
        return operation
            + "{first="
            + first.toString(state.getKey())
            + ", second="
            + second.toString(state.getValue())
            + '}';
      }
    }

    abstract Contextful<PollFn<InputT, OutputT>> getPollFn();

    abstract @Nullable SerializableFunction<OutputT, KeyT> getOutputKeyFn();

    abstract @Nullable Coder<KeyT> getOutputKeyCoder();

    abstract @Nullable Duration getPollInterval();

    abstract @Nullable TerminationCondition<InputT, ?> getTerminationPerInput();

    abstract @Nullable Coder<OutputT> getOutputCoder();

    abstract Builder<InputT, OutputT, KeyT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<InputT, OutputT, KeyT> {
      abstract Builder<InputT, OutputT, KeyT> setPollFn(Contextful<PollFn<InputT, OutputT>> pollFn);

      abstract Builder<InputT, OutputT, KeyT> setOutputKeyFn(
          @Nullable SerializableFunction<OutputT, KeyT> outputKeyFn);

      abstract Builder<InputT, OutputT, KeyT> setOutputKeyCoder(Coder<KeyT> outputKeyCoder);

      abstract Builder<InputT, OutputT, KeyT> setTerminationPerInput(
          TerminationCondition<InputT, ?> terminationPerInput);

      abstract Builder<InputT, OutputT, KeyT> setPollInterval(Duration pollInterval);

      abstract Builder<InputT, OutputT, KeyT> setOutputCoder(Coder<OutputT> outputCoder);

      abstract Growth<InputT, OutputT, KeyT> build();
    }

    /** Specifies the coder for the output key. */
    public Growth<InputT, OutputT, KeyT> withOutputKeyCoder(Coder<KeyT> outputKeyCoder) {
      return toBuilder().setOutputKeyCoder(outputKeyCoder).build();
    }

    /** Specifies a {@link TerminationCondition} that will be independently used for every input. */
    public Growth<InputT, OutputT, KeyT> withTerminationPerInput(
        TerminationCondition<InputT, ?> terminationPerInput) {
      return toBuilder().setTerminationPerInput(terminationPerInput).build();
    }

    /**
     * Specifies how long to wait after a call to {@link PollFn} before calling it again (if at all
     * - according to {@link PollResult} and the {@link TerminationCondition}).
     */
    public Growth<InputT, OutputT, KeyT> withPollInterval(Duration pollInterval) {
      return toBuilder().setPollInterval(pollInterval).build();
    }

    /**
     * Specifies a {@link Coder} to use for the outputs. If unspecified, it will be inferred from
     * the output type of {@link PollFn} whenever possible.
     *
     * <p>The coder must be deterministic, because the transform will compare encoded outputs for
     * deduplication between polling rounds.
     */
    public Growth<InputT, OutputT, KeyT> withOutputCoder(Coder<OutputT> outputCoder) {
      return toBuilder().setOutputCoder(outputCoder).build();
    }

    @Override
    public PCollection<KV<InputT, OutputT>> expand(PCollection<InputT> input) {
      checkNotNull(getPollInterval(), "pollInterval");
      checkNotNull(getTerminationPerInput(), "terminationPerInput");

      Coder<OutputT> outputCoder = getOutputCoder();
      if (outputCoder == null) {
        // If a coder was not specified explicitly, infer it from the OutputT type parameter
        // of the PollFn.
        TypeDescriptor<OutputT> outputT =
            TypeDescriptors.extractFromTypeParameters(
                getPollFn().getClosure(),
                PollFn.class,
                new TypeVariableExtractor<PollFn<InputT, OutputT>, OutputT>() {});
        try {
          outputCoder = input.getPipeline().getCoderRegistry().getCoder(outputT);
        } catch (CannotProvideCoderException e) {
          throw new RuntimeException(
              "Unable to infer coder for OutputT ("
                  + outputT
                  + "). Specify it explicitly using withOutputCoder().");
        }
      }

      Coder<KeyT> outputKeyCoder = getOutputKeyCoder();
      SerializableFunction<OutputT, KeyT> outputKeyFn = getOutputKeyFn();
      if (getOutputKeyFn() == null) {
        // This by construction can happen only if OutputT == KeyT
        outputKeyCoder = (Coder) outputCoder;
        outputKeyFn = (SerializableFunction) SerializableFunctions.identity();
      } else {
        if (outputKeyCoder == null) {
          // If a coder was not specified explicitly, infer it from the OutputT type parameter
          // of the output key fn.
          TypeDescriptor<KeyT> keyT = TypeDescriptors.outputOf(getOutputKeyFn());
          try {
            outputKeyCoder = input.getPipeline().getCoderRegistry().getCoder(keyT);
          } catch (CannotProvideCoderException e) {
            throw new RuntimeException(
                "Unable to infer coder for KeyT ("
                    + keyT
                    + "). Specify it explicitly using withOutputKeyCoder().");
          }
        }
        try {
          outputKeyCoder.verifyDeterministic();
        } catch (Coder.NonDeterministicException e) {
          throw new IllegalArgumentException(
              "Key coder " + outputKeyCoder + " must be deterministic");
        }
      }

      PCollection<KV<InputT, List<TimestampedValue<OutputT>>>> polledPc =
          input
              .apply(
                  ParDo.of(new WatchGrowthFn<>(this, outputCoder, outputKeyFn, outputKeyCoder))
                      .withSideInputs(getPollFn().getRequirements().getSideInputs()))
              .setCoder(
                  KvCoder.of(
                      input.getCoder(),
                      ListCoder.of(TimestampedValue.TimestampedValueCoder.of(outputCoder))));
      return polledPc
          .apply(ParDo.of(new PollResultSplitFn<>()))
          .setCoder(KvCoder.of(input.getCoder(), outputCoder));
    }
  }

  /** A splittable {@link DoFn} that emits {@link PollResult}s outputs. */
  @BoundedPerElement
  private static class PollResultSplitFn<InputT, OutputT>
      extends DoFn<KV<InputT, List<TimestampedValue<OutputT>>>, KV<InputT, OutputT>> {

    @ProcessElement
    public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
      long position = tracker.currentRestriction().getFrom();
      while (tracker.tryClaim(position)) {
        TimestampedValue<OutputT> value = c.element().getValue().get((int) position);
        c.outputWithTimestamp(KV.of(c.element().getKey(), value.getValue()), value.getTimestamp());
        position += 1L;
      }
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
      return currentElementTimestamp;
    }

    @NewWatermarkEstimator
    public WatermarkEstimators.MonotonicallyIncreasing newWatermarkEstimator(
        @WatermarkEstimatorState Instant watermarkEstimatorState) {
      return new WatermarkEstimators.MonotonicallyIncreasing(watermarkEstimatorState);
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(
        @Element KV<InputT, List<TimestampedValue<OutputT>>> element) {
      return new OffsetRange(0, element.getValue().size());
    }

    @NewTracker
    public OffsetRangeTracker newTracker(@Restriction OffsetRange restriction) {
      return restriction.newTracker();
    }

    @GetRestrictionCoder
    public Coder<OffsetRange> getRestrictionCoder() {
      return OffsetRange.Coder.of();
    }
  }

  @UnboundedPerElement
  private static class WatchGrowthFn<InputT, OutputT, KeyT, TerminationStateT>
      extends DoFn<InputT, KV<InputT, List<TimestampedValue<OutputT>>>> {
    private final Watch.Growth<InputT, OutputT, KeyT> spec;
    private final Coder<OutputT> outputCoder;
    private final SerializableFunction<OutputT, KeyT> outputKeyFn;
    private final Coder<KeyT> outputKeyCoder;
    private final Funnel<OutputT> coderFunnel;

    private WatchGrowthFn(
        Growth<InputT, OutputT, KeyT> spec,
        Coder<OutputT> outputCoder,
        SerializableFunction<OutputT, KeyT> outputKeyFn,
        Coder<KeyT> outputKeyCoder) {
      this.spec = spec;
      this.outputCoder = outputCoder;
      this.outputKeyFn = outputKeyFn;
      this.outputKeyCoder = outputKeyCoder;
      this.coderFunnel =
          (from, into) -> {
            try {
              // Rather than hashing the output itself, hash the output key.
              KeyT outputKey = outputKeyFn.apply(from);
              outputKeyCoder.encode(outputKey, Funnels.asOutputStream(into));
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          };
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
      return currentElementTimestamp;
    }

    @NewWatermarkEstimator
    public WatermarkEstimators.Manual newWatermarkEstimator(
        @WatermarkEstimatorState Instant watermarkEstimatorState) {
      return new WatermarkEstimators.Manual(watermarkEstimatorState);
    }

    @ProcessElement
    public ProcessContinuation process(
        ProcessContext c,
        RestrictionTracker<GrowthState, KV<Growth.PollResult<OutputT>, TerminationStateT>> tracker,
        ManualWatermarkEstimator<Instant> watermarkEstimator)
        throws Exception {

      GrowthState currentRestriction = tracker.currentRestriction();
      if (currentRestriction instanceof NonPollingGrowthState) {
        Growth.PollResult<OutputT> priorPoll =
            ((NonPollingGrowthState<OutputT>) currentRestriction).getPending();
        if (tracker.tryClaim(KV.of(priorPoll, null))) {
          if (!priorPoll.getOutputs().isEmpty()) {
            LOG.info(
                "{} - re-emitting output of prior poll containing {} results.",
                c.element(),
                priorPoll.getOutputs().size());
            c.output(KV.of(c.element(), priorPoll.getOutputs()));
          }
          watermarkEstimator.setWatermark(priorPoll.getWatermark());
        }
        return stop();
      }

      // Poll for additional elements.
      Instant now = Instant.now();
      Growth.PollResult<OutputT> res =
          spec.getPollFn().getClosure().apply(c.element(), wrapProcessContext(c));

      PollingGrowthState<TerminationStateT> pollingRestriction =
          (PollingGrowthState<TerminationStateT>) currentRestriction;
      // Produce a poll result that only contains never seen before results.
      Growth.PollResult<OutputT> newResults =
          computeNeverSeenBeforeResults(pollingRestriction, res);

      // If we had zero new results, attempt to update the watermark if the poll result
      // provided a watermark. Otherwise attempt to claim all pending outputs.
      LOG.info(
          "{} - current round of polling took {} ms and returned {} results, "
              + "of which {} were new.",
          c.element(),
          new Duration(now, Instant.now()).getMillis(),
          res.getOutputs().size(),
          newResults.getOutputs().size());

      TerminationStateT terminationState = pollingRestriction.getTerminationState();
      if (!newResults.getOutputs().isEmpty()) {
        terminationState =
            getTerminationCondition().onSeenNewOutput(Instant.now(), terminationState);
      }

      if (!tracker.tryClaim(KV.of(newResults, terminationState))) {
        LOG.info("{} - will not emit poll result tryClaim failed.", c.element());
        return stop();
      }

      if (!newResults.getOutputs().isEmpty()) {
        c.output(KV.of(c.element(), newResults.getOutputs()));
      }

      if (newResults.getWatermark() != null) {
        watermarkEstimator.setWatermark(newResults.getWatermark());
      }

      Instant currentTime = Instant.now();
      if (getTerminationCondition().canStopPolling(currentTime, terminationState)) {
        LOG.info(
            "{} - told to stop polling by polling function at {} with termination state {}.",
            c.element(),
            currentTime,
            getTerminationCondition().toString(terminationState));
        return stop();
      }

      if (BoundedWindow.TIMESTAMP_MAX_VALUE.equals(newResults.getWatermark())) {
        LOG.info("{} - will stop polling, reached max timestamp.", c.element());
        return stop();
      }

      LOG.info(
          "{} - will resume polling in {} ms.", c.element(), spec.getPollInterval().getMillis());
      return resume().withResumeDelay(spec.getPollInterval());
    }

    private HashCode hash128(OutputT value) {
      return Hashing.murmur3_128().hashObject(value, coderFunnel);
    }

    private Growth.PollResult computeNeverSeenBeforeResults(
        PollingGrowthState<TerminationStateT> state, Growth.PollResult<OutputT> pollResult) {
      // Collect results to include as newly pending. Note that the poll result may in theory
      // contain multiple outputs mapping to the same output key - we need to ignore duplicates
      // here already.
      Map<HashCode, TimestampedValue<OutputT>> newPending = Maps.newHashMap();
      for (TimestampedValue<OutputT> output : pollResult.getOutputs()) {
        OutputT value = output.getValue();
        HashCode hash = hash128(value);
        if (state.getCompleted().containsKey(hash) || newPending.containsKey(hash)) {
          continue;
        }
        // TODO (https://issues.apache.org/jira/browse/BEAM-2680):
        // Consider adding only at most N pending elements and ignoring others,
        // instead relying on future poll rounds to provide them, in order to avoid
        // blowing up the state. Combined with garbage collection of PollingGrowthState.completed,
        // this would make the transform scalable to very large poll results.
        newPending.put(hash, output);
      }

      return pollResult.withOutputs(
          Ordering.natural()
              .onResultOf((TimestampedValue<OutputT> value) -> value.getTimestamp())
              .sortedCopy(newPending.values()));
    }

    private Growth.TerminationCondition<InputT, TerminationStateT> getTerminationCondition() {
      return (Growth.TerminationCondition<InputT, TerminationStateT>) spec.getTerminationPerInput();
    }

    @GetInitialRestriction
    public GrowthState getInitialRestriction(@Element InputT element) {
      return PollingGrowthState.of(getTerminationCondition().forNewInput(Instant.now(), element));
    }

    @NewTracker
    public GrowthTracker<OutputT, TerminationStateT> newTracker(
        @Restriction GrowthState restriction) {
      return new GrowthTracker<>(restriction, coderFunnel);
    }

    @GetRestrictionCoder
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Coder<GrowthState> getRestrictionCoder() {
      return SnappyCoder.of(
          GrowthStateCoder.of(outputCoder, (Coder) spec.getTerminationPerInput().getStateCoder()));
    }
  }

  /** A base class for all restrictions related to the {@link Growth} SplittableDoFn. */
  abstract static class GrowthState {}

  /**
   * Stores the prior pending poll results related to the {@link Growth} SplittableDoFn. Used to
   * represent the primary restriction during checkpoint which should be replayed if the primary
   * ever needs to be re-executed.
   */
  @AutoValue
  @VisibleForTesting
  abstract static class NonPollingGrowthState<OutputT> extends GrowthState {
    public static <OutputT> NonPollingGrowthState<OutputT> of(Growth.PollResult<OutputT> pending) {
      return new AutoValue_Watch_NonPollingGrowthState(pending);
    }

    /**
     * Contains all pending results to output. Checkpointing/splitting moves "pending" outputs to
     * the completed set.
     */
    public abstract Growth.PollResult<OutputT> getPending();
  }

  /**
   * A restriction for the {@link Growth} transform which represents a polling state. The
   * restriction represents an unbounded amount of work until one of the termination conditions of
   * the {@link Growth} transform are met.
   */
  @AutoValue
  @VisibleForTesting
  abstract static class PollingGrowthState<TerminationStateT> extends GrowthState {
    public static <TerminationStateT> PollingGrowthState<TerminationStateT> of(
        TerminationStateT terminationState) {
      return new AutoValue_Watch_PollingGrowthState(ImmutableMap.of(), null, terminationState);
    }

    public static <TerminationStateT> PollingGrowthState<TerminationStateT> of(
        ImmutableMap<HashCode, Instant> completed,
        Instant pollWatermark,
        TerminationStateT terminationState) {
      return new AutoValue_Watch_PollingGrowthState(completed, pollWatermark, terminationState);
    }

    // Hashes and timestamps of outputs that have already been output and should be omitted
    // from future polls. Timestamps are preserved to allow garbage-collecting this state
    // in the future, e.g. dropping elements from "completed" and from
    // computeNeverSeenBeforeResults() if their timestamp is more than X behind the watermark.
    // As of writing, we don't do this, but preserve the information for forward compatibility
    // in case of pipeline update. TODO: do this.
    public abstract ImmutableMap<HashCode, Instant> getCompleted();

    public abstract @Nullable Instant getPollWatermark();

    public abstract TerminationStateT getTerminationState();
  }

  @VisibleForTesting
  static class GrowthTracker<OutputT, TerminationStateT>
      extends RestrictionTracker<GrowthState, KV<Growth.PollResult<OutputT>, TerminationStateT>> {

    static final GrowthState EMPTY_STATE =
        NonPollingGrowthState.of(new PollResult<>(Collections.emptyList(), null));

    // Used to hash values.
    private final Funnel<OutputT> coderFunnel;

    // non-null after first successful tryClaim()
    private Growth.@Nullable PollResult<OutputT> claimedPollResult;
    private @Nullable TerminationStateT claimedTerminationState;
    private @Nullable ImmutableMap<HashCode, Instant> claimedHashes;

    // The restriction describing the entire work to be done by the current ProcessElement call.
    private GrowthState state;
    // Whether we should stop claiming poll results.
    private boolean shouldStop;

    GrowthTracker(GrowthState state, Funnel<OutputT> coderFunnel) {
      this.state = state;
      this.coderFunnel = coderFunnel;
      this.shouldStop = false;
    }

    @Override
    public GrowthState currentRestriction() {
      return state;
    }

    @Override
    public SplitResult<GrowthState> trySplit(double fractionOfRemainder) {
      // TODO(BEAM-8873): Add support for splitting off a fixed amount of work for this restriction
      // instead of only supporting checkpointing.

      // residual should contain exactly the work *not* claimed in the current ProcessElement call -
      // unclaimed pending outputs or future polling output
      GrowthState residual;

      if (claimedPollResult == null) {
        // If we have yet to claim anything then our residual becomes all the work we were meant
        // to do and we update our current restriction to be empty.
        residual = state;
        state = EMPTY_STATE;
      } else if (state instanceof NonPollingGrowthState) {
        // Since we have claimed the prior poll, our residual is empty.
        residual = EMPTY_STATE;
      } else {
        // Since we claimed a poll result, our primary becomes the poll result and
        // our residual becomes everything we have claimed in the past + the current poll result.

        PollingGrowthState<TerminationStateT> currentState =
            (PollingGrowthState<TerminationStateT>) state;
        ImmutableMap.Builder<HashCode, Instant> newCompleted = ImmutableMap.builder();
        newCompleted.putAll(currentState.getCompleted());
        newCompleted.putAll(claimedHashes);
        residual =
            PollingGrowthState.of(
                newCompleted.build(),
                Ordering.natural()
                    .nullsFirst()
                    .max(currentState.getPollWatermark(), claimedPollResult.watermark),
                claimedTerminationState);
        state = NonPollingGrowthState.of(claimedPollResult);
      }

      shouldStop = true;
      return SplitResult.of(state, residual);
    }

    private HashCode hash128(OutputT value) {
      return Hashing.murmur3_128().hashObject(value, coderFunnel);
    }

    @Override
    public void checkDone() throws IllegalStateException {
      checkState(
          shouldStop, "Missing tryClaim()/checkpoint() call. Expected " + "one or the other.");
    }

    @Override
    public IsBounded isBounded() {
      if (state == EMPTY_STATE || state instanceof NonPollingGrowthState) {
        return IsBounded.BOUNDED;
      }
      return IsBounded.UNBOUNDED;
    }

    @Override
    public boolean tryClaim(KV<Growth.PollResult<OutputT>, TerminationStateT> pollResult) {
      if (shouldStop) {
        return false;
      }

      ImmutableMap.Builder<HashCode, Instant> newClaimedHashesBuilder = ImmutableMap.builder();
      for (TimestampedValue<OutputT> value : pollResult.getKey().getOutputs()) {
        HashCode hash = hash128(value.getValue());
        newClaimedHashesBuilder.put(hash, value.getTimestamp());
      }
      ImmutableMap<HashCode, Instant> newClaimedHashes = newClaimedHashesBuilder.build();

      if (state instanceof PollingGrowthState) {
        // If we have previously claimed one of these hashes then return false.
        if (!Collections.disjoint(
            newClaimedHashes.keySet(), ((PollingGrowthState) state).getCompleted().keySet())) {
          return false;
        }
      } else {
        Set<HashCode> expectedHashesToClaim = new HashSet<>();
        for (TimestampedValue<OutputT> value :
            ((NonPollingGrowthState<OutputT>) state).getPending().getOutputs()) {
          expectedHashesToClaim.add(hash128(value.getValue()));
        }
        // We expect to claim the entire poll result from a NonPollingGrowthState. This is
        // stricter then currently required and could be relaxed if this tracker supported
        // splitting a NonPollingGrowthState into two smaller NonPollingGrowthStates.
        if (!expectedHashesToClaim.equals(newClaimedHashes.keySet())) {
          return false;
        }
      }

      // Only allow claiming a single poll result at a time.
      shouldStop = true;
      claimedPollResult = pollResult.getKey();
      claimedTerminationState = pollResult.getValue();
      claimedHashes = newClaimedHashes;

      return true;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("state", state)
          .add("pollResult", claimedPollResult)
          .add("terminationState", claimedTerminationState)
          .add("shouldStop", shouldStop)
          .toString();
    }
  }

  private static class HashCode128Coder extends AtomicCoder<HashCode> {
    private static final HashCode128Coder INSTANCE = new HashCode128Coder();

    public static HashCode128Coder of() {
      return INSTANCE;
    }

    @Override
    public void encode(HashCode value, OutputStream os) throws IOException {
      checkArgument(
          value.bits() == 128, "Expected a 128-bit hash code, but got %s bits", value.bits());
      byte[] res = new byte[16];
      value.writeBytesTo(res, 0, 16);
      os.write(res);
    }

    @Override
    public HashCode decode(InputStream is) throws IOException {
      byte[] res = new byte[16];
      int numRead = is.read(res, 0, 16);
      checkArgument(numRead == 16, "Expected to read 16 bytes, but read %s", numRead);
      return HashCode.fromBytes(res);
    }
  }

  static class GrowthStateCoder<OutputT, TerminationStateT> extends StructuredCoder<GrowthState> {

    private static final int POLLING_GROWTH_STATE = 0;
    private static final int NON_POLLING_GROWTH_STATE = 1;

    public static <OutputT, TerminationStateT> GrowthStateCoder<OutputT, TerminationStateT> of(
        Coder<OutputT> outputCoder, Coder<TerminationStateT> terminationStateCoder) {
      return new GrowthStateCoder<>(outputCoder, terminationStateCoder);
    }

    private static final MapCoder<HashCode, Instant> COMPLETED_CODER =
        MapCoder.of(HashCode128Coder.of(), InstantCoder.of());
    private static final Coder<Instant> NULLABLE_INSTANT_CODER =
        NullableCoder.of(InstantCoder.of());

    private final Coder<OutputT> outputCoder;
    private final Coder<List<TimestampedValue<OutputT>>> timestampedOutputCoder;
    private final Coder<TerminationStateT> terminationStateCoder;

    private GrowthStateCoder(
        Coder<OutputT> outputCoder, Coder<TerminationStateT> terminationStateCoder) {
      this.outputCoder = outputCoder;
      this.terminationStateCoder = terminationStateCoder;
      this.timestampedOutputCoder =
          ListCoder.of(TimestampedValue.TimestampedValueCoder.of(outputCoder));
    }

    @Override
    public void encode(GrowthState value, OutputStream os) throws IOException {
      if (value instanceof PollingGrowthState) {
        VarInt.encode(POLLING_GROWTH_STATE, os);
        encodePollingGrowthState((PollingGrowthState<TerminationStateT>) value, os);
      } else if (value instanceof NonPollingGrowthState) {
        VarInt.encode(NON_POLLING_GROWTH_STATE, os);
        encodeNonPollingGrowthState((NonPollingGrowthState<OutputT>) value, os);
      } else {
        throw new IOException("Unknown growth state: " + value);
      }
    }

    private void encodePollingGrowthState(
        PollingGrowthState<TerminationStateT> value, OutputStream os) throws IOException {
      terminationStateCoder.encode(value.getTerminationState(), os);
      NULLABLE_INSTANT_CODER.encode(value.getPollWatermark(), os);
      COMPLETED_CODER.encode(value.getCompleted(), os);
    }

    private void encodeNonPollingGrowthState(NonPollingGrowthState<OutputT> value, OutputStream os)
        throws IOException {
      NULLABLE_INSTANT_CODER.encode(value.getPending().getWatermark(), os);
      timestampedOutputCoder.encode(value.getPending().getOutputs(), os);
    }

    @Override
    public GrowthState decode(InputStream is) throws IOException {
      int type = VarInt.decodeInt(is);
      switch (type) {
        case NON_POLLING_GROWTH_STATE:
          return decodeNonPollingGrowthState(is);
        case POLLING_GROWTH_STATE:
          return decodePollingGrowthState(is);
        default:
          throw new IOException("Unknown growth state type " + type);
      }
    }

    private GrowthState decodeNonPollingGrowthState(InputStream is) throws IOException {
      Instant watermark = NULLABLE_INSTANT_CODER.decode(is);
      List<TimestampedValue<OutputT>> values = timestampedOutputCoder.decode(is);
      return NonPollingGrowthState.of(new Growth.PollResult<>(values, watermark));
    }

    private GrowthState decodePollingGrowthState(InputStream is) throws IOException {
      TerminationStateT terminationState = terminationStateCoder.decode(is);
      Instant watermark = NULLABLE_INSTANT_CODER.decode(is);
      Map<HashCode, Instant> completed = COMPLETED_CODER.decode(is);
      return PollingGrowthState.of(ImmutableMap.copyOf(completed), watermark, terminationState);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Arrays.asList(outputCoder, terminationStateCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      outputCoder.verifyDeterministic();
    }
  }
}
