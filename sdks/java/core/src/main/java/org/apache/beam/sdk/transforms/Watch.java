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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.sdk.transforms.Contextful.Fn.Context.wrapProcessContext;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.resume;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.stop;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DurationCoder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SnappyCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.Contextful.Fn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.TypeDescriptors.TypeVariableExtractor;
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
@Experimental(Experimental.Kind.SPLITTABLE_DO_FN)
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
      @Nullable private final Instant watermark;

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
       * Sets the watermark - an approximate lower bound on timestamps of future new outputs from
       * this {@link PollFn}.
       */
      public PollResult<OutputT> withWatermark(Instant watermark) {
        checkNotNull(watermark, "watermark");
        return new PollResult<>(outputs, watermark);
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
     * Wraps a given input-independent {@link TerminationCondition} as an equivalent condition
     * with a given input type, passing {@code null} to the original condition as input.
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

    @Nullable
    abstract SerializableFunction<OutputT, KeyT> getOutputKeyFn();

    @Nullable
    abstract Coder<KeyT> getOutputKeyCoder();

    @Nullable
    abstract Duration getPollInterval();

    @Nullable
    abstract TerminationCondition<InputT, ?> getTerminationPerInput();

    @Nullable
    abstract Coder<OutputT> getOutputCoder();

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

      return input
          .apply(ParDo.of(new WatchGrowthFn<>(this, outputCoder, outputKeyFn, outputKeyCoder))
          .withSideInputs(getPollFn().getRequirements().getSideInputs()))
          .setCoder(KvCoder.of(input.getCoder(), outputCoder));
    }
  }

  private static class WatchGrowthFn<InputT, OutputT, KeyT, TerminationStateT>
      extends DoFn<InputT, KV<InputT, OutputT>> {
    private final Watch.Growth<InputT, OutputT, KeyT> spec;
    private final Coder<OutputT> outputCoder;
    private final SerializableFunction<OutputT, KeyT> outputKeyFn;
    private final Coder<KeyT> outputKeyCoder;

    private WatchGrowthFn(
        Growth<InputT, OutputT, KeyT> spec,
        Coder<OutputT> outputCoder,
        SerializableFunction<OutputT, KeyT> outputKeyFn,
        Coder<KeyT> outputKeyCoder) {
      this.spec = spec;
      this.outputCoder = outputCoder;
      this.outputKeyFn = outputKeyFn;
      this.outputKeyCoder = outputKeyCoder;
    }

    @ProcessElement
    public ProcessContinuation process(
        ProcessContext c, final GrowthTracker<OutputT, KeyT, TerminationStateT> tracker)
        throws Exception {
      if (!tracker.hasPending() && !tracker.currentRestriction().isOutputComplete) {
        Instant now = Instant.now();
        Growth.PollResult<OutputT> res =
            spec.getPollFn().getClosure().apply(c.element(), wrapProcessContext(c));
        // TODO (https://issues.apache.org/jira/browse/BEAM-2680):
        // Consider truncating the pending outputs if there are too many, to avoid blowing
        // up the state. In that case, we'd rely on the next poll cycle to provide more outputs.
        // All outputs would still have to be stored in state.completed, but it is more compact
        // because it stores hashes and because it could potentially be garbage-collected.
        int numPending = tracker.addNewAsPending(res);
        if (numPending > 0) {
          LOG.info(
              "{} - current round of polling took {} ms and returned {} results, "
                  + "of which {} were new. The output is {}.",
              c.element(),
              new Duration(now, Instant.now()).getMillis(),
              res.getOutputs().size(),
              numPending,
              BoundedWindow.TIMESTAMP_MAX_VALUE.equals(res.getWatermark())
                  ? "final"
                  : "not yet final");
        }
      }
      int numEmittedInThisRound = 0;
      int numTotalPending = tracker.getNumPending();
      int numPreviouslyEmitted = tracker.currentRestriction().completed.size();
      int numTotalKnown = numPreviouslyEmitted + numTotalPending;
      while (true) {
        c.updateWatermark(tracker.getWatermark());
        Map.Entry<HashCode, TimestampedValue<OutputT>> entry = tracker.getNextPending();
        if (entry == null || !tracker.tryClaim(entry.getKey())) {
          break;
        }
        TimestampedValue<OutputT> nextPending = entry.getValue();
        c.outputWithTimestamp(
            KV.of(c.element(), nextPending.getValue()), nextPending.getTimestamp());
        ++numEmittedInThisRound;
      }
      LOG.info(
          "{} - emitted {} new results (of {} total known: {} emitted so far, {} more to emit).",
          c.element(),
          numEmittedInThisRound,
          numTotalKnown,
          numEmittedInThisRound + numPreviouslyEmitted,
          numTotalPending - numEmittedInThisRound);
      Instant watermark = tracker.getWatermark();
      if (watermark != null) {
        // Null means the poll result did not provide a watermark and there were no new elements,
        // so we have no information to update the watermark and should keep it as-is.
        c.updateWatermark(watermark);
      }
      // No more pending outputs - future output will come from more polling,
      // unless output is complete or termination condition is reached.
      if (tracker.shouldPollMore()) {
        LOG.info(
            "{} - emitted all {} known results so far; will resume polling in {} ms",
            c.element(),
            numTotalKnown,
            spec.getPollInterval().getMillis());
        return resume().withResumeDelay(spec.getPollInterval());
      }
      return stop();
    }

    private Growth.TerminationCondition<InputT, TerminationStateT> getTerminationCondition() {
      return (Growth.TerminationCondition<InputT, TerminationStateT>) spec.getTerminationPerInput();
    }

    @GetInitialRestriction
    public GrowthState<OutputT, KeyT, TerminationStateT> getInitialRestriction(InputT element) {
      return new GrowthState<>(getTerminationCondition().forNewInput(Instant.now(), element));
    }

    @NewTracker
    public GrowthTracker<OutputT, KeyT, TerminationStateT> newTracker(
        GrowthState<OutputT, KeyT, TerminationStateT> restriction) {
      return new GrowthTracker<>(
          outputKeyFn, outputKeyCoder, restriction, getTerminationCondition());
    }

    @GetRestrictionCoder
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Coder<GrowthState<OutputT, KeyT, TerminationStateT>> getRestrictionCoder() {
      return SnappyCoder.of(GrowthStateCoder.of(
          outputCoder, (Coder) spec.getTerminationPerInput().getStateCoder()));
    }
  }

  @VisibleForTesting
  static class GrowthState<OutputT, KeyT, TerminationStateT> {
    // Hashes and timestamps of outputs that have already been output and should be omitted
    // from future polls. Timestamps are preserved to allow garbage-collecting this state
    // in the future, e.g. dropping elements from "completed" and from addNewAsPending() if their
    // timestamp is more than X behind the watermark.
    // As of writing, we don't do this, but preserve the information for forward compatibility
    // in case of pipeline update. TODO: do this.
    private final ImmutableMap<HashCode, Instant> completed;
    // Outputs that are known to be present in a poll result, but have not yet been returned
    // from a ProcessElement call, sorted by timestamp to help smooth watermark progress.
    private final ImmutableMap<HashCode, TimestampedValue<OutputT>> pending;
    // If true, processing of this restriction should only output "pending". Otherwise, it should
    // also continue polling.
    private final boolean isOutputComplete;
    // Can be null only if isOutputComplete is true.
    @Nullable private final TerminationStateT terminationState;
    // A lower bound on timestamps of future outputs from PollFn, excluding completed and pending.
    @Nullable private final Instant pollWatermark;

    GrowthState(TerminationStateT terminationState) {
      this.completed = ImmutableMap.of();
      this.pending = ImmutableMap.of();
      this.isOutputComplete = false;
      this.terminationState = checkNotNull(terminationState);
      this.pollWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    GrowthState(
        ImmutableMap<HashCode, Instant> completed,
        ImmutableMap<HashCode, TimestampedValue<OutputT>> pending,
        boolean isOutputComplete,
        @Nullable TerminationStateT terminationState,
        @Nullable Instant pollWatermark) {
      if (!isOutputComplete) {
        checkNotNull(terminationState);
      }
      this.completed = completed;
      this.pending = pending;
      this.isOutputComplete = isOutputComplete;
      this.terminationState = terminationState;
      this.pollWatermark = pollWatermark;
    }

    public String toString(Growth.TerminationCondition<?, TerminationStateT> terminationCondition) {
      return "GrowthState{"
          + "completed=<"
          + completed.size()
          + " elements>, pending=<"
          + pending.size()
          + " elements"
          + (pending.isEmpty() ? "" : (", earliest " + pending.values().iterator().next()))
          + ">, isOutputComplete="
          + isOutputComplete
          + ", terminationState="
          + terminationCondition.toString(terminationState)
          + ", pollWatermark="
          + pollWatermark
          + '}';
    }
  }

  @VisibleForTesting
  static class GrowthTracker<OutputT, KeyT, TerminationStateT>
      extends RestrictionTracker<GrowthState<OutputT, KeyT, TerminationStateT>, HashCode> {
    private final Funnel<OutputT> coderFunnel;
    private final Growth.TerminationCondition<?, TerminationStateT> terminationCondition;

    // The restriction describing the entire work to be done by the current ProcessElement call.
    // Changes only in checkpoint().
    private GrowthState<OutputT, KeyT, TerminationStateT> state;

    // Mutable state changed by the ProcessElement call itself, and used to compute the primary
    // and residual restrictions in checkpoint().

    // Remaining pending outputs; initialized from state.pending (if non-empty) or in
    // addNewAsPending(); drained via tryClaimNextPending().
    private Map<HashCode, TimestampedValue<OutputT>> pending;
    // Outputs that have been claimed in the current ProcessElement call. A prefix of "pending".
    private Map<HashCode, TimestampedValue<OutputT>> claimed = Maps.newLinkedHashMap();
    private boolean isOutputComplete;
    @Nullable private TerminationStateT terminationState;
    @Nullable private Instant pollWatermark;
    private boolean shouldStop = false;

    GrowthTracker(
        final SerializableFunction<OutputT, KeyT> keyFn,
        final Coder<KeyT> outputKeyCoder,
        GrowthState<OutputT, KeyT, TerminationStateT> state,
        Growth.TerminationCondition<?, TerminationStateT> terminationCondition) {
      this.coderFunnel =
          (from, into) -> {
            try {
              // Rather than hashing the output itself, hash the output key.
              KeyT outputKey = keyFn.apply(from);
              outputKeyCoder.encode(outputKey, Funnels.asOutputStream(into));
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          };
      this.terminationCondition = terminationCondition;
      this.state = state;
      this.isOutputComplete = state.isOutputComplete;
      this.pollWatermark = state.pollWatermark;
      this.terminationState = state.terminationState;
      this.pending = Maps.newLinkedHashMap(state.pending);
    }

    @Override
    public synchronized GrowthState<OutputT, KeyT, TerminationStateT> currentRestriction() {
      return state;
    }

    @Override
    public synchronized GrowthState<OutputT, KeyT, TerminationStateT> checkpoint() {
      checkState(
          !claimed.isEmpty(), "Can't checkpoint before any element was successfully claimed");

      // primary should contain exactly the work claimed in the current ProcessElement call - i.e.
      // claimed outputs become pending, and it shouldn't poll again.
      GrowthState<OutputT, KeyT, TerminationStateT> primary =
          new GrowthState<>(
              state.completed /* completed */,
              ImmutableMap.copyOf(claimed) /* pending */,
              true /* isOutputComplete */,
              null /* terminationState */,
              BoundedWindow.TIMESTAMP_MAX_VALUE /* pollWatermark */);

      // residual should contain exactly the work *not* claimed in the current ProcessElement call -
      // unclaimed pending outputs plus future polling outputs.
      ImmutableMap.Builder<HashCode, Instant> newCompleted = ImmutableMap.builder();
      newCompleted.putAll(state.completed);
      for (Map.Entry<HashCode, TimestampedValue<OutputT>> claimedOutput : claimed.entrySet()) {
        newCompleted.put(
            claimedOutput.getKey(), claimedOutput.getValue().getTimestamp());
      }
      GrowthState<OutputT, KeyT, TerminationStateT> residual =
          new GrowthState<>(
              newCompleted.build() /* completed */,
              ImmutableMap.copyOf(pending) /* pending */,
              isOutputComplete /* isOutputComplete */,
              terminationState,
              pollWatermark);

      // Morph ourselves into primary, except for "pending" - the current call has already claimed
      // everything from it.
      this.state = primary;
      this.isOutputComplete = primary.isOutputComplete;
      this.pollWatermark = primary.pollWatermark;
      this.terminationState = null;
      this.pending = Maps.newLinkedHashMap();

      this.shouldStop = true;
      return residual;
    }

    private HashCode hash128(OutputT value) {
      return Hashing.murmur3_128().hashObject(value, coderFunnel);
    }

    @Override
    public synchronized void checkDone() throws IllegalStateException {
      if (shouldStop) {
        return;
      }
      checkState(!shouldPollMore(), "Polling is still allowed to continue");
      checkState(pending.isEmpty(), "There are %s unclaimed pending outputs", pending.size());
    }

    @VisibleForTesting
    synchronized boolean hasPending() {
      return !pending.isEmpty();
    }

    private synchronized int getNumPending() {
      return pending.size();
    }

    @VisibleForTesting
    @Nullable
    synchronized Map.Entry<HashCode, TimestampedValue<OutputT>> getNextPending() {
      if (pending.isEmpty()) {
        return null;
      }
      return pending.entrySet().iterator().next();
    }

    @Override
    protected synchronized boolean tryClaimImpl(HashCode hash) {
      if (shouldStop) {
        return false;
      }
      checkState(!pending.isEmpty(), "No more unclaimed pending outputs");
      TimestampedValue<OutputT> value = pending.remove(hash);
      checkArgument(value != null, "Attempted to claim unknown hash %s", hash);
      claimed.put(hash, value);
      return true;
    }

    @VisibleForTesting
    synchronized boolean shouldPollMore() {
      return !isOutputComplete
          && !terminationCondition.canStopPolling(Instant.now(), terminationState);
    }

    @VisibleForTesting
    synchronized int addNewAsPending(Growth.PollResult<OutputT> pollResult) {
      checkState(
          state.pending.isEmpty(),
          "Should have drained all old pending outputs before adding new, "
              + "but there are %s old pending outputs",
          state.pending.size());
      // Collect results to include as newly pending. Note that the poll result may in theory
      // contain multiple outputs mapping to the the same output key - we need to ignore duplicates
      // here already.
      Map<HashCode, TimestampedValue<OutputT>> newPending = Maps.newHashMap();
      for (TimestampedValue<OutputT> output : pollResult.getOutputs()) {
        OutputT value = output.getValue();
        HashCode hash = hash128(value);
        if (state.completed.containsKey(hash) || newPending.containsKey(hash)) {
          continue;
        }
        // TODO (https://issues.apache.org/jira/browse/BEAM-2680):
        // Consider adding only at most N pending elements and ignoring others,
        // instead relying on future poll rounds to provide them, in order to avoid
        // blowing up the state. Combined with garbage collection of GrowthState.completed,
        // this would make the transform scalable to very large poll results.
        newPending.put(hash, TimestampedValue.of(value, output.getTimestamp()));
      }
      if (!newPending.isEmpty()) {
        terminationState = terminationCondition.onSeenNewOutput(Instant.now(), terminationState);
      }

      List<Map.Entry<HashCode, TimestampedValue<OutputT>>> sortedPending =
          Ordering.natural()
              .onResultOf(
                  (Map.Entry<HashCode, TimestampedValue<OutputT>> entry) ->
                      entry.getValue().getTimestamp())
              .sortedCopy(newPending.entrySet());
      this.pending = Maps.newLinkedHashMap();
      for (Map.Entry<HashCode, TimestampedValue<OutputT>> entry : sortedPending) {
        this.pending.put(entry.getKey(), entry.getValue());
      }
      // If poll result doesn't provide a watermark, assume that future new outputs may
      // arrive with about the same timestamps as the current new outputs.
      if (pollResult.getWatermark() != null) {
        this.pollWatermark = pollResult.getWatermark();
      } else if (!pending.isEmpty()) {
        this.pollWatermark = pending.values().iterator().next().getTimestamp();
      }
      if (BoundedWindow.TIMESTAMP_MAX_VALUE.equals(pollWatermark)) {
        isOutputComplete = true;
      }
      return pending.size();
    }

    @VisibleForTesting
    synchronized Instant getWatermark() {
      // Future elements that can be claimed in this restriction come either from
      // "pending" or from future polls, so the total watermark is
      // min(watermark for future polling, earliest remaining pending element)
      return Ordering.natural()
          .nullsLast()
          .min(
              pollWatermark,
              pending.isEmpty() ? null : pending.values().iterator().next().getTimestamp());
    }

    @Override
    public synchronized String toString() {
      return "GrowthTracker{"
          + "state="
          + state.toString(terminationCondition)
          + ", pending=<"
          + pending.size()
          + " elements"
          + (pending.isEmpty() ? "" : (", earliest " + pending.values().iterator().next()))
          + ">, claimed=<"
          + claimed.size()
          + " elements>, isOutputComplete="
          + isOutputComplete
          + ", terminationState="
          + terminationState
          + ", pollWatermark="
          + pollWatermark
          + ", shouldStop="
          + shouldStop
          + '}';
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

  private static class GrowthStateCoder<OutputT, KeyT, TerminationStateT>
      extends StructuredCoder<GrowthState<OutputT, KeyT, TerminationStateT>> {
    public static <OutputT, KeyT, TerminationStateT>
        GrowthStateCoder<OutputT, KeyT, TerminationStateT> of(
            Coder<OutputT> outputCoder, Coder<TerminationStateT> terminationStateCoder) {
      return new GrowthStateCoder<>(outputCoder, terminationStateCoder);
    }

    private static final Coder<Boolean> BOOLEAN_CODER = BooleanCoder.of();
    private static final Coder<Instant> INSTANT_CODER = NullableCoder.of(InstantCoder.of());
    private static final Coder<HashCode> HASH_CODE_CODER = HashCode128Coder.of();

    private final Coder<OutputT> outputCoder;
    private final Coder<Map<HashCode, Instant>> completedCoder;
    private final Coder<TimestampedValue<OutputT>> timestampedOutputCoder;
    private final Coder<TerminationStateT> terminationStateCoder;

    private GrowthStateCoder(
        Coder<OutputT> outputCoder, Coder<TerminationStateT> terminationStateCoder) {
      this.outputCoder = outputCoder;
      this.terminationStateCoder = terminationStateCoder;
      this.completedCoder = MapCoder.of(HASH_CODE_CODER, INSTANT_CODER);
      this.timestampedOutputCoder = TimestampedValue.TimestampedValueCoder.of(outputCoder);
    }

    @Override
    public void encode(GrowthState<OutputT, KeyT, TerminationStateT> value, OutputStream os)
        throws IOException {
      completedCoder.encode(value.completed, os);
      VarIntCoder.of().encode(value.pending.size(), os);
      for (Map.Entry<HashCode, TimestampedValue<OutputT>> entry : value.pending.entrySet()) {
        HASH_CODE_CODER.encode(entry.getKey(), os);
        timestampedOutputCoder.encode(entry.getValue(), os);
      }
      BOOLEAN_CODER.encode(value.isOutputComplete, os);
      terminationStateCoder.encode(value.terminationState, os);
      INSTANT_CODER.encode(value.pollWatermark, os);
    }

    @Override
    public GrowthState<OutputT, KeyT, TerminationStateT> decode(InputStream is) throws IOException {
      Map<HashCode, Instant> completed = completedCoder.decode(is);
      int numPending = VarIntCoder.of().decode(is);
      ImmutableMap.Builder<HashCode, TimestampedValue<OutputT>> pending = ImmutableMap.builder();
      for (int i = 0; i < numPending; ++i) {
        HashCode hash = HASH_CODE_CODER.decode(is);
        TimestampedValue<OutputT> output = timestampedOutputCoder.decode(is);
        pending.put(hash, output);
      }
      boolean isOutputComplete = BOOLEAN_CODER.decode(is);
      TerminationStateT terminationState = terminationStateCoder.decode(is);
      Instant pollWatermark = INSTANT_CODER.decode(is);
      return new GrowthState<>(
          ImmutableMap.copyOf(completed),
          pending.build(),
          isOutputComplete,
          terminationState,
          pollWatermark);
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
