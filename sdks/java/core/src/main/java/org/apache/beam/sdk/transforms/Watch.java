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
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.resume;
import static org.apache.beam.sdk.transforms.DoFn.ProcessContinuation.stop;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.TypeVariable;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
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
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
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
  public static <InputT, OutputT> Growth<InputT, OutputT> growthOf(
      Growth.PollFn<InputT, OutputT> pollFn) {
    return new AutoValue_Watch_Growth.Builder<InputT, OutputT>()
        .setTerminationPerInput(Watch.Growth.<InputT>never())
        .setPollFn(pollFn)
        .build();
  }

  /** Implementation of {@link #growthOf}. */
  @AutoValue
  public abstract static class Growth<InputT, OutputT>
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
     * A function that computes the current set of outputs for the given input (given as a {@link
     * TimestampedValue}), in the form of a {@link PollResult}.
     */
    public interface PollFn<InputT, OutputT> extends Serializable {
      PollResult<OutputT> apply(InputT input, Instant timestamp) throws Exception;
    }

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
      StateT forNewInput(Instant now, InputT input);

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

    abstract PollFn<InputT, OutputT> getPollFn();

    @Nullable
    abstract Duration getPollInterval();

    @Nullable
    abstract TerminationCondition<InputT, ?> getTerminationPerInput();

    @Nullable
    abstract Coder<OutputT> getOutputCoder();

    abstract Builder<InputT, OutputT> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<InputT, OutputT> {
      abstract Builder<InputT, OutputT> setPollFn(PollFn<InputT, OutputT> pollFn);

      abstract Builder<InputT, OutputT> setTerminationPerInput(
          TerminationCondition<InputT, ?> terminationPerInput);

      abstract Builder<InputT, OutputT> setPollInterval(Duration pollInterval);

      abstract Builder<InputT, OutputT> setOutputCoder(Coder<OutputT> outputCoder);

      abstract Growth<InputT, OutputT> build();
    }

    /** Specifies a {@link TerminationCondition} that will be independently used for every input. */
    public Growth<InputT, OutputT> withTerminationPerInput(
        TerminationCondition<InputT, ?> terminationPerInput) {
      return toBuilder().setTerminationPerInput(terminationPerInput).build();
    }

    /**
     * Specifies how long to wait after a call to {@link PollFn} before calling it again (if at all
     * - according to {@link PollResult} and the {@link TerminationCondition}).
     */
    public Growth<InputT, OutputT> withPollInterval(Duration pollInterval) {
      return toBuilder().setPollInterval(pollInterval).build();
    }

    /**
     * Specifies a {@link Coder} to use for the outputs. If unspecified, it will be inferred from
     * the output type of {@link PollFn} whenever possible.
     *
     * <p>The coder must be deterministic, because the transform will compare encoded outputs for
     * deduplication between polling rounds.
     */
    public Growth<InputT, OutputT> withOutputCoder(Coder<OutputT> outputCoder) {
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
        TypeDescriptor<?> superDescriptor =
            TypeDescriptor.of(getPollFn().getClass()).getSupertype(PollFn.class);
        TypeVariable typeVariable = superDescriptor.getTypeParameter("OutputT");
        @SuppressWarnings("unchecked")
        TypeDescriptor<OutputT> descriptor =
            (TypeDescriptor<OutputT>) superDescriptor.resolveType(typeVariable);
        try {
          outputCoder = input.getPipeline().getCoderRegistry().getCoder(descriptor);
        } catch (CannotProvideCoderException e) {
          throw new RuntimeException(
              "Unable to infer coder for OutputT. Specify it explicitly using withOutputCoder().");
        }
      }
      try {
        outputCoder.verifyDeterministic();
      } catch (Coder.NonDeterministicException e) {
        throw new IllegalArgumentException(
            "Output coder " + outputCoder + " must be deterministic");
      }

      return input
          .apply(ParDo.of(new WatchGrowthFn<>(this, outputCoder)))
          .setCoder(KvCoder.of(input.getCoder(), outputCoder));
    }
  }

  private static class WatchGrowthFn<InputT, OutputT, TerminationStateT>
      extends DoFn<InputT, KV<InputT, OutputT>> {
    private final Watch.Growth<InputT, OutputT> spec;
    private final Coder<OutputT> outputCoder;

    private WatchGrowthFn(Growth<InputT, OutputT> spec, Coder<OutputT> outputCoder) {
      this.spec = spec;
      this.outputCoder = outputCoder;
    }

    @ProcessElement
    public ProcessContinuation process(
        ProcessContext c, final GrowthTracker<OutputT, TerminationStateT> tracker)
        throws Exception {
      if (!tracker.hasPending() && !tracker.currentRestriction().isOutputComplete) {
        LOG.debug("{} - polling input", c.element());
        Growth.PollResult<OutputT> res = spec.getPollFn().apply(c.element(), c.timestamp());
        // TODO (https://issues.apache.org/jira/browse/BEAM-2680):
        // Consider truncating the pending outputs if there are too many, to avoid blowing
        // up the state. In that case, we'd rely on the next poll cycle to provide more outputs.
        // All outputs would still have to be stored in state.completed, but it is more compact
        // because it stores hashes and because it could potentially be garbage-collected.
        int numPending = tracker.addNewAsPending(res);
        if (numPending > 0) {
          LOG.info(
              "{} - polling returned {} results, of which {} were new. The output is {}.",
              c.element(),
              res.getOutputs().size(),
              numPending,
              BoundedWindow.TIMESTAMP_MAX_VALUE.equals(res.getWatermark())
                  ? "complete"
                  : "incomplete");
        }
      }
      while (tracker.hasPending()) {
        c.updateWatermark(tracker.getWatermark());

        TimestampedValue<OutputT> nextPending = tracker.tryClaimNextPending();
        if (nextPending == null) {
          return stop();
        }
        c.outputWithTimestamp(
            KV.of(c.element(), nextPending.getValue()), nextPending.getTimestamp());
      }
      Instant watermark = tracker.getWatermark();
      if (watermark != null) {
        // Null means the poll result did not provide a watermark and there were no new elements,
        // so we have no information to update the watermark and should keep it as-is.
        c.updateWatermark(watermark);
      }
      // No more pending outputs - future output will come from more polling,
      // unless output is complete or termination condition is reached.
      if (tracker.shouldPollMore()) {
        return resume().withResumeDelay(spec.getPollInterval());
      }
      return stop();
    }

    private Growth.TerminationCondition<InputT, TerminationStateT> getTerminationCondition() {
      return ((Growth.TerminationCondition<InputT, TerminationStateT>)
          spec.getTerminationPerInput());
    }

    @GetInitialRestriction
    public GrowthState<OutputT, TerminationStateT> getInitialRestriction(InputT element) {
      return new GrowthState<>(getTerminationCondition().forNewInput(Instant.now(), element));
    }

    @NewTracker
    public GrowthTracker<OutputT, TerminationStateT> newTracker(
        GrowthState<OutputT, TerminationStateT> restriction) {
      return new GrowthTracker<>(outputCoder, restriction, getTerminationCondition());
    }

    @GetRestrictionCoder
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Coder<GrowthState<OutputT, TerminationStateT>> getRestrictionCoder() {
      return GrowthStateCoder.of(
          outputCoder, (Coder) spec.getTerminationPerInput().getStateCoder());
    }
  }

  @VisibleForTesting
  static class GrowthState<OutputT, TerminationStateT> {
    // Hashes and timestamps of outputs that have already been output and should be omitted
    // from future polls. Timestamps are preserved to allow garbage-collecting this state
    // in the future, e.g. dropping elements from "completed" and from addNewAsPending() if their
    // timestamp is more than X behind the watermark.
    // As of writing, we don't do this, but preserve the information for forward compatibility
    // in case of pipeline update. TODO: do this.
    private final Map<HashCode, Instant> completed;
    // Outputs that are known to be present in a poll result, but have not yet been returned
    // from a ProcessElement call, sorted by timestamp to help smooth watermark progress.
    private final List<TimestampedValue<OutputT>> pending;
    // If true, processing of this restriction should only output "pending". Otherwise, it should
    // also continue polling.
    private final boolean isOutputComplete;
    // Can be null only if isOutputComplete is true.
    @Nullable private final TerminationStateT terminationState;
    // A lower bound on timestamps of future outputs from PollFn, excluding completed and pending.
    @Nullable private final Instant pollWatermark;

    GrowthState(TerminationStateT terminationState) {
      this.completed = Collections.emptyMap();
      this.pending = Collections.emptyList();
      this.isOutputComplete = false;
      this.terminationState = checkNotNull(terminationState);
      this.pollWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    GrowthState(
        Map<HashCode, Instant> completed,
        List<TimestampedValue<OutputT>> pending,
        boolean isOutputComplete,
        @Nullable TerminationStateT terminationState,
        @Nullable Instant pollWatermark) {
      if (!isOutputComplete) {
        checkNotNull(terminationState);
      }
      this.completed = Collections.unmodifiableMap(completed);
      this.pending = Collections.unmodifiableList(pending);
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
          + (pending.isEmpty() ? "" : (", earliest " + pending.get(0)))
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
  static class GrowthTracker<OutputT, TerminationStateT>
      implements RestrictionTracker<GrowthState<OutputT, TerminationStateT>> {
    private final Funnel<OutputT> coderFunnel;
    private final Growth.TerminationCondition<?, TerminationStateT> terminationCondition;

    // The restriction describing the entire work to be done by the current ProcessElement call.
    // Changes only in checkpoint().
    private GrowthState<OutputT, TerminationStateT> state;

    // Mutable state changed by the ProcessElement call itself, and used to compute the primary
    // and residual restrictions in checkpoint().

    // Remaining pending outputs; initialized from state.pending (if non-empty) or in
    // addNewAsPending(); drained via tryClaimNextPending().
    private LinkedList<TimestampedValue<OutputT>> pending;
    // Outputs that have been claimed in the current ProcessElement call. A prefix of "pending".
    private List<TimestampedValue<OutputT>> claimed = Lists.newArrayList();
    private boolean isOutputComplete;
    private TerminationStateT terminationState;
    @Nullable private Instant pollWatermark;
    private boolean shouldStop = false;

    GrowthTracker(final Coder<OutputT> outputCoder, GrowthState<OutputT, TerminationStateT> state,
                  Growth.TerminationCondition<?, TerminationStateT> terminationCondition) {
      this.coderFunnel =
          new Funnel<OutputT>() {
            @Override
            public void funnel(OutputT from, PrimitiveSink into) {
              try {
                outputCoder.encode(from, Funnels.asOutputStream(into));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          };
      this.terminationCondition = terminationCondition;
      this.state = state;
      this.isOutputComplete = state.isOutputComplete;
      this.pollWatermark = state.pollWatermark;
      this.terminationState = state.terminationState;
      this.pending = Lists.newLinkedList(state.pending);
    }

    @Override
    public synchronized GrowthState<OutputT, TerminationStateT> currentRestriction() {
      return state;
    }

    @Override
    public synchronized GrowthState<OutputT, TerminationStateT> checkpoint() {
      // primary should contain exactly the work claimed in the current ProcessElement call - i.e.
      // claimed outputs become pending, and it shouldn't poll again.
      GrowthState<OutputT, TerminationStateT> primary =
          new GrowthState<>(
              state.completed /* completed */,
              claimed /* pending */,
              true /* isOutputComplete */,
              null /* terminationState */,
              BoundedWindow.TIMESTAMP_MAX_VALUE /* pollWatermark */);

      // residual should contain exactly the work *not* claimed in the current ProcessElement call -
      // unclaimed pending outputs plus future polling outputs.
      Map<HashCode, Instant> newCompleted = Maps.newHashMap(state.completed);
      for (TimestampedValue<OutputT> claimedOutput : claimed) {
        newCompleted.put(hash128(claimedOutput.getValue()), claimedOutput.getTimestamp());
      }
      GrowthState<OutputT, TerminationStateT> residual =
          new GrowthState<>(
              newCompleted /* completed */,
              pending /* pending */,
              isOutputComplete /* isOutputComplete */,
              terminationState,
              pollWatermark);

      // Morph ourselves into primary, except for "pending" - the current call has already claimed
      // everything from it.
      this.state = primary;
      this.isOutputComplete = primary.isOutputComplete;
      this.pollWatermark = primary.pollWatermark;
      this.terminationState = null;
      this.pending = Lists.newLinkedList();

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

    @VisibleForTesting
    @Nullable
    synchronized TimestampedValue<OutputT> tryClaimNextPending() {
      if (shouldStop) {
        return null;
      }
      checkState(!pending.isEmpty(), "No more unclaimed pending outputs");
      TimestampedValue<OutputT> value = pending.removeFirst();
      claimed.add(value);
      return value;
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
      List<TimestampedValue<OutputT>> newPending = Lists.newArrayList();
      for (TimestampedValue<OutputT> output : pollResult.getOutputs()) {
        OutputT value = output.getValue();
        if (state.completed.containsKey(hash128(value))) {
          continue;
        }
        // TODO (https://issues.apache.org/jira/browse/BEAM-2680):
        // Consider adding only at most N pending elements and ignoring others,
        // instead relying on future poll rounds to provide them, in order to avoid
        // blowing up the state. Combined with garbage collection of GrowthState.completed,
        // this would make the transform scalable to very large poll results.
        newPending.add(TimestampedValue.of(value, output.getTimestamp()));
      }
      if (!newPending.isEmpty()) {
        terminationState = terminationCondition.onSeenNewOutput(Instant.now(), terminationState);
      }
      this.pending =
          Lists.newLinkedList(
              Ordering.natural()
                  .onResultOf(
                      new Function<TimestampedValue<OutputT>, Instant>() {
                        @Override
                        public Instant apply(TimestampedValue<OutputT> output) {
                          return output.getTimestamp();
                        }
                      })
                  .sortedCopy(newPending));
      // If poll result doesn't provide a watermark, assume that future new outputs may
      // arrive with about the same timestamps as the current new outputs.
      if (pollResult.getWatermark() != null) {
        this.pollWatermark = pollResult.getWatermark();
      } else if (!pending.isEmpty()) {
        this.pollWatermark = pending.getFirst().getTimestamp();
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
          .min(pollWatermark, pending.isEmpty() ? null : pending.getFirst().getTimestamp());
    }

    @Override
    public synchronized String toString() {
      return "GrowthTracker{"
          + "state="
          + state.toString(terminationCondition)
          + ", pending=<"
          + pending.size()
          + " elements"
          + (pending.isEmpty() ? "" : (", earliest " + pending.get(0)))
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

  private static class GrowthStateCoder<OutputT, TerminationStateT>
      extends StructuredCoder<GrowthState<OutputT, TerminationStateT>> {
    public static <OutputT, TerminationStateT> GrowthStateCoder<OutputT, TerminationStateT> of(
        Coder<OutputT> outputCoder, Coder<TerminationStateT> terminationStateCoder) {
      return new GrowthStateCoder<>(outputCoder, terminationStateCoder);
    }

    private static final Coder<Boolean> BOOLEAN_CODER = BooleanCoder.of();
    private static final Coder<Instant> INSTANT_CODER = NullableCoder.of(InstantCoder.of());
    private static final Coder<HashCode> HASH_CODE_CODER = HashCode128Coder.of();

    private final Coder<OutputT> outputCoder;
    private final Coder<Map<HashCode, Instant>> completedCoder;
    private final Coder<List<TimestampedValue<OutputT>>> pendingCoder;
    private final Coder<TerminationStateT> terminationStateCoder;

    private GrowthStateCoder(
        Coder<OutputT> outputCoder, Coder<TerminationStateT> terminationStateCoder) {
      this.outputCoder = outputCoder;
      this.terminationStateCoder = terminationStateCoder;
      this.completedCoder = MapCoder.of(HASH_CODE_CODER, INSTANT_CODER);
      this.pendingCoder = ListCoder.of(TimestampedValue.TimestampedValueCoder.of(outputCoder));
    }

    @Override
    public void encode(GrowthState<OutputT, TerminationStateT> value, OutputStream os)
        throws IOException {
      completedCoder.encode(value.completed, os);
      pendingCoder.encode(value.pending, os);
      BOOLEAN_CODER.encode(value.isOutputComplete, os);
      terminationStateCoder.encode(value.terminationState, os);
      INSTANT_CODER.encode(value.pollWatermark, os);
    }

    @Override
    public GrowthState<OutputT, TerminationStateT> decode(InputStream is) throws IOException {
      Map<HashCode, Instant> completed = completedCoder.decode(is);
      List<TimestampedValue<OutputT>> pending = pendingCoder.decode(is);
      boolean isOutputComplete = BOOLEAN_CODER.decode(is);
      TerminationStateT terminationState = terminationStateCoder.decode(is);
      Instant pollWatermark = INSTANT_CODER.decode(is);
      return new GrowthState<>(
          completed, pending, isOutputComplete, terminationState, pollWatermark);
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
