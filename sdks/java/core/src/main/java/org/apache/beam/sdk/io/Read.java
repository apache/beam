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
package org.apache.beam.sdk.io;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark.NoopCheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Deduplicate;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.HasProgress;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.NameUtils;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.beam.sdk.values.ValueWithRecordId.StripIdsDoFn;
import org.apache.beam.sdk.values.ValueWithRecordId.ValueWithRecordIdCoder;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} for reading from a {@link Source}.
 *
 * <p>Usage example:
 *
 * <pre>
 * Pipeline p = Pipeline.create();
 * p.apply(Read.from(new MySource().withFoo("foo").withBar("bar")));
 * </pre>
 */
public class Read {

  /**
   * Returns a new {@code Read.Bounded} {@code PTransform} reading from the given {@code
   * BoundedSource}.
   */
  public static <T> Bounded<T> from(BoundedSource<T> source) {
    return new Bounded<>(null, source);
  }

  /**
   * Returns a new {@link Read.Unbounded} {@link PTransform} reading from the given {@link
   * UnboundedSource}.
   */
  public static <T> Unbounded<T> from(UnboundedSource<T, ?> source) {
    return new Unbounded<>(null, source);
  }

  /** Helper class for building {@link Read} transforms. */
  public static class Builder {
    private final String name;

    private Builder(String name) {
      this.name = name;
    }

    /**
     * Returns a new {@code Read.Bounded} {@code PTransform} reading from the given {@code
     * BoundedSource}.
     */
    public <T> Bounded<T> from(BoundedSource<T> source) {
      return new Bounded<>(name, source);
    }

    /**
     * Returns a new {@code Read.Unbounded} {@code PTransform} reading from the given {@code
     * UnboundedSource}.
     */
    public <T> Unbounded<T> from(UnboundedSource<T, ?> source) {
      return new Unbounded<>(name, source);
    }
  }

  /** {@link PTransform} that reads from a {@link BoundedSource}. */
  public static class Bounded<T> extends PTransform<PBegin, PCollection<T>> {
    private final BoundedSource<T> source;

    private Bounded(@Nullable String name, BoundedSource<T> source) {
      super(name);
      this.source = SerializableUtils.ensureSerializable(source);
    }

    @Override
    public final PCollection<T> expand(PBegin input) {
      source.validate();

      if (useSdf(input.getPipeline().getOptions())) {
        // We don't use Create here since Create is defined as a BoundedSource and using it would
        // cause an infinite expansion loop. We can reconsider this if Create is implemented
        // directly as a SplittableDoFn.
        return input
            .getPipeline()
            .apply(Impulse.create())
            .apply(
                MapElements.into(new TypeDescriptor<BoundedSource<T>>() {}).via(element -> source))
            .setCoder(SerializableCoder.of(new TypeDescriptor<BoundedSource<T>>() {}))
            .apply(ParDo.of(new BoundedSourceAsSDFWrapperFn<>()))
            .setCoder(source.getOutputCoder());
      }

      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(),
          WindowingStrategy.globalDefault(),
          PCollection.IsBounded.BOUNDED,
          source.getOutputCoder());
    }

    /** Returns the {@code BoundedSource} used to create this {@code Read} {@code PTransform}. */
    public BoundedSource<T> getSource() {
      return source;
    }

    @Override
    public String getKindString() {
      return String.format("Read(%s)", NameUtils.approximateSimpleName(source));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(DisplayData.item("source", source.getClass()).withLabel("Read Source"))
          .include("source", source);
    }
  }

  /** {@link PTransform} that reads from a {@link UnboundedSource}. */
  public static class Unbounded<T> extends PTransform<PBegin, PCollection<T>> {
    private final UnboundedSource<T, ?> source;

    private Unbounded(@Nullable String name, UnboundedSource<T, ?> source) {
      super(name);
      this.source = SerializableUtils.ensureSerializable(source);
    }

    /**
     * Returns a new {@link BoundedReadFromUnboundedSource} that reads a bounded amount of data from
     * the given {@link UnboundedSource}. The bound is specified as a number of records to read.
     *
     * <p>This may take a long time to execute if the splits of this source are slow to read
     * records.
     */
    public BoundedReadFromUnboundedSource<T> withMaxNumRecords(long maxNumRecords) {
      return new BoundedReadFromUnboundedSource<>(source, maxNumRecords, null);
    }

    /**
     * Returns a new {@link BoundedReadFromUnboundedSource} that reads a bounded amount of data from
     * the given {@link UnboundedSource}. The bound is specified as an amount of time to read for.
     * Each split of the source will read for this much time.
     */
    public BoundedReadFromUnboundedSource<T> withMaxReadTime(Duration maxReadTime) {
      return new BoundedReadFromUnboundedSource<>(source, Long.MAX_VALUE, maxReadTime);
    }

    @Override
    public final PCollection<T> expand(PBegin input) {
      source.validate();

      if (useSdf(input.getPipeline().getOptions())) {
        // We don't use Create here since Create is defined as a BoundedSource and using it would
        // cause an infinite expansion loop. We can reconsider this if Create is implemented
        // directly as a SplittableDoFn.
        PCollection<ValueWithRecordId<T>> outputWithIds =
            input
                .getPipeline()
                .apply(Impulse.create())
                .apply(
                    MapElements.into(new TypeDescriptor<UnboundedSource<T, CheckpointMark>>() {})
                        .via(element -> (UnboundedSource<T, CheckpointMark>) source))
                .setCoder(
                    SerializableCoder.of(
                        new TypeDescriptor<UnboundedSource<T, CheckpointMark>>() {}))
                .apply(
                    ParDo.of(
                        new UnboundedSourceAsSDFWrapperFn<>(
                            (Coder<CheckpointMark>) source.getCheckpointMarkCoder())))
                .setCoder(ValueWithRecordIdCoder.of(source.getOutputCoder()));

        if (source.requiresDeduping()) {
          outputWithIds.apply(
              Deduplicate.<ValueWithRecordId<T>, byte[]>withRepresentativeValueFn(
                      element -> element.getId())
                  .withRepresentativeType(TypeDescriptor.of(byte[].class)));
        }
        return outputWithIds.apply(ParDo.of(new StripIdsDoFn<>()));
      }

      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(),
          WindowingStrategy.globalDefault(),
          PCollection.IsBounded.UNBOUNDED,
          source.getOutputCoder());
    }

    /** Returns the {@code UnboundedSource} used to create this {@code Read} {@code PTransform}. */
    public UnboundedSource<T, ?> getSource() {
      return source;
    }

    @Override
    public String getKindString() {
      return String.format("Read(%s)", NameUtils.approximateSimpleName(source));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      builder
          .add(DisplayData.item("source", source.getClass()).withLabel("Read Source"))
          .include("source", source);
    }
  }

  /**
   * A splittable {@link DoFn} which executes a {@link BoundedSource}.
   *
   * <p>We model the element as the original source and the restriction as the sub-source. This
   * allows us to split the sub-source over and over yet still receive "source" objects as inputs.
   */
  static class BoundedSourceAsSDFWrapperFn<T> extends DoFn<BoundedSource<T>, T> {
    private static final Logger LOG = LoggerFactory.getLogger(BoundedSourceAsSDFWrapperFn.class);
    private static final long DEFAULT_DESIRED_BUNDLE_SIZE_BYTES = 64 * (1 << 20);

    @GetInitialRestriction
    public BoundedSource<T> initialRestriction(@Element BoundedSource<T> element) {
      return element;
    }

    @GetSize
    public double getSize(
        @Restriction BoundedSource<T> restriction, PipelineOptions pipelineOptions)
        throws Exception {
      return restriction.getEstimatedSizeBytes(pipelineOptions);
    }

    @SplitRestriction
    public void splitRestriction(
        @Restriction BoundedSource<T> restriction,
        OutputReceiver<BoundedSource<T>> receiver,
        PipelineOptions pipelineOptions)
        throws Exception {
      long estimatedSize = restriction.getEstimatedSizeBytes(pipelineOptions);
      // Split into pieces as close to the default desired bundle size but if that would cause too
      // few splits then prefer to split up to the default desired number of splits.
      long splitBundleSize =
          Math.min(
              DEFAULT_DESIRED_BUNDLE_SIZE_BYTES,
              Math.max(1L, estimatedSize / DEFAULT_DESIRED_NUM_SPLITS));
      for (BoundedSource<T> split : restriction.split(splitBundleSize, pipelineOptions)) {
        receiver.output(split);
      }
    }

    @NewTracker
    public RestrictionTracker<BoundedSource<T>, TimestampedValue<T>[]> restrictionTracker(
        @Restriction BoundedSource<T> restriction, PipelineOptions pipelineOptions) {
      return new BoundedSourceAsSDFRestrictionTracker<>(restriction, pipelineOptions);
    }

    @ProcessElement
    public void processElement(
        RestrictionTracker<BoundedSource<T>, TimestampedValue<T>[]> tracker,
        OutputReceiver<T> receiver)
        throws IOException {
      TimestampedValue<T>[] out = new TimestampedValue[1];
      while (tracker.tryClaim(out)) {
        receiver.outputWithTimestamp(out[0].getValue(), out[0].getTimestamp());
      }
    }

    @GetRestrictionCoder
    public Coder<BoundedSource<T>> restrictionCoder() {
      return SerializableCoder.of(new TypeDescriptor<BoundedSource<T>>() {});
    }

    /**
     * A fake restriction tracker which adapts to the {@link BoundedSource} API. The restriction
     * object is used to advance the underlying source and to "return" the current element.
     */
    private static class BoundedSourceAsSDFRestrictionTracker<T>
        extends RestrictionTracker<BoundedSource<T>, TimestampedValue<T>[]> {
      private final BoundedSource<T> initialRestriction;
      private final PipelineOptions pipelineOptions;
      private BoundedSource.BoundedReader<T> currentReader;
      private boolean claimedAll;

      BoundedSourceAsSDFRestrictionTracker(
          BoundedSource<T> initialRestriction, PipelineOptions pipelineOptions) {
        this.initialRestriction = initialRestriction;
        this.pipelineOptions = pipelineOptions;
      }

      @Override
      public boolean tryClaim(TimestampedValue<T>[] position) {
        if (claimedAll) {
          return false;
        }
        try {
          if (currentReader == null) {
            currentReader = initialRestriction.createReader(pipelineOptions);
            if (!currentReader.start()) {
              claimedAll = true;
              try {
                currentReader.close();
              } finally {
                currentReader = null;
              }
              return false;
            }
            position[0] =
                TimestampedValue.of(
                    currentReader.getCurrent(), currentReader.getCurrentTimestamp());
            return true;
          }
          if (!currentReader.advance()) {
            claimedAll = true;
            try {
              currentReader.close();
            } finally {
              currentReader = null;
            }
            return false;
          }
          position[0] =
              TimestampedValue.of(currentReader.getCurrent(), currentReader.getCurrentTimestamp());
          return true;
        } catch (IOException e) {
          if (currentReader != null) {
            try {
              currentReader.close();
            } catch (IOException closeException) {
              e.addSuppressed(closeException);
            } finally {
              currentReader = null;
            }
          }
          throw new RuntimeException(e);
        }
      }

      @Override
      protected void finalize() throws Throwable {
        if (currentReader != null) {
          try {
            currentReader.close();
          } catch (IOException e) {
            LOG.error("Failed to close BoundedReader due to failure processing bundle.", e);
          }
        }
      }

      /** The value is invalid if {@link #tryClaim} has ever thrown an exception. */
      @Override
      public BoundedSource<T> currentRestriction() {
        if (currentReader == null) {
          return initialRestriction;
        }
        return currentReader.getCurrentSource();
      }

      @Override
      public SplitResult<BoundedSource<T>> trySplit(double fractionOfRemainder) {
        if (currentReader == null) {
          return null;
        }
        Double consumedFraction = currentReader.getFractionConsumed();
        double fraction = fractionOfRemainder;
        if (consumedFraction != null) {
          fraction = consumedFraction + (1 - consumedFraction) * fractionOfRemainder;
        }

        BoundedSource<T> residual = currentReader.splitAtFraction(fraction);
        if (residual == null) {
          return null;
        }
        BoundedSource<T> primary = currentReader.getCurrentSource();
        return SplitResult.of(primary, residual);
      }

      @Override
      public void checkDone() throws IllegalStateException {
        checkState(
            claimedAll,
            "Expected all records to have been claimed but finished processing "
                + "bounded source while some records may have not been read.");
      }

      @Override
      public IsBounded isBounded() {
        return IsBounded.BOUNDED;
      }
    }
  }

  /**
   * A splittable {@link DoFn} which executes an {@link UnboundedSource}.
   *
   * <p>We model the element as the original source and the restriction as a pair of the sub-source
   * and its {@link CheckpointMark}. This allows us to split the sub-source over and over as long as
   * the checkpoint mark is {@code null} or the {@link NoopCheckpointMark} since it does not
   * maintain any state.
   */
  @UnboundedPerElement
  static class UnboundedSourceAsSDFWrapperFn<OutputT, CheckpointT extends CheckpointMark>
      extends DoFn<UnboundedSource<OutputT, CheckpointT>, ValueWithRecordId<OutputT>> {

    private static final Logger LOG = LoggerFactory.getLogger(UnboundedSourceAsSDFWrapperFn.class);
    private static final int DEFAULT_BUNDLE_FINALIZATION_LIMIT_MINS = 10;
    private final Coder<CheckpointT> checkpointCoder;

    private UnboundedSourceAsSDFWrapperFn(Coder<CheckpointT> checkpointCoder) {
      this.checkpointCoder = checkpointCoder;
    }

    @GetInitialRestriction
    public UnboundedSourceRestriction<OutputT, CheckpointT> initialRestriction(
        @Element UnboundedSource<OutputT, CheckpointT> element) {
      return UnboundedSourceRestriction.create(element, null, BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    @SplitRestriction
    public void splitRestriction(
        @Restriction UnboundedSourceRestriction<OutputT, CheckpointT> restriction,
        OutputReceiver<UnboundedSourceRestriction<OutputT, CheckpointT>> receiver,
        PipelineOptions pipelineOptions)
        throws Exception {
      // The empty unbounded source is trivially done and hence we don't need to output any splits
      // for it.
      if (restriction.getSource() instanceof EmptyUnboundedSource) {
        return;
      }

      // The UnboundedSource API does not support splitting after a meaningful checkpoint mark has
      // been created.
      if (restriction.getCheckpoint() != null
          && !(restriction.getCheckpoint()
              instanceof UnboundedSource.CheckpointMark.NoopCheckpointMark)) {
        receiver.output(restriction);
      }

      try {
        for (UnboundedSource<OutputT, CheckpointT> split :
            restriction.getSource().split(DEFAULT_DESIRED_NUM_SPLITS, pipelineOptions)) {
          receiver.output(
              UnboundedSourceRestriction.create(split, null, restriction.getWatermark()));
        }
      } catch (Exception e) {
        receiver.output(restriction);
      }
    }

    @NewTracker
    public RestrictionTracker<
            UnboundedSourceRestriction<OutputT, CheckpointT>, UnboundedSourceValue<OutputT>[]>
        restrictionTracker(
            @Restriction UnboundedSourceRestriction<OutputT, CheckpointT> restriction,
            PipelineOptions pipelineOptions) {
      return new UnboundedSourceAsSDFRestrictionTracker(restriction, pipelineOptions);
    }

    @ProcessElement
    public ProcessContinuation processElement(
        RestrictionTracker<UnboundedSourceRestriction<OutputT, CheckpointT>, UnboundedSourceValue[]>
            tracker,
        ManualWatermarkEstimator<Instant> watermarkEstimator,
        OutputReceiver<ValueWithRecordId<OutputT>> receiver,
        BundleFinalizer bundleFinalizer)
        throws IOException {
      UnboundedSourceRestriction<OutputT, CheckpointT> initialRestriction =
          tracker.currentRestriction();

      UnboundedSourceValue<OutputT>[] out = new UnboundedSourceValue[1];
      while (tracker.tryClaim(out) && out[0] != null) {
        receiver.outputWithTimestamp(
            new ValueWithRecordId<>(out[0].getValue(), out[0].getId()), out[0].getTimestamp());
      }

      UnboundedSourceRestriction<OutputT, CheckpointT> currentRestriction =
          tracker.currentRestriction();

      // Advance the watermark even if zero elements may have been output.
      watermarkEstimator.setWatermark(
          ensureTimestampWithinBounds(currentRestriction.getWatermark()));

      // Add the checkpoint mark to be finalized if the checkpoint mark isn't trivial and is not
      // the initial restriction. The initial restriction would have been finalized as part of
      // a prior bundle being executed.
      @SuppressWarnings("ReferenceEquality")
      boolean isInitialRestriction = initialRestriction == currentRestriction;
      if (currentRestriction.getCheckpoint() != null
          && !isInitialRestriction
          && !(tracker.currentRestriction().getCheckpoint() instanceof NoopCheckpointMark)) {
        bundleFinalizer.afterBundleCommit(
            Instant.now().plus(Duration.standardMinutes(DEFAULT_BUNDLE_FINALIZATION_LIMIT_MINS)),
            currentRestriction.getCheckpoint()::finalizeCheckpoint);
      }

      // If we have been split/checkpoint by a runner, the tracker will have been updated to the
      // empty source and we will return stop. Otherwise the unbounded source has only temporarily
      // run out of work.
      if (currentRestriction.getSource() instanceof EmptyUnboundedSource) {
        return ProcessContinuation.stop();
      }
      return ProcessContinuation.resume();
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
      return currentElementTimestamp;
    }

    private Instant ensureTimestampWithinBounds(Instant timestamp) {
      if (timestamp.isBefore(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
        timestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
      } else if (timestamp.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
        timestamp = BoundedWindow.TIMESTAMP_MAX_VALUE;
      }
      return timestamp;
    }

    @NewWatermarkEstimator
    public WatermarkEstimators.Manual newWatermarkEstimator(
        @WatermarkEstimatorState Instant watermarkEstimatorState) {
      return new WatermarkEstimators.Manual(ensureTimestampWithinBounds(watermarkEstimatorState));
    }

    @GetRestrictionCoder
    public Coder<UnboundedSourceRestriction<OutputT, CheckpointT>> restrictionCoder() {
      return new UnboundedSourceRestrictionCoder<>(
          SerializableCoder.of(new TypeDescriptor<UnboundedSource<OutputT, CheckpointT>>() {}),
          NullableCoder.of(checkpointCoder));
    }

    /**
     * A POJO representing all the values we need to pass between the {@link UnboundedReader} and
     * the {@link org.apache.beam.sdk.transforms.DoFn.ProcessElement @ProcessElement} method of the
     * splittable DoFn for each output element.
     */
    @AutoValue
    abstract static class UnboundedSourceValue<T> {

      public static <T> UnboundedSourceValue<T> create(byte[] id, T value, Instant timestamp) {
        return new AutoValue_Read_UnboundedSourceAsSDFWrapperFn_UnboundedSourceValue<T>(
            id, value, timestamp);
      }

      @SuppressWarnings("mutable")
      public abstract byte[] getId();

      public abstract T getValue();

      public abstract Instant getTimestamp();
    }

    /**
     * A POJO representing all the state we need to maintain between the {@link UnboundedReader} and
     * future {@link org.apache.beam.sdk.transforms.DoFn.ProcessElement @ProcessElement} calls.
     */
    @AutoValue
    abstract static class UnboundedSourceRestriction<OutputT, CheckpointT extends CheckpointMark>
        implements Serializable {
      public static <OutputT, CheckpointT extends CheckpointMark>
          UnboundedSourceRestriction<OutputT, CheckpointT> create(
              UnboundedSource<OutputT, CheckpointT> source,
              CheckpointT checkpoint,
              Instant watermark) {
        return new AutoValue_Read_UnboundedSourceAsSDFWrapperFn_UnboundedSourceRestriction<>(
            source, checkpoint, watermark);
      }

      public abstract UnboundedSource<OutputT, CheckpointT> getSource();

      public abstract @Nullable CheckpointT getCheckpoint();

      public abstract Instant getWatermark();
    }

    /** A {@link Coder} for {@link UnboundedSourceRestriction}s. */
    private static class UnboundedSourceRestrictionCoder<
            OutputT, CheckpointT extends CheckpointMark>
        extends StructuredCoder<UnboundedSourceRestriction<OutputT, CheckpointT>> {

      private final Coder<UnboundedSource<OutputT, CheckpointT>> sourceCoder;
      private final Coder<CheckpointT> checkpointCoder;

      private UnboundedSourceRestrictionCoder(
          Coder<UnboundedSource<OutputT, CheckpointT>> sourceCoder,
          Coder<CheckpointT> checkpointCoder) {
        this.sourceCoder = sourceCoder;
        this.checkpointCoder = checkpointCoder;
      }

      @Override
      public void encode(
          UnboundedSourceRestriction<OutputT, CheckpointT> value, OutputStream outStream)
          throws CoderException, IOException {
        sourceCoder.encode(value.getSource(), outStream);
        checkpointCoder.encode(value.getCheckpoint(), outStream);
        InstantCoder.of().encode(value.getWatermark(), outStream);
      }

      @Override
      public UnboundedSourceRestriction<OutputT, CheckpointT> decode(InputStream inStream)
          throws CoderException, IOException {
        return UnboundedSourceRestriction.create(
            sourceCoder.decode(inStream),
            checkpointCoder.decode(inStream),
            InstantCoder.of().decode(inStream));
      }

      @Override
      public List<? extends Coder<?>> getCoderArguments() {
        return Arrays.asList(sourceCoder, checkpointCoder);
      }

      @Override
      public void verifyDeterministic() throws NonDeterministicException {
        verifyDeterministic(sourceCoder, "source coder not deterministic");
        verifyDeterministic(checkpointCoder, "checkpoint coder not deterministic");
        verifyDeterministic(InstantCoder.of(), "watermark coder not deterministic");
      }
    }

    /**
     * A marker implementation that is used to represent the primary "source" when performing a
     * split. The methods on this object are not meant to be called and only exist to fulfill the
     * {@link UnboundedSource} API contract.
     */
    private static class EmptyUnboundedSource<OutputT, CheckpointT extends CheckpointMark>
        extends UnboundedSource<OutputT, CheckpointT> {

      private static final EmptyUnboundedSource INSTANCE = new EmptyUnboundedSource();

      @Override
      public List<? extends UnboundedSource<OutputT, CheckpointT>> split(
          int desiredNumSplits, PipelineOptions options) throws Exception {
        throw new UnsupportedOperationException("split is never meant to be invoked.");
      }

      @Override
      public UnboundedReader<OutputT> createReader(
          PipelineOptions options, @Nullable CheckpointT checkpointMark) {
        return this.new EmptyUnboundedReader(checkpointMark);
      }

      @Override
      public Coder<CheckpointT> getCheckpointMarkCoder() {
        throw new UnsupportedOperationException(
            "getCheckpointMarkCoder is never meant to be invoked.");
      }

      private class EmptyUnboundedReader extends UnboundedReader<OutputT> {
        private final @Nullable CheckpointT checkpointMark;

        private EmptyUnboundedReader(@Nullable CheckpointT checkpointMark) {
          this.checkpointMark = checkpointMark;
        }

        @Override
        public boolean start() throws IOException {
          return false;
        }

        @Override
        public boolean advance() throws IOException {
          return false;
        }

        @Override
        public OutputT getCurrent() throws NoSuchElementException {
          throw new UnsupportedOperationException("getCurrent is never meant to be invoked.");
        }

        @Override
        public Instant getCurrentTimestamp() throws NoSuchElementException {
          throw new UnsupportedOperationException(
              "getCurrentTimestamp is never meant to be invoked.");
        }

        @Override
        public void close() throws IOException {}

        @Override
        public Instant getWatermark() {
          return BoundedWindow.TIMESTAMP_MAX_VALUE;
        }

        @Override
        public CheckpointMark getCheckpointMark() {
          return checkpointMark;
        }

        @Override
        public UnboundedSource<OutputT, ?> getCurrentSource() {
          return EmptyUnboundedSource.INSTANCE;
        }
      }
    }

    /**
     * A fake restriction tracker which adapts to the {@link UnboundedSource} API. The restriction
     * object is used to advance the underlying source and to "return" the current element.
     *
     * <p>In an {@link UnboundedReader}, both {@link UnboundedReader#start} and {@link
     * UnboundedReader#advance} will return false when there is no data to process right now so this
     * restriction tracker only tracks the "known" amount of outstanding work.
     *
     * <p>In an {@link UnboundedSource}, the {@link CheckpointMark} represents both any work that
     * should be done when "finalizing" the bundle and also any state information that is used to
     * resume the next portion of processing. We use the {@link #currentRestriction} to return any
     * checkpointing information. Typically accessing the {@link #currentRestriction} is meant to be
     * thread safe since the "restriction" is meant to represent a point in time view of the
     * restriction tracker but this is not possible because the {@link UnboundedSource} and {@link
     * UnboundedReader} do not provide enough visibility into their position space to be able to
     * update a meaningful restriction space so we must be careful to not mutate the current
     * restriction when splitting if we are done.
     */
    private static class UnboundedSourceAsSDFRestrictionTracker<
            OutputT, CheckpointT extends CheckpointMark>
        extends RestrictionTracker<
            UnboundedSourceRestriction<OutputT, CheckpointT>, UnboundedSourceValue<OutputT>[]>
        implements HasProgress {
      private final UnboundedSourceRestriction<OutputT, CheckpointT> initialRestriction;
      private final PipelineOptions pipelineOptions;
      private UnboundedSource.UnboundedReader<OutputT> currentReader;
      private boolean readerHasBeenStarted;

      UnboundedSourceAsSDFRestrictionTracker(
          UnboundedSourceRestriction<OutputT, CheckpointT> initialRestriction,
          PipelineOptions pipelineOptions) {
        this.initialRestriction = initialRestriction;
        this.pipelineOptions = pipelineOptions;
      }

      @Override
      public boolean tryClaim(UnboundedSourceValue<OutputT>[] position) {
        try {
          if (currentReader == null) {
            currentReader =
                initialRestriction
                    .getSource()
                    .createReader(pipelineOptions, initialRestriction.getCheckpoint());
          }
          if (currentReader instanceof EmptyUnboundedSource.EmptyUnboundedReader) {
            return false;
          }
          if (!readerHasBeenStarted) {
            readerHasBeenStarted = true;
            if (!currentReader.start()) {
              position[0] = null;
              return true;
            }
          } else if (!currentReader.advance()) {
            position[0] = null;
            return true;
          }
          position[0] =
              UnboundedSourceValue.create(
                  currentReader.getCurrentRecordId(),
                  currentReader.getCurrent(),
                  currentReader.getCurrentTimestamp());
          return true;
        } catch (IOException e) {
          if (currentReader != null) {
            try {
              currentReader.close();
            } catch (IOException closeException) {
              e.addSuppressed(closeException);
            }
          }
          throw new RuntimeException(e);
        }
      }

      @Override
      protected void finalize() throws Throwable {
        if (currentReader != null) {
          try {
            currentReader.close();
          } catch (IOException e) {
            LOG.error("Failed to close UnboundedReader due to failure processing bundle.", e);
          }
        }
      }

      /** The value is invalid if {@link #tryClaim} has ever thrown an exception. */
      @Override
      public UnboundedSourceRestriction<OutputT, CheckpointT> currentRestriction() {
        if (currentReader == null) {
          return initialRestriction;
        }
        return UnboundedSourceRestriction.create(
            (UnboundedSource<OutputT, CheckpointT>) currentReader.getCurrentSource(),
            (CheckpointT) currentReader.getCheckpointMark(),
            currentReader.getWatermark());
      }

      @Override
      public SplitResult<UnboundedSourceRestriction<OutputT, CheckpointT>> trySplit(
          double fractionOfRemainder) {
        // Don't split if we have the empty sources since the SDF wrapper will be finishing soon.
        UnboundedSourceRestriction<OutputT, CheckpointT> currentRestriction = currentRestriction();
        if (currentRestriction.getSource() instanceof EmptyUnboundedSource) {
          return null;
        }

        // Our split result sets the primary to have no checkpoint mark associated
        // with it since when we resume we don't have any state but we specifically pass
        // the checkpoint mark to the current reader so that when we finish the current bundle
        // we may register for finalization.
        SplitResult<UnboundedSourceRestriction<OutputT, CheckpointT>> result =
            SplitResult.of(
                UnboundedSourceRestriction.create(
                    EmptyUnboundedSource.INSTANCE, null, BoundedWindow.TIMESTAMP_MAX_VALUE),
                currentRestriction);
        currentReader =
            EmptyUnboundedSource.INSTANCE.createReader(null, currentRestriction.getCheckpoint());
        return result;
      }

      @Override
      public void checkDone() throws IllegalStateException {
        checkState(
            currentReader instanceof EmptyUnboundedSource.EmptyUnboundedReader,
            "Expected all records to have been claimed but finished processing "
                + "unbounded source while some records may have not been read.");
      }

      @Override
      public IsBounded isBounded() {
        return IsBounded.UNBOUNDED;
      }

      @Override
      public Progress getProgress() {
        // We treat the empty source as implicitly done.
        if (currentRestriction().getSource() instanceof EmptyUnboundedSource) {
          return RestrictionTracker.Progress.from(1, 0);
        }

        if (currentReader == null) {
          try {
            currentReader =
                initialRestriction
                    .getSource()
                    .createReader(pipelineOptions, initialRestriction.getCheckpoint());
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        long size = currentReader.getSplitBacklogBytes();
        if (size != UnboundedReader.BACKLOG_UNKNOWN) {
          // The UnboundedSource/UnboundedReader API has no way of reporting how much work
          // has been completed so runners can only see the work remaining changing.
          return RestrictionTracker.Progress.from(0, size);
        }

        // TODO: Support "global" backlog reporting
        // size = reader.getTotalBacklogBytes();
        // if (size != UnboundedReader.BACKLOG_UNKNOWN) {
        //   return size;
        // }

        // We treat unknown as 0 progress
        return RestrictionTracker.Progress.from(0, 1);
      }
    }
  }

  private static final int DEFAULT_DESIRED_NUM_SPLITS = 20;

  /**
   * Used to migrate runners to use splittable DoFn without needing to rely on PTransform
   * replacement which allows removal of the migration code without changing the pipeline shape
   * since pipeline shape affects pipeline update for some runners.
   */
  private static final Set<String> SPLITTABLE_DOFN_PREFERRED_RUNNERS =
      ImmutableSet.of("DirectRunner");

  private static boolean useSdf(PipelineOptions options) {
    // TODO(BEAM-10670): Make this by default true and have runners opt-out instead.
    boolean runnerPrefersSdf = false;
    try {
      runnerPrefersSdf =
          SPLITTABLE_DOFN_PREFERRED_RUNNERS.contains(options.getRunner().getSimpleName());
    } catch (Exception e) {
      // Ignore construction failures since there may not be a runner on the classpath if this is a
      // test.
    }

    // We keep the old names of experiments around for portable runners and existing users.
    return (runnerPrefersSdf
            || ExperimentalOptions.hasExperiment(options, "beam_fn_api")
            || ExperimentalOptions.hasExperiment(options, "use_sdf_read"))
        && !(ExperimentalOptions.hasExperiment(options, "beam_fn_api_use_deprecated_read")
            || ExperimentalOptions.hasExperiment(options, "use_deprecated_read"));
  }
}
