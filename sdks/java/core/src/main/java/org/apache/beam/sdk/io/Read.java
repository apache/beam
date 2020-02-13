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

import java.io.IOException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.util.NameUtils;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;

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

      if (ExperimentalOptions.hasExperiment(input.getPipeline().getOptions(), "beam_fn_api")
          && !ExperimentalOptions.hasExperiment(
              input.getPipeline().getOptions(), "beam_fn_api_use_deprecated_read")) {
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
          IsBounded.BOUNDED,
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
      return PCollection.createPrimitiveOutputInternal(
          input.getPipeline(),
          WindowingStrategy.globalDefault(),
          IsBounded.UNBOUNDED,
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
      for (BoundedSource<T> split :
          restriction.split(DEFAULT_DESIRED_BUNDLE_SIZE_BYTES, pipelineOptions)) {
        receiver.output(split);
      }
    }

    @NewTracker
    public RestrictionTracker<BoundedSource<T>, Object[]> restrictionTracker(
        @Restriction BoundedSource<T> restriction, PipelineOptions pipelineOptions) {
      return new BoundedSourceAsSDFRestrictionTracker<>(restriction, pipelineOptions);
    }

    @ProcessElement
    public void processElement(
        RestrictionTracker<BoundedSource<T>, Object[]> tracker, OutputReceiver<T> receiver)
        throws IOException {
      Object[] out = new Object[1];
      while (tracker.tryClaim(out)) {
        receiver.output((T) out[0]);
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
        extends RestrictionTracker<BoundedSource<T>, Object[]> {
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
      public boolean tryClaim(Object[] position) {
        if (claimedAll) {
          return false;
        }
        try {
          if (currentReader == null) {
            currentReader = initialRestriction.createReader(pipelineOptions);
            if (!currentReader.start()) {
              claimedAll = true;
              return false;
            }
            position[0] = currentReader.getCurrent();
            return true;
          }
          if (!currentReader.advance()) {
            claimedAll = true;
            return false;
          }
          position[0] = currentReader.getCurrent();
          return true;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public BoundedSource<T> currentRestriction() {
        return currentReader.getCurrentSource();
      }

      @Override
      public SplitResult<BoundedSource<T>> trySplit(double fractionOfRemainder) {
        double consumedFraction = currentReader.getFractionConsumed();
        double fraction = consumedFraction + (1 - consumedFraction) * fractionOfRemainder;
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
    }
  }
}
