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
package org.apache.beam.sdk.util.construction;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.NameUtils;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform} that converts a {@link BoundedSource} as an {@link UnboundedSource}.
 *
 * <p>{@link BoundedSource} is read directly without calling {@link BoundedSource#split}, and
 * element timestamps are propagated. While any elements remain, the watermark is the beginning of
 * time {@link BoundedWindow#TIMESTAMP_MIN_VALUE}, and after all elements have been produced the
 * watermark goes to the end of time {@link BoundedWindow#TIMESTAMP_MAX_VALUE}.
 *
 * <p>Checkpoints are created by calling {@link BoundedReader#splitAtFraction} on inner {@link
 * BoundedSource}. Sources that cannot be split are read entirely into memory, so this transform
 * does not work well with large, unsplittable sources.
 *
 * <p>This transform is intended to be used by a runner during pipeline translation to convert a
 * Read.Bounded into a Read.Unbounded.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class UnboundedReadFromBoundedSource<T> extends PTransform<PBegin, PCollection<T>> {

  private static final Logger LOG = LoggerFactory.getLogger(UnboundedReadFromBoundedSource.class);

  // Using 64MB in cases where we cannot compute a valid estimated size for a source.
  private static final long DEFAULT_ESTIMATED_SIZE = 64 * 1024 * 1024;

  private final BoundedSource<T> source;

  /**
   * Constructs a {@link PTransform} that performs an unbounded read from a {@link BoundedSource}.
   */
  public UnboundedReadFromBoundedSource(BoundedSource<T> source) {
    this.source = source;
  }

  @Override
  public PCollection<T> expand(PBegin input) {
    return input.getPipeline().apply(Read.from(new BoundedToUnboundedSourceAdapter<>(source)));
  }

  @Override
  protected Coder<T> getDefaultOutputCoder() {
    return source.getDefaultOutputCoder();
  }

  @Override
  public String getKindString() {
    return String.format("Read(%s)", NameUtils.approximateSimpleName(source));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    // We explicitly do not register base-class data, instead we use the delegate inner source.
    builder.add(DisplayData.item("source", source.getClass())).include("source", source);
  }

  /** A {@code BoundedSource} to {@code UnboundedSource} adapter. */
  @VisibleForTesting
  public static class BoundedToUnboundedSourceAdapter<T>
      extends UnboundedSource<T, BoundedToUnboundedSourceAdapter.Checkpoint<T>> {

    private BoundedSource<T> boundedSource;

    public BoundedToUnboundedSourceAdapter(BoundedSource<T> boundedSource) {
      this.boundedSource = boundedSource;
    }

    @Override
    public void validate() {
      boundedSource.validate();
    }

    @Override
    public List<BoundedToUnboundedSourceAdapter<T>> split(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      try {
        long estimatedSize = boundedSource.getEstimatedSizeBytes(options);
        if (estimatedSize <= 0) {
          // Source is unable to provide a valid estimated size. So using default size.
          LOG.warn(
              "Cannot determine a valid estimated size for BoundedSource {}. Using default "
                  + "size of {} bytes",
              boundedSource,
              DEFAULT_ESTIMATED_SIZE);
          estimatedSize = DEFAULT_ESTIMATED_SIZE;
        }

        // Each split should at least be of size 1 byte.
        long desiredBundleSize = Math.max(estimatedSize / desiredNumSplits, 1);

        List<? extends BoundedSource<T>> splits = boundedSource.split(desiredBundleSize, options);
        if (splits.size() == 0) {
          splits = ImmutableList.of(boundedSource);
        }
        return splits.stream()
            .map(input -> new BoundedToUnboundedSourceAdapter<>(input))
            .collect(Collectors.toList());
      } catch (Exception e) {
        LOG.warn("Exception while splitting {}, skips the initial splits.", boundedSource, e);
        return ImmutableList.of(this);
      }
    }

    @Override
    public Reader createReader(PipelineOptions options, Checkpoint<T> checkpoint)
        throws IOException {
      if (checkpoint == null) {
        return new Reader(null /* residualElements */, boundedSource, options);
      } else {
        return new Reader(checkpoint.residualElements, checkpoint.residualSource, options);
      }
    }

    @Override
    public Coder<T> getDefaultOutputCoder() {
      return boundedSource.getDefaultOutputCoder();
    }

    @SuppressWarnings({
      "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
      "unchecked"
    })
    @Override
    public Coder<Checkpoint<T>> getCheckpointMarkCoder() {
      return new CheckpointCoder<>(boundedSource.getDefaultOutputCoder());
    }

    /**
     * A marker representing the progress and state of an {@link BoundedToUnboundedSourceAdapter}.
     */
    @VisibleForTesting
    public static class Checkpoint<T> implements UnboundedSource.CheckpointMark {
      private final @Nullable List<TimestampedValue<T>> residualElements;
      private final @Nullable BoundedSource<T> residualSource;

      public Checkpoint(
          @Nullable List<TimestampedValue<T>> residualElements,
          @Nullable BoundedSource<T> residualSource) {
        this.residualElements = residualElements;
        this.residualSource = residualSource;
      }

      @Override
      public void finalizeCheckpoint() {}

      @VisibleForTesting
      @Nullable
      List<TimestampedValue<T>> getResidualElements() {
        return residualElements;
      }

      @VisibleForTesting
      @Nullable
      BoundedSource<T> getResidualSource() {
        return residualSource;
      }
    }

    @VisibleForTesting
    static class CheckpointCoder<T> extends StructuredCoder<Checkpoint<T>> {

      // The coder for a list of residual elements and their timestamps
      private final Coder<List<TimestampedValue<T>>> elemsCoder;
      // The coder from the BoundedReader for coding each element
      private final Coder<T> elemCoder;
      // The nullable and serializable coder for the BoundedSource.
      @SuppressWarnings("rawtypes")
      private final Coder<BoundedSource> sourceCoder;

      CheckpointCoder(Coder<T> elemCoder) {
        this.elemsCoder =
            NullableCoder.of(ListCoder.of(TimestampedValue.TimestampedValueCoder.of(elemCoder)));
        this.elemCoder = elemCoder;
        this.sourceCoder = NullableCoder.of(SerializableCoder.of(BoundedSource.class));
      }

      @Override
      public void encode(Checkpoint<T> value, OutputStream outStream)
          throws CoderException, IOException {
        elemsCoder.encode(value.residualElements, outStream);
        sourceCoder.encode(value.residualSource, outStream);
      }

      @SuppressWarnings("unchecked")
      @Override
      public Checkpoint<T> decode(InputStream inStream) throws CoderException, IOException {
        return new Checkpoint<>(elemsCoder.decode(inStream), sourceCoder.decode(inStream));
      }

      @Override
      public List<Coder<?>> getCoderArguments() {
        return Arrays.asList(elemCoder);
      }

      @Override
      public void verifyDeterministic() throws NonDeterministicException {
        throw new NonDeterministicException(
            this, "CheckpointCoder uses Java Serialization, which may be non-deterministic.");
      }
    }

    /**
     * An {@code UnboundedReader<T>} that wraps a {@code BoundedSource<T>} into {@link
     * ResidualElements} and {@link ResidualSource}.
     *
     * <p>In the initial state, {@link ResidualElements} is null and {@link ResidualSource} contains
     * the {@code BoundedSource<T>}. After the first checkpoint, the {@code BoundedSource<T>} will
     * be split into {@link ResidualElements} and {@link ResidualSource}.
     */
    @VisibleForTesting
    class Reader extends UnboundedReader<T> {
      // Initialized in init()
      private @Nullable ResidualElements residualElements;
      private @Nullable ResidualSource residualSource;
      private final PipelineOptions options;
      private boolean done;

      Reader(
          @Nullable List<TimestampedValue<T>> residualElementsList,
          @Nullable BoundedSource<T> residualSource,
          PipelineOptions options) {
        init(residualElementsList, residualSource, options);
        this.options = checkNotNull(options, "options");
        this.done = false;
      }

      private void init(
          @Nullable List<TimestampedValue<T>> residualElementsList,
          @Nullable BoundedSource<T> residualSource,
          PipelineOptions options) {
        this.residualElements =
            residualElementsList == null
                ? new ResidualElements(Collections.emptyList())
                : new ResidualElements(residualElementsList);

        if (this.residualSource != null) {
          // close current residualSource to avoid leak of reader.close() in ResidualSource
          try {
            this.residualSource.close();
          } catch (IOException e) {
            LOG.warn("Ignore error at closing ResidualSource", e);
          }
        }
        this.residualSource =
            residualSource == null ? null : new ResidualSource(residualSource, options);
      }

      @Override
      public boolean start() throws IOException {
        return advance();
      }

      @Override
      public boolean advance() throws IOException {
        if (residualElements.advance()) {
          return true;
        } else if (residualSource != null && residualSource.advance()) {
          return true;
        } else {
          done = true;
          return false;
        }
      }

      @Override
      public void close() throws IOException {
        if (residualSource != null) {
          residualSource.close();
        }
      }

      @Override
      public T getCurrent() throws NoSuchElementException {
        if (residualElements.hasCurrent()) {
          return residualElements.getCurrent();
        } else if (residualSource != null) {
          return residualSource.getCurrent();
        } else {
          throw new NoSuchElementException();
        }
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        if (residualElements.hasCurrent()) {
          return residualElements.getCurrentTimestamp();
        } else if (residualSource != null) {
          return residualSource.getCurrentTimestamp();
        } else {
          throw new NoSuchElementException();
        }
      }

      @Override
      public Instant getWatermark() {
        return done ? BoundedWindow.TIMESTAMP_MAX_VALUE : BoundedWindow.TIMESTAMP_MIN_VALUE;
      }

      /**
       * {@inheritDoc}
       *
       * <p>If only part of the {@link ResidualElements} is consumed, the new checkpoint will
       * contain the remaining elements in {@link ResidualElements} and the {@link ResidualSource}.
       *
       * <p>If all {@link ResidualElements} and part of the {@link ResidualSource} are consumed, the
       * new checkpoint is done by splitting {@link ResidualSource} into new {@link
       * ResidualElements} and {@link ResidualSource}. {@link ResidualSource} is the source split
       * from the current source, and {@link ResidualElements} contains rest elements from the
       * current source after the splitting. For unsplittable source, it will put all remaining
       * elements into the {@link ResidualElements}.
       */
      @Override
      public Checkpoint<T> getCheckpointMark() {
        Checkpoint<T> newCheckpoint;
        if (!residualElements.done()) {
          // Part of residualElements are consumed.
          // Checkpoints the remaining elements and residualSource.
          newCheckpoint =
              new Checkpoint<>(
                  residualElements.getRestElements(),
                  residualSource == null ? null : residualSource.getSource());
        } else if (residualSource != null) {
          newCheckpoint = residualSource.getCheckpointMark();
        } else {
          newCheckpoint = new Checkpoint<>(null /* residualElements */, null /* residualSource */);
        }
        // Re-initialize since the residualElements and the residualSource might be
        // consumed or split by checkpointing.
        init(newCheckpoint.residualElements, newCheckpoint.residualSource, options);
        return newCheckpoint;
      }

      @Override
      public BoundedToUnboundedSourceAdapter<T> getCurrentSource() {
        return BoundedToUnboundedSourceAdapter.this;
      }
    }

    private class ResidualElements {
      private final List<TimestampedValue<T>> elementsList;
      private @Nullable Iterator<TimestampedValue<T>> elementsIterator;
      private @Nullable TimestampedValue<T> currentT;
      private boolean hasCurrent;
      private boolean done;

      ResidualElements(List<TimestampedValue<T>> residualElementsList) {
        this.elementsList = checkNotNull(residualElementsList, "residualElementsList");
        this.elementsIterator = null;
        this.currentT = null;
        this.hasCurrent = false;
        this.done = false;
      }

      public boolean advance() {
        if (elementsIterator == null) {
          elementsIterator = elementsList.iterator();
        }
        if (elementsIterator.hasNext()) {
          currentT = elementsIterator.next();
          hasCurrent = true;
          return true;
        } else {
          done = true;
          hasCurrent = false;
          return false;
        }
      }

      boolean hasCurrent() {
        return hasCurrent;
      }

      boolean done() {
        return done;
      }

      TimestampedValue<T> getCurrentTimestampedValue() {
        if (!hasCurrent) {
          throw new NoSuchElementException();
        }
        return currentT;
      }

      T getCurrent() {
        return getCurrentTimestampedValue().getValue();
      }

      Instant getCurrentTimestamp() {
        return getCurrentTimestampedValue().getTimestamp();
      }

      List<TimestampedValue<T>> getRestElements() {
        if (elementsIterator == null) {
          return elementsList;
        } else {
          List<TimestampedValue<T>> newResidualElements = Lists.newArrayList();
          while (elementsIterator.hasNext()) {
            newResidualElements.add(elementsIterator.next());
          }
          return newResidualElements;
        }
      }
    }

    private class ResidualSource {
      private BoundedSource<T> residualSource;
      private PipelineOptions options;
      private @Nullable BoundedReader<T> reader;
      private boolean closed;
      private boolean readerDone;

      public ResidualSource(BoundedSource<T> residualSource, PipelineOptions options) {
        this.residualSource = checkNotNull(residualSource, "residualSource");
        this.options = checkNotNull(options, "options");
        this.reader = null;
        this.closed = false;
        this.readerDone = false;
      }

      private boolean advance() throws IOException {
        checkState(!closed, "advance() call on closed %s", getClass().getName());
        if (readerDone) {
          return false;
        }
        if (reader == null) {
          reader = residualSource.createReader(options);
          readerDone = !reader.start();
        } else {
          readerDone = !reader.advance();
        }
        return !readerDone;
      }

      T getCurrent() throws NoSuchElementException {
        if (reader == null) {
          throw new NoSuchElementException();
        }
        return reader.getCurrent();
      }

      Instant getCurrentTimestamp() throws NoSuchElementException {
        if (reader == null) {
          throw new NoSuchElementException();
        }
        return reader.getCurrentTimestamp();
      }

      void close() throws IOException {
        if (reader != null) {
          reader.close();
          reader = null;
        }
        closed = true;
      }

      BoundedSource<T> getSource() {
        return residualSource;
      }

      Checkpoint<T> getCheckpointMark() {
        checkState(!closed, "getCheckpointMark() call on closed %s", getClass().getName());
        if (reader == null) {
          // Reader hasn't started, checkpoint the residualSource.
          return new Checkpoint<>(null /* residualElements */, residualSource);
        } else {
          // Part of residualSource are consumed.
          // Splits the residualSource and tracks the new residualElements in current source.
          BoundedSource<T> residualSplit = null;
          Double fractionConsumed = reader.getFractionConsumed();
          if (fractionConsumed != null && 0 <= fractionConsumed && fractionConsumed <= 1) {
            double fractionRest = 1 - fractionConsumed;
            int splitAttempts = 8;
            for (int i = 0; i < 8 && residualSplit == null; ++i) {
              double fractionToSplit = fractionConsumed + fractionRest * i / splitAttempts;
              residualSplit = reader.splitAtFraction(fractionToSplit);
            }
          }
          List<TimestampedValue<T>> newResidualElements = Lists.newArrayList();
          try {
            while (advance()) {
              newResidualElements.add(
                  TimestampedValue.of(reader.getCurrent(), reader.getCurrentTimestamp()));
            }
          } catch (IOException e) {
            throw new RuntimeException("Failed to read elements from the bounded reader.", e);
          }
          return new Checkpoint<>(newResidualElements, residualSplit);
        }
      }
    }
  }
}
