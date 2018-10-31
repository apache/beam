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
package org.apache.beam.runners.core.construction;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
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
public class UnboundedReadFromBoundedSource<T> extends PTransform<PBegin, PCollection<T>> {

  private static final Logger LOG = LoggerFactory.getLogger(UnboundedReadFromBoundedSource.class);

  private final BoundedSource<T> source;

  /**
   * Constructs a {@link PTransform} that performs an unbounded read from a {@link BoundedSource}.
   */
  public UnboundedReadFromBoundedSource(BoundedSource<T> source) {
    this.source = source;
  }

  @Override
  public PCollection<T> expand(PBegin input) {
    final ArrayDeque<BoundedSource<T>> dequeue = new ArrayDeque<>(Arrays.asList(source));
    return input.getPipeline().apply(Read.from(new BoundedToUnboundedSourceAdapter<>(dequeue)));
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

    //TODO: There must be a better way to figure out the maxOpenConnections available. Use that number here
    //Mention this as part of PR to get feedback.
    private static final int READER_QUEUE_SIZE = 10;
    private ArrayDeque<BoundedSource<T>> boundedSources;
    private Coder<Checkpoint<T>> checkpointCoder;
    private Coder<T> sourceCoder;

    public BoundedToUnboundedSourceAdapter(ArrayDeque<BoundedSource<T>> boundedSources) {
      this.boundedSources = boundedSources;
      final BoundedSource<T> source = boundedSources.peek();
      this.checkpointCoder = new CheckpointCoder<>(source.getDefaultOutputCoder());
      this.sourceCoder = source.getOutputCoder();
    }

    @Override
    public List<BoundedToUnboundedSourceAdapter<T>> split(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      try {
        /**
         * This method gets called from UnboundedSourceWrapper and the PTransform gets a
         * BoundedSource. Within the wrapper we wrap the BoundedSource into a
         * ArrayDeque<BoundedSource<T>>. We need the wrapper so that we don't open a lot of readers
         * for the reading BoundedSources at the same time. More
         * details @https://issues.apache.org/jira/browse/BEAM-5650
         */
        final BoundedSource<T> boundedSource = boundedSources.peek();
        long estimatedSize = boundedSource.getEstimatedSizeBytes(options);
        long desiredBundleSize = estimatedSize / desiredNumSplits;

        if (desiredBundleSize <= 0) {
          LOG.warn(
              "BoundedSource {} cannot estimate its size, skips the initial splits.",
              boundedSources);
          return ImmutableList.of(this);
        }
        List<? extends BoundedSource<T>> splits = boundedSource.split(desiredBundleSize, options);
        final List<? extends List<? extends BoundedSource<T>>> partition =
            Lists.partition(splits, READER_QUEUE_SIZE);
        return partition
            .stream()
            .map(
                data -> {
                  ArrayDeque<BoundedSource<T>> queue = new ArrayDeque<>();
                  data.stream().forEach(queue::add);
                  return new BoundedToUnboundedSourceAdapter<>(queue);
                })
            .collect(Collectors.toList());
      } catch (Exception e) {
        LOG.warn("Exception while splitting {}, skips the initial splits.", boundedSources, e);
        return ImmutableList.of(this);
      }
    }

    @Override
    public Reader createReader(PipelineOptions options, @Nullable Checkpoint<T> checkpoint)
        throws IOException {
      if (checkpoint == null) {
        return new Reader(null, boundedSources, options);
      } else {
        return new Reader(checkpoint.residualElements, checkpoint.residualSources, options);
      }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public Coder<Checkpoint<T>> getCheckpointMarkCoder() {
      return checkpointCoder;
    }

    @Override
    public Coder<T> getOutputCoder() {
      return sourceCoder;
    }

    static class CheckpointCoder<T> extends StructuredCoder<Checkpoint<T>> {

      // The coder for a list of residual elements and their timestamps
      private final Coder<List<TimestampedValue<T>>> elemsCoder;

      // The coder from the BoundedReader for coding each element
      private final Coder<T> elemCoder;

      // The nullable and serializable coder for ArrayDeque
      @SuppressWarnings("rawtypes")
      private final Coder<ArrayDeque> sourceCoder;

      CheckpointCoder(Coder<T> elemCoder) {
        this.elemsCoder =
            NullableCoder.of(ListCoder.of(TimestampedValue.TimestampedValueCoder.of(elemCoder)));
        this.elemCoder = elemCoder;
        this.sourceCoder = NullableCoder.of(SerializableCoder.of(ArrayDeque.class));
      }

      @Override
      public void encode(Checkpoint<T> value, OutputStream outStream)
          throws CoderException, IOException {
        elemsCoder.encode(value.residualElements, outStream);
        sourceCoder.encode(value.residualSources, outStream);
      }

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

    @VisibleForTesting
    class Reader extends UnboundedReader<T> {
      // Initialized in init()
      private @Nullable ResidualElements residualElements;
      private @Nullable ResidualSources residualSources;
      private final PipelineOptions options;
      private boolean done;

      Reader(
          @Nullable List<TimestampedValue<T>> residualElementsList,
          @Nullable ArrayDeque<BoundedSource<T>> residualSources,
          PipelineOptions options) {
        init(residualElementsList, residualSources, options);
        this.options = checkNotNull(options, "options");
        this.done = false;
      }

      private void init(
          @Nullable List<TimestampedValue<T>> residualElementsList,
          @Nullable ArrayDeque<BoundedSource<T>> residualSources,
          PipelineOptions options) {
        this.residualElements =
            residualElementsList == null
                ? new ResidualElements(Collections.emptyList())
                : new ResidualElements(residualElementsList);
        this.residualSources =
            residualSources == null ? null : new ResidualSources(residualSources, options);
      }

      @Override
      public boolean start() throws IOException {
        return advance();
      }

      @Override
      public boolean advance() throws IOException {
        if (residualElements.advance()) {
          return true;
        } else if (residualSources != null && residualSources.advance()) {
          return true;
        } else {
          done = true;
          return false;
        }
      }

      @Override
      public T getCurrent() throws NoSuchElementException {
        if (residualElements.hasCurrent()) {
          return residualElements.getCurrent();
        } else if (residualSources != null) {
          return residualSources.getCurrent();
        } else {
          throw new NoSuchElementException();
        }
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        if (residualElements.hasCurrent()) {
          return residualElements.getCurrentTimestamp();
        } else if (residualSources != null) {
          return residualSources.getCurrentTimestamp();
        } else {
          throw new NoSuchElementException();
        }
      }

      @Override
      public void close() throws IOException {
        if (residualSources != null) {
          residualSources.close();
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
       * contain the remaining elements in {@link ResidualElements} and the {@link ResidualSources}.
       *
       * <p>If all {@link ResidualElements} and part of the {@link ResidualSources} are consumed,
       * the new checkpoint is done by splitting {@link ResidualSources} into new {@link
       * ResidualElements} and {@link ResidualSources}. {@link ResidualSources} is the source split
       * from the current source, and {@link ResidualElements} contains rest elements from the
       * current source after the splitting. For unsplittable source, it will put all remaining
       * elements into the {@link ResidualElements}.
       */
      @Override
      public Checkpoint<T> getCheckpointMark() {
        Checkpoint<T> checkpoint;
        if (!residualElements.done()) {
          // Part of residualElements are consumed.
          // Checkpoints the remaining elements and residualSources.
          checkpoint =
              new Checkpoint<>(
                  residualElements.getRestElements(),
                  residualSources == null ? new ArrayDeque<>() : residualSources.getSources());
        } else if (residualSources != null) {
          checkpoint = residualSources.getCheckpointMark();
        } else {
          checkpoint =
              new Checkpoint<>(
                  null /* residualElements */, new ArrayDeque<>() /* residualSource */);
        }
        // Re-initialize since the residualElements and the residualSources might be
        // consumed or split by checkpointing.
        init(checkpoint.residualElements, checkpoint.residualSources, options);
        return checkpoint;
      }

      @Override
      public BoundedToUnboundedSourceAdapter<T> getCurrentSource() {
        return BoundedToUnboundedSourceAdapter.this;
      }
    }

    /**
     * A marker representing the progress and state of an {@link BoundedToUnboundedSourceAdapter}.
     */
    @VisibleForTesting
    public static class Checkpoint<T> implements UnboundedSource.CheckpointMark {

      private final @Nullable List<TimestampedValue<T>> residualElements;
      private @Nullable ArrayDeque<BoundedSource<T>> residualSources;

      public Checkpoint(
          @Nullable List<TimestampedValue<T>> residualElements,
          ArrayDeque<BoundedSource<T>> residualSources) {
        this.residualElements = residualElements;
        this.residualSources = residualSources;
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
      ArrayDeque<BoundedSource<T>> getResidualSources() {
        return residualSources;
      }
    }

    private class ResidualSources {
      private ArrayDeque<BoundedSource<T>> residualSources;
      private PipelineOptions options;
      private @Nullable BoundedReader<T> reader;
      private boolean closed;
      private boolean readerDone;
      private boolean currentResidualSourceDone;
      private @Nullable BoundedSource<T> currentResidualSource;

      public ResidualSources(
          ArrayDeque<BoundedSource<T>> residualSources, PipelineOptions options) {
        this.residualSources = residualSources;
        this.options = checkNotNull(options, "options");
        this.reader = null;
        this.closed = false;
        this.readerDone = false;
        this.currentResidualSourceDone = false;
      }

      private boolean checkpointAdvanceHelper(boolean onlyFinishReadingCurrentSource)
          throws IOException {
        return helper(onlyFinishReadingCurrentSource);
      }

      private boolean helper(boolean onlyFinishReadingCurrentSource) throws IOException {
        checkArgument(!closed, "advance() call on closed %s,", getClass().getName());
        if (readerDone) {
          return false;
        }

        if (reader == null && !residualSources.isEmpty()) {
          currentResidualSource = residualSources.poll();
          reader = currentResidualSource.createReader(options);
          currentResidualSourceDone = !reader.start();
        } else {
          if (reader == null || residualSources == null) {
            return false;
          }
          currentResidualSourceDone = !reader.advance();
        }

        if (currentResidualSourceDone
            && !residualSources.isEmpty()
            && !onlyFinishReadingCurrentSource) {
          reader.close();
          reader = null;
          currentResidualSource = residualSources.poll();
          reader = currentResidualSource.createReader(options);
          currentResidualSourceDone = !reader.start();
        }

        if (residualSources.isEmpty() && currentResidualSourceDone) {
          readerDone = true;
        }
        return !readerDone;
      }

      private boolean advance() throws IOException {
        return helper(false);
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

      ArrayDeque<BoundedSource<T>> getSources() {
        return residualSources;
      }

      Checkpoint<T> getCheckpointMark() {
        if (reader == null) {
          // Reader hasn't started, checkpoint the residualSources.
          return new Checkpoint<>(null, residualSources);
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
            while (checkpointAdvanceHelper(true) && !currentResidualSourceDone) {

              newResidualElements.add(
                  TimestampedValue.of(reader.getCurrent(), reader.getCurrentTimestamp()));
            }
          } catch (IOException e) {
            throw new RuntimeException("Failed to read elements from the bounded reader.", e);
          }
          if (residualSplit != null) {
            residualSources.addFirst(residualSplit);
          }
          return new Checkpoint<>(newResidualElements, residualSources);
        }
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
  }
}
