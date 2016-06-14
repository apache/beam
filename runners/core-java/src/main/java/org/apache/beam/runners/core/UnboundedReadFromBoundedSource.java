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
package org.apache.beam.runners.core;

import static org.apache.beam.sdk.util.StringUtils.approximateSimpleName;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.TimestampedValue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * {@link PTransform} that performs a unbounded read from an {@link BoundedSource}.
 *
 * <p>Created by {@link Read}.
 */
public class UnboundedReadFromBoundedSource<T> extends PTransform<PInput, PCollection<T>> {

  private static final Logger LOG = LoggerFactory.getLogger(UnboundedReadFromBoundedSource.class);

  private final BoundedSource<T> source;

  /**
   * Constructs a {@link PTransform} that performs an unbounded read from a {@link BoundedSource}.
   */
  public UnboundedReadFromBoundedSource(BoundedSource<T> source) {
    this.source = source;
  }

  @Override
  public PCollection<T> apply(PInput input) {
    return Pipeline.applyTransform(input,
        Read.from(new BoundedToUnboundedSourceAdapter<>(source)));
  }

  @Override
  protected Coder<T> getDefaultOutputCoder() {
    return source.getDefaultOutputCoder();
  }

  @Override
  public String getKindString() {
    return "Read(" + approximateSimpleName(source.getClass()) + ")";
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    // We explicitly do not register base-class data, instead we use the delegate inner source.
    builder
        .add(DisplayData.item("source", source.getClass()))
        .include(source);
  }

  /**
   * A {@code BoundedSource} to {@code UnboundedSource} adapter.
   */
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
    public List<BoundedToUnboundedSourceAdapter<T>> generateInitialSplits(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      try {
        long desiredBundleSize = boundedSource.getEstimatedSizeBytes(options) / desiredNumSplits;
        if (desiredBundleSize <= 0) {
          LOG.warn("BoundedSource cannot estimate its size, skips the initial splits.");
          return ImmutableList.of(this);
        }
        List<? extends BoundedSource<T>> splits
            = boundedSource.splitIntoBundles(desiredBundleSize, options);
        if (splits == null) {
          LOG.warn("BoundedSource cannot split, skips the initial splits.");
          return ImmutableList.of(this);
        }
        return Lists.transform(
            splits,
            new Function<BoundedSource<T>, BoundedToUnboundedSourceAdapter<T>>() {
              @Override
              public BoundedToUnboundedSourceAdapter<T> apply(BoundedSource<T> input) {
                return new BoundedToUnboundedSourceAdapter<>(input);
              }});
      } catch (Exception e) {
        LOG.warn("Exception while splitting, skips the initial splits.", e);
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

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public Coder<Checkpoint<T>> getCheckpointMarkCoder() {
      return new CheckpointCoder<>(boundedSource.getDefaultOutputCoder());
    }

    @VisibleForTesting
    static class Checkpoint<T> implements UnboundedSource.CheckpointMark {
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
      @Nullable List<TimestampedValue<T>> getResidualElements() {
        return residualElements;
      }

      @VisibleForTesting
      @Nullable BoundedSource<T> getResidualSource() {
        return residualSource;
      }
    }

    @VisibleForTesting
    static class CheckpointCoder<T> extends StandardCoder<Checkpoint<T>> {

      @JsonCreator
      public static CheckpointCoder<?> of(
          @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
          List<Coder<?>> components) {
        Preconditions.checkArgument(components.size() == 1,
            "Expecting 1 components, got " + components.size());
        return new CheckpointCoder<>(components.get(0));
      }

      private final Coder<List<TimestampedValue<T>>> elemsCoder;
      private final Coder<T> elemCoder;
      @SuppressWarnings("rawtypes")
      private final Coder<BoundedSource> sourceCoder =
          NullableCoder.of(SerializableCoder.of(BoundedSource.class));

      CheckpointCoder(Coder<T> elemCoder) {
        this.elemsCoder = NullableCoder.of(
            ListCoder.of(TimestampedValue.TimestampedValueCoder.of(elemCoder)));
        this.elemCoder = elemCoder;
      }

      @Override
      public void encode(Checkpoint<T> value, OutputStream outStream, Context context)
          throws CoderException, IOException {
        Context nested = context.nested();
        elemsCoder.encode(value.residualElements, outStream, nested);
        sourceCoder.encode(value.residualSource, outStream, nested);
      }

      @SuppressWarnings("unchecked")
      @Override
      public Checkpoint<T> decode(InputStream inStream, Context context)
          throws CoderException, IOException {
        Context nested = context.nested();
        return new Checkpoint<>(
            elemsCoder.decode(inStream, nested),
            sourceCoder.decode(inStream, nested));
      }

      @Override
      public List<Coder<?>> getCoderArguments() {
        return Arrays.<Coder<?>>asList(elemCoder);
      }

      @Override
      public void verifyDeterministic() throws NonDeterministicException {
        sourceCoder.verifyDeterministic();
        elemsCoder.verifyDeterministic();
      }
    }

    /**
     * An {@code UnboundedReader<T>} that wraps a {@code BoundedSource<T>} into
     * {@link ResidualElements} and {@link ResidualSource}.
     *
     * <p>In the initial state, {@link ResidualElements} is null and {@link ResidualSource} contains
     * the {@code BoundedSource<T>}. After the first checkpoint, the {@code BoundedSource<T>} will
     * be split into {@link ResidualElements} and {@link ResidualSource}.
     */
    @VisibleForTesting
    class Reader extends UnboundedReader<T> {
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
            residualElementsList == null ? null : new ResidualElements(residualElementsList);
        this.residualSource =
            residualSource == null ? null : new ResidualSource(residualSource, options);
      }

      @Override
      public boolean start() throws IOException {
        return advance();
      }

      @Override
      public boolean advance() throws IOException {
        if (residualElements != null && residualElements.advance()) {
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
        if (residualElements != null && residualElements.hasCurrent()) {
          return residualElements.getCurrent();
        } else if (residualSource != null) {
          return residualSource.getCurrent();
        } else {
          throw new NoSuchElementException();
        }
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        if (residualElements != null && residualElements.hasCurrent()) {
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
       * <p>If only part of the {@link ResidualElements} is consumed, the new
       * checkpoint will contain the remaining elements in {@link ResidualElements} and
       * the {@link ResidualSource}.
       *
       * <p>If all {@link ResidualElements} and part of the
       * {@link ResidualSource} are consumed, the new checkpoint is done by splitting
       * {@link ResidualSource} into new {@link ResidualElements} and {@link ResidualSource}.
       * {@link ResidualSource} is the source split from the current source,
       * and {@link ResidualElements} contains rest elements from the current source after
       * the splitting. For unsplittable source, it will put all remaining elements into
       * the {@link ResidualElements}.
       */
      @Override
      public Checkpoint<T> getCheckpointMark() {
        Checkpoint<T> newCheckpoint;
        if (residualElements != null && !residualElements.done()) {
          // Part of residualElements are consumed.
          // Checkpoints the remaining elements and residualSource.
          newCheckpoint = new Checkpoint<>(
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

      public ResidualSource(BoundedSource<T> residualSource, PipelineOptions options) {
        this.residualSource = checkNotNull(residualSource, "residualSource");
        this.options = checkNotNull(options, "options");
        this.reader = null;
      }

      private boolean advance() throws IOException {
        if (reader == null) {
          reader = residualSource.createReader(options);
          return reader.start();
        } else {
          return reader.advance();
        }
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
        }
      }

      BoundedSource<T> getSource() {
        return residualSource;
      }

      Checkpoint<T> getCheckpointMark() {
        if (reader == null) {
          // Reader hasn't started, checkpoint the residualSource.
          return new Checkpoint<>(null /* residualElements */, residualSource);
        } else {
          // Part of residualSource are consumed.
          // Splits the residualSource and tracks the new residualElements in current source.
          BoundedSource<T> residualSplit = null;
          Double fractionConsumed = reader.getFractionConsumed();
          if (fractionConsumed != null) {
            residualSplit = reader.splitAtFraction(fractionConsumed);
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
