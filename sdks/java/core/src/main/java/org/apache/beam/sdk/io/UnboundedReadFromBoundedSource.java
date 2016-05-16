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

import static org.apache.beam.sdk.util.StringUtils.approximateSimpleName;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.PropertyNames;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.TimestampedValue;

import com.google.api.client.util.Lists;
import com.google.api.client.util.Preconditions;
import com.google.common.annotations.VisibleForTesting;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.joda.time.Instant;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * {@link PTransform} that performs a unbounded read from an {@link BoundedSource}.
 *
 * <p>Created by {@link Read}.
 */
public class UnboundedReadFromBoundedSource<T> extends PTransform<PInput, PCollection<T>> {
  private final BoundedSource<T> source;

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

  @VisibleForTesting
  static class BoundedToUnboundedSourceAdapter<T>
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
      long desiredBundleSize = boundedSource.getEstimatedSizeBytes(options) / desiredNumSplits;
      List<BoundedToUnboundedSourceAdapter<T>> result = Lists.newArrayList();
      for (BoundedSource<T> split : boundedSource.splitIntoBundles(desiredBundleSize, options)) {
        result.add(new BoundedToUnboundedSourceAdapter<>(split));
      }
      return result;
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
      private final List<TimestampedValue<T>> residualElements;
      private final BoundedSource<T> residualSource;

      public Checkpoint(
          List<TimestampedValue<T>> residualElements, BoundedSource<T> residualSource) {
        this.residualElements = residualElements;
        this.residualSource = residualSource;
      }

      @Override
      public void finalizeCheckpoint() {}
    }

    private static class CheckpointCoder<T> extends AtomicCoder<Checkpoint<T>> {
      private final Coder<List<TimestampedValue<T>>> elemsCoder;
      private final Coder<T> elemCoder;

      @JsonCreator
      public static CheckpointCoder<?> of(
          @JsonProperty(PropertyNames.COMPONENT_ENCODINGS)
          List<Coder<?>> components) {
        Preconditions.checkArgument(components.size() == 1,
            "Expecting 1 components, got " + components.size());
        return new CheckpointCoder<>(components.get(0));
      }

      @SuppressWarnings("rawtypes")
      private final SerializableCoder<BoundedSource> serializableCoder =
          SerializableCoder.of(BoundedSource.class);

      CheckpointCoder(Coder<T> elemCoder) {
        this.elemsCoder = ListCoder.of(TimestampedValue.TimestampedValueCoder.of(elemCoder));
        this.elemCoder = elemCoder;
      }

      @Override
      public void encode(Checkpoint<T> value, OutputStream outStream, Context context)
          throws CoderException, IOException {
        elemsCoder.encode(value.residualElements, outStream, context);
        serializableCoder.encode(value.residualSource, outStream, context);
      }

      @SuppressWarnings("unchecked")
      @Override
      public Checkpoint<T> decode(InputStream inStream, Context context)
          throws CoderException, IOException {
        return new Checkpoint<>(
            elemsCoder.decode(inStream, context),
            serializableCoder.decode(inStream, context));
      }

      @Override
      public List<Coder<?>> getCoderArguments() {
        return Arrays.<Coder<?>>asList(elemCoder);
      }
    }

    @VisibleForTesting
    class Reader extends UnboundedReader<T> {
      private final PipelineOptions options;

      private @Nullable final List<TimestampedValue<T>> residualElements;
      private @Nullable BoundedSource<T> residualSource;

      private boolean currentElementInList;
      private int currentIndexInList;
      private BoundedReader<T> reader;
      private boolean done;

      public Reader(
          @Nullable List<TimestampedValue<T>> residualElements,
          @Nullable BoundedSource<T> residualSource,
          PipelineOptions options) {
        this.residualElements = residualElements;
        this.residualSource = residualSource;
        this.options = Preconditions.checkNotNull(options, "options");
        this.currentElementInList = true;
        this.currentIndexInList = -1;
        this.reader = null;
        this.done = false;
      }

      @Override
      public boolean start() throws IOException {
        return advance();
      }

      @Override
      public boolean advance() throws IOException {
        if (advanceInList()) {
          done = false;
        } else {
          done = !advanceInReader();
        }
        return !done;
      }

      private boolean advanceInList() {
        if (residualElements != null && currentIndexInList + 1 < residualElements.size()) {
          ++currentIndexInList;
          currentElementInList = true;
          return true;
        } else {
          currentElementInList = false;
          return false;
        }
      }

      private boolean advanceInReader() throws IOException {
        boolean hasNext;
        if (boundedSource == null) {
          hasNext = false;
        } else if (reader == null) {
          reader = residualSource.createReader(options);
          hasNext = reader.start();
        } else {
          hasNext = reader.advance();
        }
        return hasNext;
      }

      @Override
      public void close() throws IOException {
        reader.close();
      }

      @Override
      public T getCurrent() throws NoSuchElementException {
        if (currentElementInList) {
          return residualElements.get(currentIndexInList).getValue();
        } else {
          return reader.getCurrent();
        }
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        if (currentElementInList) {
          return residualElements.get(currentIndexInList).getTimestamp();
        } else {
          return reader.getCurrentTimestamp();
        }
      }

      @Override
      public Instant getWatermark() {
        return done ? BoundedWindow.TIMESTAMP_MAX_VALUE : BoundedWindow.TIMESTAMP_MIN_VALUE;
      }

      @Override
      public Checkpoint<T> getCheckpointMark() {
        List<TimestampedValue<T>> newResidualElements = Lists.newArrayList();
        BoundedSource<T> newResidualSource;
        if (currentElementInList) {
          while (advanceInList()) {
            newResidualElements.add(residualElements.get(currentIndexInList));
          }
          newResidualSource = residualSource;
        } else {
          BoundedSource<T> residualSplit = reader.splitAtFraction(reader.getFractionConsumed());
          try {
            while (advanceInReader()) {
              newResidualElements.add(
                  TimestampedValue.of(reader.getCurrent(), reader.getCurrentTimestamp()));
            }
          } catch (NoSuchElementException | IOException e) {
            throw new RuntimeException("Failed to read elements from the bounded reader.", e);
          }
          newResidualSource = residualSplit;
        }
        return new Checkpoint<T>(newResidualElements, newResidualSource);
      }

      @Override
      public BoundedToUnboundedSourceAdapter<T> getCurrentSource() {
        return BoundedToUnboundedSourceAdapter.this;
      }
    }
  }
}
