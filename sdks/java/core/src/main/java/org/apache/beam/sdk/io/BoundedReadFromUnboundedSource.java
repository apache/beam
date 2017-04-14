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

import com.google.api.client.util.BackOff;
import com.google.auto.value.AutoValue;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.NameUtils;
import org.apache.beam.sdk.util.ValueWithRecordId;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;


/**
 * {@link PTransform} that reads a bounded amount of data from an {@link UnboundedSource},
 * specified as one or both of a maximum number of elements or a maximum period of time to read.
 */
public class BoundedReadFromUnboundedSource<T> extends PTransform<PBegin, PCollection<T>> {
  private final UnboundedSource<T, ?> source;
  private final long maxNumRecords;
  private final Duration maxReadTime;
  private final BoundedSource<ValueWithRecordId<T>> adaptedSource;
  private static final FluentBackoff BACKOFF_FACTORY =
      FluentBackoff.DEFAULT
          .withInitialBackoff(Duration.millis(10))
          .withMaxBackoff(Duration.standardSeconds(10));

  /**
   * Returns a new {@link BoundedReadFromUnboundedSource} that reads a bounded amount
   * of data from the given {@link UnboundedSource}.  The bound is specified as a number
   * of records to read.
   *
   * <p>This may take a long time to execute if the splits of this source are slow to read
   * records.
   */
  public BoundedReadFromUnboundedSource<T> withMaxNumRecords(long maxNumRecords) {
    return new BoundedReadFromUnboundedSource<T>(source, maxNumRecords, maxReadTime);
  }

  /**
   * Returns a new {@link BoundedReadFromUnboundedSource} that reads a bounded amount
   * of data from the given {@link UnboundedSource}.  The bound is specified as an amount
   * of time to read for.  Each split of the source will read for this much time.
   */
  public BoundedReadFromUnboundedSource<T> withMaxReadTime(Duration maxReadTime) {
    return new BoundedReadFromUnboundedSource<T>(source, maxNumRecords, maxReadTime);
  }

  BoundedReadFromUnboundedSource(
      UnboundedSource<T, ?> source, long maxNumRecords, Duration maxReadTime) {
    this.source = source;
    this.maxNumRecords = maxNumRecords;
    this.maxReadTime = maxReadTime;
    this.adaptedSource =
            new AutoValue_BoundedReadFromUnboundedSource_UnboundedToBoundedSourceAdapter
                    .Builder()
                    .setSource(source)
                    .setMaxNumRecords(maxNumRecords)
                    .setMaxReadTime(maxReadTime).build();
  }

  /**
   * Returns an adapted {@link BoundedSource} wrapping the underlying {@link UnboundedSource},
   * with the specified bounds on number of records and read time.
   */
  @Experimental
  public BoundedSource<ValueWithRecordId<T>> getAdaptedSource() {
    return adaptedSource;
  }

  @Override
  public PCollection<T> expand(PBegin input) {
    PCollection<ValueWithRecordId<T>> read = Pipeline.applyTransform(input,
        Read.from(getAdaptedSource()));
    if (source.requiresDeduping()) {
      read = read.apply(Distinct.withRepresentativeValueFn(
          new SerializableFunction<ValueWithRecordId<T>, byte[]>() {
            @Override
            public byte[] apply(ValueWithRecordId<T> input) {
              return input.getId();
            }
          }));
    }
    return read.apply("StripIds", ParDo.of(new ValueWithRecordId.StripIdsDoFn<T>()));
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
    builder
        .add(DisplayData.item("source", source.getClass())
          .withLabel("Read Source"))
        .addIfNotDefault(DisplayData.item("maxRecords", maxNumRecords)
          .withLabel("Maximum Read Records"), Long.MAX_VALUE)
        .addIfNotNull(DisplayData.item("maxReadTime", maxReadTime)
          .withLabel("Maximum Read Time"))
        .include("source", source);
  }

  /**
   * Adapter that wraps the underlying {@link UnboundedSource} with the specified bounds on
   * number of records and read time into a {@link BoundedSource}.
   */
  @AutoValue
  abstract static class UnboundedToBoundedSourceAdapter<T>
      extends BoundedSource<ValueWithRecordId<T>> {
    @Nullable abstract UnboundedSource<T, ?> getSource();
    abstract long getMaxNumRecords();
    @Nullable abstract Duration getMaxReadTime();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setSource(UnboundedSource<T, ?> source);
      abstract Builder<T> setMaxNumRecords(long maxNumRecords);
      abstract Builder<T> setMaxReadTime(Duration maxReadTime);
      abstract UnboundedToBoundedSourceAdapter<T> build();
    }

    /**
     * Divide the given number of records into {@code numSplits} approximately
     * equal parts that sum to {@code numRecords}.
     */
    private static long[] splitNumRecords(long numRecords, int numSplits) {
      long[] splitNumRecords = new long[numSplits];
      for (int i = 0; i < numSplits; i++) {
        splitNumRecords[i] = numRecords / numSplits;
      }
      for (int i = 0; i < numRecords % numSplits; i++) {
        splitNumRecords[i] = splitNumRecords[i] + 1;
      }
      return splitNumRecords;
    }

    /**
     * Pick a number of initial splits based on the number of records expected to be processed.
     */
    private static int numInitialSplits(long numRecords) {
      final int maxSplits = 100;
      final long recordsPerSplit = 10000;
      return (int) Math.min(maxSplits, numRecords / recordsPerSplit + 1);
    }

    @Override
    public List<? extends BoundedSource<ValueWithRecordId<T>>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      List<UnboundedToBoundedSourceAdapter<T>> result = new ArrayList<>();
      int numInitialSplits = numInitialSplits(getMaxNumRecords());
      List<? extends UnboundedSource<T, ?>> splits =
          getSource().split(numInitialSplits, options);
      int numSplits = splits.size();
      long[] numRecords = splitNumRecords(getMaxNumRecords(), numSplits);
      for (int i = 0; i < numSplits; i++) {
        result.add(toBuilder()
                .setSource(splits.get(i))
                .setMaxNumRecords(numRecords[i])
                .setMaxReadTime(getMaxReadTime())
                .build());
      }
      return result;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
      // No way to estimate bytes, so returning 0.
      return 0L;
    }

    @Override
    public Coder<ValueWithRecordId<T>> getDefaultOutputCoder() {
      return ValueWithRecordId.ValueWithRecordIdCoder.of(getSource().getDefaultOutputCoder());
    }

    @Override
    public void validate() {
      getSource().validate();
    }

    @Override
    public BoundedReader<ValueWithRecordId<T>> createReader(PipelineOptions options)
        throws IOException {
      return new Reader(getSource().createReader(options, null));
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      builder.delegate(getSource());
    }

    private class Reader extends BoundedReader<ValueWithRecordId<T>> {
      private long recordsRead = 0L;
      private Instant endTime = Instant.now().plus(getMaxReadTime());
      private UnboundedSource.UnboundedReader<T> reader;

      private Reader(UnboundedSource.UnboundedReader<T> reader) {
        this.recordsRead = 0L;
        if (getMaxReadTime() != null) {
          this.endTime = Instant.now().plus(getMaxReadTime());
        } else {
          this.endTime = null;
        }
        this.reader = reader;
      }

      @Override
      public boolean start() throws IOException {
        if (getMaxNumRecords() <= 0 || (getMaxReadTime() != null
                && getMaxReadTime().getMillis() == 0)) {
          return false;
        }

        recordsRead++;
        if (reader.start()) {
          return true;
        } else {
          return advanceWithBackoff();
        }
      }

      @Override
      public boolean advance() throws IOException {
        if (recordsRead >= getMaxNumRecords()) {
          finalizeCheckpoint();
          return false;
        }
        recordsRead++;
        return advanceWithBackoff();
      }

      private boolean advanceWithBackoff() throws IOException {
        // Try reading from the source with exponential backoff
        BackOff backoff = BACKOFF_FACTORY.backoff();
        long nextSleep = backoff.nextBackOffMillis();
        while (nextSleep != BackOff.STOP) {
          if (endTime != null && Instant.now().isAfter(endTime)) {
            finalizeCheckpoint();
            return false;
          }
          if (reader.advance()) {
            return true;
          }
          Uninterruptibles.sleepUninterruptibly(nextSleep, TimeUnit.MILLISECONDS);
          nextSleep = backoff.nextBackOffMillis();
        }
        finalizeCheckpoint();
        return false;
      }

      private void finalizeCheckpoint() throws IOException {
        reader.getCheckpointMark().finalizeCheckpoint();
      }

      @Override
      public ValueWithRecordId<T> getCurrent() throws NoSuchElementException {
        return new ValueWithRecordId<>(reader.getCurrent(), reader.getCurrentRecordId());
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        return reader.getCurrentTimestamp();
      }

      @Override
      public void close() throws IOException {
        reader.close();
      }

      @Override
      public BoundedSource<ValueWithRecordId<T>> getCurrentSource() {
        return UnboundedToBoundedSourceAdapter.this;
      }
    }
  }
}
