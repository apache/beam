/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import static com.google.cloud.dataflow.sdk.util.StringUtils.approximateSimpleName;

import com.google.api.client.util.BackOff;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.RemoveDuplicates;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.util.IntervalBoundedExponentialBackOff;
import com.google.cloud.dataflow.sdk.util.ValueWithRecordId;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;

import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * {@link PTransform} that reads a bounded amount of data from an {@link UnboundedSource},
 * specified as one or both of a maximum number of elements or a maximum period of time to read.
 *
 * <p>Created by {@link Read}.
 */
class BoundedReadFromUnboundedSource<T> extends PTransform<PInput, PCollection<T>> {
  private final UnboundedSource<T, ?> source;
  private final long maxNumRecords;
  private final Duration maxReadTime;

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
  }

  @Override
  public PCollection<T> apply(PInput input) {
    PCollection<ValueWithRecordId<T>> read = Pipeline.applyTransform(input,
        Read.from(new UnboundedToBoundedSourceAdapter<>(source, maxNumRecords, maxReadTime)));
    if (source.requiresDeduping()) {
      read = read.apply(RemoveDuplicates.withRepresentativeValueFn(
          new SerializableFunction<ValueWithRecordId<T>, byte[]>() {
            @Override
            public byte[] apply(ValueWithRecordId<T> input) {
              return input.getId();
            }
          }));
    }
    return read.apply(ValueWithRecordId.<T>stripIds());
  }

  @Override
  protected Coder<T> getDefaultOutputCoder() {
    return source.getDefaultOutputCoder();
  }

  @Override
  public String getKindString() {
    return "Read(" + approximateSimpleName(source.getClass()) + ")";
  }

  private static class UnboundedToBoundedSourceAdapter<T>
      extends BoundedSource<ValueWithRecordId<T>> {
    private final UnboundedSource<T, ?> source;
    private final long maxNumRecords;
    private final Duration maxReadTime;

    private UnboundedToBoundedSourceAdapter(
        UnboundedSource<T, ?> source, long maxNumRecords, Duration maxReadTime) {
      this.source = source;
      this.maxNumRecords = maxNumRecords;
      this.maxReadTime = maxReadTime;
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
    public List<? extends BoundedSource<ValueWithRecordId<T>>> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      List<UnboundedToBoundedSourceAdapter<T>> result = new ArrayList<>();
      int numInitialSplits = numInitialSplits(maxNumRecords);
      List<? extends UnboundedSource<T, ?>> splits =
          source.generateInitialSplits(numInitialSplits, options);
      int numSplits = splits.size();
      long[] numRecords = splitNumRecords(maxNumRecords, numSplits);
      for (int i = 0; i < numSplits; i++) {
        result.add(
            new UnboundedToBoundedSourceAdapter<T>(splits.get(i), numRecords[i], maxReadTime));
      }
      return result;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) {
      // No way to estimate bytes, so returning 0.
      return 0L;
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) {
      return false;
    }

    @Override
    public Coder<ValueWithRecordId<T>> getDefaultOutputCoder() {
      return ValueWithRecordId.ValueWithRecordIdCoder.of(source.getDefaultOutputCoder());
    }

    @Override
    public void validate() {
      source.validate();
    }

    @Override
    public BoundedReader<ValueWithRecordId<T>> createReader(PipelineOptions options) {
      return new Reader(source.createReader(options, null));
    }

    private class Reader extends BoundedReader<ValueWithRecordId<T>> {
      private long recordsRead = 0L;
      private Instant endTime = Instant.now().plus(maxReadTime);
      private UnboundedSource.UnboundedReader<T> reader;

      private Reader(UnboundedSource.UnboundedReader<T> reader) {
        this.recordsRead = 0L;
        if (maxReadTime != null) {
          this.endTime = Instant.now().plus(maxReadTime);
        } else {
          this.endTime = null;
        }
        this.reader = reader;
      }

      @Override
      public boolean start() throws IOException {
        if (maxNumRecords <= 0 || (maxReadTime != null && maxReadTime.getMillis() == 0)) {
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
        if (recordsRead >= maxNumRecords) {
          finalizeCheckpoint();
          return false;
        }
        recordsRead++;
        return advanceWithBackoff();
      }

      private boolean advanceWithBackoff() throws IOException {
        // Try reading from the source with exponential backoff
        BackOff backoff = new IntervalBoundedExponentialBackOff(10000, 10);
        long nextSleep = backoff.nextBackOffMillis();
        while (nextSleep != BackOff.STOP) {
          if (endTime != null && Instant.now().isAfter(endTime)) {
            finalizeCheckpoint();
            return false;
          }
          if (reader.advance()) {
            return true;
          }
          try {
            Thread.sleep(nextSleep);
          } catch (InterruptedException e) {}
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
