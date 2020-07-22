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

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.NameUtils;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * {@link PTransform} that reads a bounded amount of data from an {@link UnboundedSource}, specified
 * as one or both of a maximum number of elements or a maximum period of time to read.
 */
public class BoundedReadFromUnboundedSource<T> extends PTransform<PBegin, PCollection<T>> {
  private final UnboundedSource<T, ?> source;
  private final long maxNumRecords;
  private final @Nullable Duration maxReadTime;

  private static final FluentBackoff BACKOFF_FACTORY =
      FluentBackoff.DEFAULT
          .withInitialBackoff(Duration.millis(10))
          .withMaxBackoff(Duration.standardSeconds(10));

  /**
   * Returns a new {@link BoundedReadFromUnboundedSource} that reads a bounded amount of data from
   * the given {@link UnboundedSource}. The bound is specified as a number of records to read.
   *
   * <p>This may take a long time to execute if the splits of this source are slow to read records.
   */
  public BoundedReadFromUnboundedSource<T> withMaxNumRecords(long maxNumRecords) {
    return new BoundedReadFromUnboundedSource<>(source, maxNumRecords, maxReadTime);
  }

  /**
   * Returns a new {@link BoundedReadFromUnboundedSource} that reads a bounded amount of data from
   * the given {@link UnboundedSource}. The bound is specified as an amount of time to read for.
   * Each split of the source will read for this much time.
   */
  public BoundedReadFromUnboundedSource<T> withMaxReadTime(Duration maxReadTime) {
    return new BoundedReadFromUnboundedSource<>(source, maxNumRecords, maxReadTime);
  }

  BoundedReadFromUnboundedSource(
      UnboundedSource<T, ?> source, long maxNumRecords, @Nullable Duration maxReadTime) {
    this.source = source;
    this.maxNumRecords = maxNumRecords;
    this.maxReadTime = maxReadTime;
  }

  @Override
  public PCollection<T> expand(PBegin input) {
    Coder<Shard<T>> shardCoder = SerializableCoder.of((Class<Shard<T>>) (Class) Shard.class);
    PCollection<ValueWithRecordId<T>> read =
        input
            .apply(
                "Create",
                Create.of(
                        new AutoValue_BoundedReadFromUnboundedSource_Shard.Builder<T>()
                            .setSource(source)
                            .setMaxNumRecords(maxNumRecords)
                            .setMaxReadTime(maxReadTime)
                            .build())
                    .withCoder(shardCoder))
            .apply("Split", ParDo.of(new SplitFn<>()))
            .setCoder(shardCoder)
            .apply("Reshuffle", Reshuffle.viaRandomKey())
            .apply("Read", ParDo.of(new ReadFn<>()))
            .setCoder(ValueWithRecordId.ValueWithRecordIdCoder.of(source.getOutputCoder()));
    if (source.requiresDeduping()) {
      read =
          read.apply(
              Distinct.<ValueWithRecordId<T>, byte[]>withRepresentativeValueFn(
                      ValueWithRecordId::getId)
                  .withRepresentativeType(TypeDescriptor.of(byte[].class)));
    }
    return read.apply("StripIds", ParDo.of(new ValueWithRecordId.StripIdsDoFn<>()))
        .setCoder(source.getOutputCoder());
  }

  @Override
  public String getKindString() {
    return String.format("Read(%s)", NameUtils.approximateSimpleName(source));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    // We explicitly do not register base-class data, instead we use the delegate inner source.
    builder
        .add(DisplayData.item("source", source.getClass()).withLabel("Read Source"))
        .addIfNotDefault(
            DisplayData.item("maxRecords", maxNumRecords).withLabel("Maximum Read Records"),
            Long.MAX_VALUE)
        .addIfNotNull(DisplayData.item("maxReadTime", maxReadTime).withLabel("Maximum Read Time"))
        .include("source", source);
  }

  private static class SplitFn<T> extends DoFn<Shard<T>, Shard<T>> {
    /**
     * Divide the given number of records into {@code numSplits} approximately equal parts that sum
     * to {@code numRecords}.
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

    /** Pick a number of initial splits based on the number of records expected to be processed. */
    private static int numInitialSplits(long numRecords) {
      final int maxSplits = 100;
      final long recordsPerSplit = 10000;
      return (int) Math.min(maxSplits, numRecords / recordsPerSplit + 1);
    }

    @ProcessElement
    public void process(
        @Element Shard<T> shard, OutputReceiver<Shard<T>> out, PipelineOptions options)
        throws Exception {
      int numInitialSplits = numInitialSplits(shard.getMaxNumRecords());
      List<? extends UnboundedSource<T, ?>> splits =
          shard.getSource().split(numInitialSplits, options);
      int numSplits = splits.size();
      long[] numRecords = splitNumRecords(shard.getMaxNumRecords(), numSplits);
      for (int i = 0; i < numSplits; i++) {
        out.output(
            shard
                .toBuilder()
                .setSource(splits.get(i))
                .setMaxNumRecords(numRecords[i])
                .setMaxReadTime(shard.getMaxReadTime())
                .build());
      }
    }
  }

  private static class ReadFn<T> extends DoFn<Shard<T>, ValueWithRecordId<T>> {
    @ProcessElement
    public void process(
        @Element Shard<T> shard, OutputReceiver<ValueWithRecordId<T>> out, PipelineOptions options)
        throws Exception {
      Instant endTime =
          shard.getMaxReadTime() == null ? null : Instant.now().plus(shard.getMaxReadTime());
      if (shard.getMaxNumRecords() <= 0
          || (shard.getMaxReadTime() != null && shard.getMaxReadTime().getMillis() == 0)) {
        return;
      }

      try (UnboundedSource.UnboundedReader<T> reader =
          SerializableUtils.clone(shard.getSource()).createReader(options, null)) {
        for (long i = 0L; i < shard.getMaxNumRecords(); ++i) {
          boolean available = (i == 0) ? reader.start() : reader.advance();
          if (!available && !advanceWithBackoff(reader, endTime)) {
            break;
          }
          out.outputWithTimestamp(
              new ValueWithRecordId<T>(reader.getCurrent(), reader.getCurrentRecordId()),
              reader.getCurrentTimestamp());
        }
        reader.getCheckpointMark().finalizeCheckpoint();
      }
    }

    private boolean advanceWithBackoff(UnboundedReader<T> reader, Instant endTime)
        throws IOException {
      // Try reading from the source with exponential backoff
      BackOff backoff = BACKOFF_FACTORY.backoff();
      long nextSleep = backoff.nextBackOffMillis();
      while (true) {
        if (nextSleep == BackOff.STOP || (endTime != null && Instant.now().isAfter(endTime))) {
          return false;
        }
        if (reader.advance()) {
          return true;
        }
        Uninterruptibles.sleepUninterruptibly(nextSleep, TimeUnit.MILLISECONDS);
        nextSleep = backoff.nextBackOffMillis();
      }
    }
  }

  /**
   * Adapter that wraps the underlying {@link UnboundedSource} with the specified bounds on number
   * of records and read time into a {@link BoundedSource}.
   */
  @AutoValue
  abstract static class Shard<T> implements Serializable {

    abstract @Nullable UnboundedSource<T, ?> getSource();

    abstract long getMaxNumRecords();

    abstract @Nullable Duration getMaxReadTime();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setSource(UnboundedSource<T, ?> source);

      abstract Builder<T> setMaxNumRecords(long maxNumRecords);

      abstract Builder<T> setMaxReadTime(@Nullable Duration maxReadTime);

      abstract Shard<T> build();
    }
  }
}
