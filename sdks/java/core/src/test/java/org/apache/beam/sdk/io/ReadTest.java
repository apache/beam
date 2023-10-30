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

import static org.apache.beam.sdk.testing.SerializableMatchers.greaterThanOrEqualTo;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.includesDisplayDataFor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.CountingSource.CounterMark;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesUnboundedPCollections;
import org.apache.beam.sdk.testing.UsesUnboundedSplittableParDo;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Read}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class ReadTest implements Serializable {

  private static final Map<String, List<Instant>> STATIC_INSTANT_LIST_MAP =
      new ConcurrentHashMap<>();

  @Rule public transient ExpectedException thrown = ExpectedException.none();
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testInstantiationOfBoundedSourceAsSDFWrapper() {
    DoFn dofn = new Read.BoundedSourceAsSDFWrapperFn<>();
    DoFnInvokers.invokerFor(dofn);
  }

  @Test
  public void failsWhenCustomBoundedSourceIsNotSerializable() {
    thrown.expect(IllegalArgumentException.class);
    Read.from(new NotSerializableBoundedSource());
  }

  @Test
  public void succeedsWhenCustomBoundedSourceIsSerializable() {
    Read.from(new SerializableBoundedSource());
  }

  @Test
  public void failsWhenCustomUnboundedSourceIsNotSerializable() {
    thrown.expect(IllegalArgumentException.class);
    Read.from(new NotSerializableUnboundedSource());
  }

  @Test
  public void succeedsWhenCustomUnboundedSourceIsSerializable() {
    Read.from(new SerializableUnboundedSource());
  }

  @Test
  public void testDisplayData() {
    SerializableBoundedSource boundedSource =
        new SerializableBoundedSource() {
          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add(DisplayData.item("foo", "bar"));
          }
        };
    SerializableUnboundedSource unboundedSource =
        new SerializableUnboundedSource() {
          @Override
          public void populateDisplayData(DisplayData.Builder builder) {
            builder.add(DisplayData.item("foo", "bar"));
          }
        };
    Duration maxReadTime = Duration.standardMinutes(2345);

    Read.Bounded<String> bounded = Read.from(boundedSource);
    BoundedReadFromUnboundedSource<String> unbounded =
        Read.from(unboundedSource).withMaxNumRecords(1234).withMaxReadTime(maxReadTime);

    DisplayData boundedDisplayData = DisplayData.from(bounded);
    assertThat(boundedDisplayData, hasDisplayItem("source", boundedSource.getClass()));
    assertThat(boundedDisplayData, includesDisplayDataFor("source", boundedSource));

    DisplayData unboundedDisplayData = DisplayData.from(unbounded);
    assertThat(unboundedDisplayData, hasDisplayItem("source", unboundedSource.getClass()));
    assertThat(unboundedDisplayData, includesDisplayDataFor("source", unboundedSource));
    assertThat(unboundedDisplayData, hasDisplayItem("maxRecords", 1234));
    assertThat(unboundedDisplayData, hasDisplayItem("maxReadTime", maxReadTime));
  }

  @Test
  public void testReadBoundedPreservesTypeDescriptor() {
    PCollection<String> input = pipeline.apply(Read.from(new SerializableBoundedSource()));
    TypeDescriptor<String> typeDescriptor = input.getTypeDescriptor();
    assertEquals(String.class, typeDescriptor.getType());

    ListBoundedSource<Long> longs = new ListBoundedSource<>(VarLongCoder.of());
    PCollection<List<Long>> numbers = pipeline.apply(Read.from(longs));
    assertEquals(new TypeDescriptor<List<Long>>() {}, numbers.getTypeDescriptor());
  }

  @Test
  @Category({
    NeedsRunner.class,
    UsesUnboundedPCollections.class,
    UsesUnboundedSplittableParDo.class
  })
  public void testUnboundedSdfWrapperCacheStartedReaders() {
    long numElements = 1000L;
    PCollection<Long> input =
        pipeline.apply(Read.from(new ExpectCacheUnboundedSource(numElements)));
    PAssert.that(input)
        .containsInAnyOrder(
            LongStream.rangeClosed(1L, numElements).boxed().collect(Collectors.toList()));
    // TODO(https://github.com/apache/beam/issues/20530): Remove additional experiments when SDF
    // read is default.
    ExperimentalOptions.addExperiment(
        pipeline.getOptions().as(ExperimentalOptions.class), "use_sdf_read");
    // Force the pipeline to run with one thread to ensure the reader will be reused on one DoFn
    // instance.
    // We are not able to use DirectOptions because of circular dependency.
    pipeline
        .runWithAdditionalOptionArgs(ImmutableList.of("--targetParallelism=1"))
        .waitUntilFinish();
  }

  @Test
  @Category({
    NeedsRunner.class,
    UsesUnboundedPCollections.class,
    UsesUnboundedSplittableParDo.class
  })
  public void testWatermarkAdvanceOnClaimFail() {
    // NOTE: this test is supposed to run only against DirectRunner
    // as for other runners it might not be working the interception of watermark
    // through the STATIC_INSTANT_LIST_MAP
    int numElements = 1000;
    final String uuid = UUID.randomUUID().toString();
    List<Instant> interceptedWatermark =
        STATIC_INSTANT_LIST_MAP.computeIfAbsent(uuid, tmp -> new ArrayList<>());
    PCollection<Long> counted =
        pipeline
            .apply(
                newUnboundedReadInterceptingWatermark(
                    numElements,
                    (Serializable & Consumer<Instant>)
                        instant -> STATIC_INSTANT_LIST_MAP.get(uuid).add(instant)))
            .apply(
                Window.<Long>into(new GlobalWindows())
                    .discardingFiredPanes()
                    .triggering(AfterWatermark.pastEndOfWindow()))
            .apply(Count.globally());
    PAssert.that(counted).containsInAnyOrder((long) numElements);
    pipeline.run().waitUntilFinish();
    // verify that the observed watermark gradually moves
    assertThat(interceptedWatermark.size(), greaterThanOrEqualTo(numElements));
    Instant watermark = interceptedWatermark.get(0);
    for (int i = 1; i < interceptedWatermark.size(); i++) {
      assertThat(
          "Watermarks should be non-decreasing sequence, got " + interceptedWatermark,
          !watermark.isAfter(interceptedWatermark.get(i)));
      watermark = interceptedWatermark.get(i);
    }
  }

  private <T extends Serializable & Consumer<Instant>>
      Read.Unbounded<Long> newUnboundedReadInterceptingWatermark(
          long numElements, T interceptedWatermarkReceiver) {

    UnboundedLongSource source = new UnboundedLongSource(numElements);
    return new Read.Unbounded<Long>(null, source) {
      @Override
      @SuppressWarnings("unchecked")
      Read.UnboundedSourceAsSDFWrapperFn<Long, CheckpointMark> createUnboundedSdfWrapper() {
        return new Read.UnboundedSourceAsSDFWrapperFn<Long, CheckpointMark>(
            (Coder) source.getCheckpointMarkCoder()) {
          @Override
          public WatermarkEstimators.Manual newWatermarkEstimator(Instant watermarkEstimatorState) {
            return new WatermarkEstimators.Manual(watermarkEstimatorState) {
              @Override
              public void setWatermark(Instant watermark) {
                super.setWatermark(watermark);
                interceptedWatermarkReceiver.accept(watermark);
              }
            };
          }
        };
      }
    };
  }

  private abstract static class CustomBoundedSource extends BoundedSource<String> {
    @Override
    public List<? extends BoundedSource<String>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      return null;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return 0;
    }

    @Override
    public BoundedReader<String> createReader(PipelineOptions options) throws IOException {
      return null;
    }

    @Override
    public Coder<String> getOutputCoder() {
      return StringUtf8Coder.of();
    }
  }

  private static class ListBoundedSource<T> extends BoundedSource<List<T>> {
    private Coder<T> coder;

    ListBoundedSource(Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public List<? extends BoundedSource<List<T>>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      return null;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return 0;
    }

    @Override
    public BoundedReader<List<T>> createReader(PipelineOptions options) throws IOException {
      return null;
    }

    @Override
    public Coder<List<T>> getOutputCoder() {
      return ListCoder.of(coder);
    }
  }

  private static class NotSerializableBoundedSource extends CustomBoundedSource {
    @SuppressWarnings("unused")
    private final NotSerializableClass notSerializableClass = new NotSerializableClass();
  }

  private static class SerializableBoundedSource extends CustomBoundedSource {}

  private static class ExpectCacheUnboundedSource
      extends UnboundedSource<Long, CountingSource.CounterMark> {

    private final long numElements;

    ExpectCacheUnboundedSource(long numElements) {
      this.numElements = numElements;
    }

    @Override
    public List<? extends UnboundedSource<Long, CounterMark>> split(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      return ImmutableList.of(this);
    }

    @Override
    public UnboundedReader<Long> createReader(
        PipelineOptions options, @Nullable CounterMark checkpointMark) throws IOException {
      if (checkpointMark != null) {
        throw new IOException("The reader should be retrieved from cache instead of a new one");
      }
      return new ExpectCacheReader(this, checkpointMark);
    }

    @Override
    public Coder<Long> getOutputCoder() {
      return VarLongCoder.of();
    }

    @Override
    public Coder<CounterMark> getCheckpointMarkCoder() {
      return new CountingSource.CounterMarkCoder();
    }
  }

  private static class ExpectCacheReader extends UnboundedReader<Long> {
    private long current;
    private ExpectCacheUnboundedSource source;

    ExpectCacheReader(ExpectCacheUnboundedSource source, CounterMark checkpointMark) {
      this.source = source;
      if (checkpointMark == null) {
        current = 0L;
      } else {
        current = checkpointMark.getLastEmitted();
      }
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      current += 1;
      if (current > source.numElements) {
        return false;
      }
      return true;
    }

    @Override
    public Long getCurrent() throws NoSuchElementException {
      return current;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return getWatermark();
    }

    @Override
    public void close() throws IOException {}

    @Override
    public Instant getWatermark() {
      if (current > source.numElements) {
        return BoundedWindow.TIMESTAMP_MAX_VALUE;
      }
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      if (current <= 0) {
        return null;
      }
      return new CounterMark(current, BoundedWindow.TIMESTAMP_MIN_VALUE);
    }

    @Override
    public UnboundedSource<Long, ?> getCurrentSource() {
      return source;
    }
  }

  private abstract static class CustomUnboundedSource
      extends UnboundedSource<String, NoOpCheckpointMark> {
    @Override
    public List<? extends UnboundedSource<String, NoOpCheckpointMark>> split(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      return null;
    }

    @Override
    public UnboundedReader<String> createReader(
        PipelineOptions options, NoOpCheckpointMark checkpointMark) {
      return null;
    }

    @Override
    public @Nullable Coder<NoOpCheckpointMark> getCheckpointMarkCoder() {
      return null;
    }

    @Override
    public boolean requiresDeduping() {
      return true;
    }

    @Override
    public Coder<String> getOutputCoder() {
      return StringUtf8Coder.of();
    }
  }

  private static class NoOpCheckpointMark implements CheckpointMark {
    @Override
    public void finalizeCheckpoint() throws IOException {}
  }

  private static class NotSerializableUnboundedSource extends CustomUnboundedSource {
    @SuppressWarnings("unused")
    private final NotSerializableClass notSerializableClass = new NotSerializableClass();
  }

  private static class SerializableUnboundedSource extends CustomUnboundedSource {}

  private static class NotSerializableClass {}

  private static class OffsetCheckpointMark implements CheckpointMark {

    private static final Coder<OffsetCheckpointMark> CODER =
        new CustomCoder<OffsetCheckpointMark>() {
          private final VarLongCoder longCoder = VarLongCoder.of();

          @Override
          public void encode(OffsetCheckpointMark value, OutputStream outStream)
              throws CoderException, IOException {
            longCoder.encode(value.offset, outStream);
          }

          @Override
          public OffsetCheckpointMark decode(InputStream inStream)
              throws CoderException, IOException {
            return new OffsetCheckpointMark(longCoder.decode(inStream));
          }
        };

    private final long offset;

    OffsetCheckpointMark(Long offset) {
      this.offset = MoreObjects.firstNonNull(offset, -1L);
    }

    @Override
    public void finalizeCheckpoint() {}
  }

  private class UnboundedLongSource extends UnboundedSource<Long, OffsetCheckpointMark> {

    private final long numElements;

    public UnboundedLongSource(long numElements) {
      this.numElements = numElements;
    }

    @Override
    public List<? extends UnboundedSource<Long, OffsetCheckpointMark>> split(
        int desiredNumSplits, PipelineOptions options) {

      return Collections.singletonList(this);
    }

    @Override
    public UnboundedReader<Long> createReader(
        PipelineOptions options, @Nullable OffsetCheckpointMark checkpointMark) {

      return new UnboundedLongSourceReader(
          Optional.ofNullable(checkpointMark).map(m -> m.offset).orElse(-1L));
    }

    @Override
    public Coder<Long> getOutputCoder() {
      return VarLongCoder.of();
    }

    @Override
    public Coder<OffsetCheckpointMark> getCheckpointMarkCoder() {
      return OffsetCheckpointMark.CODER;
    }

    private class UnboundedLongSourceReader extends UnboundedReader<Long> {
      private final Instant now = Instant.now();
      private long current;

      UnboundedLongSourceReader(long current) {
        this.current = current;
      }

      @Override
      public Long getCurrent() throws NoSuchElementException {
        return current;
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        return current < 0 ? now : now.plus(Duration.millis(current));
      }

      @Override
      public void close() throws IOException {}

      @Override
      public boolean start() throws IOException {
        return advance();
      }

      @Override
      public boolean advance() throws IOException {
        return ++current < numElements;
      }

      @Override
      public Instant getWatermark() {
        return current < numElements ? getCurrentTimestamp() : BoundedWindow.TIMESTAMP_MAX_VALUE;
      }

      @Override
      public CheckpointMark getCheckpointMark() {
        return new OffsetCheckpointMark(current);
      }

      @Override
      public UnboundedSource<Long, ?> getCurrentSource() {
        return UnboundedLongSource.this;
      }
    }
  }
}
