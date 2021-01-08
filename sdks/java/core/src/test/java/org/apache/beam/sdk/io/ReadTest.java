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

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.includesDisplayDataFor;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.CountingSource.CounterMark;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
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
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class ReadTest implements Serializable {
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
  @Category(NeedsRunner.class)
  public void testUnboundedSdfWrapperCacheStartedReaders() throws Exception {
    long numElements = 1000L;
    PCollection<Long> input =
        pipeline.apply(Read.from(new ExpectCacheUnboundedSource(numElements)));
    PAssert.that(input)
        .containsInAnyOrder(
            LongStream.rangeClosed(1L, numElements).boxed().collect(Collectors.toList()));
    // Force the pipeline to run with one thread to ensure the reader will be reused on one DoFn
    // instance.
    // We are not able to use DirectOptions because of circular dependency.
    pipeline
        .runWithAdditionalOptionArgs(ImmutableList.of("--targetParallelism=1"))
        .waitUntilFinish();
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
      return AvroCoder.of(CountingSource.CounterMark.class);
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
}
