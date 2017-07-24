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
import static org.hamcrest.Matchers.hasItem;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link Read}.
 */
@RunWith(JUnit4.class)
public class ReadTest implements Serializable{
  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  @Rule
  public transient TestPipeline pipeline = TestPipeline.create();

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
    SerializableBoundedSource boundedSource = new SerializableBoundedSource() {
      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.add(DisplayData.item("foo", "bar"));
      }
    };
    SerializableUnboundedSource unboundedSource = new SerializableUnboundedSource() {
      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.add(DisplayData.item("foo", "bar"));
      }
    };
    Duration maxReadTime = Duration.standardMinutes(2345);

    Read.Bounded<String> bounded = Read.from(boundedSource);
    BoundedReadFromUnboundedSource<String> unbounded = Read.from(unboundedSource)
        .withMaxNumRecords(1234)
        .withMaxReadTime(maxReadTime);

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
  @Category(ValidatesRunner.class)
  public void testBoundedPrimitiveDisplayData() {
    testPrimitiveDisplayData(/* isStreaming: */ false);
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testStreamingPrimitiveDisplayData() {
    testPrimitiveDisplayData(/* isStreaming: */ true);
  }

  private void testPrimitiveDisplayData(boolean isStreaming) {
    PipelineOptions options = DisplayDataEvaluator.getDefaultOptions();
    options.as(StreamingOptions.class).setStreaming(isStreaming);
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create(options);

    SerializableBoundedSource boundedSource = new SerializableBoundedSource() {
      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.add(DisplayData.item("foo", "bar"));
      }
    };
    SerializableUnboundedSource unboundedSource = new SerializableUnboundedSource() {
      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        builder.add(DisplayData.item("foo", "bar"));
      }
    };

    Read.Bounded<String> bounded = Read.from(boundedSource);
    BoundedReadFromUnboundedSource<String> unbounded = Read.from(unboundedSource)
        .withMaxNumRecords(1234);

    Set<DisplayData> boundedDisplayData = evaluator
        .displayDataForPrimitiveSourceTransforms(bounded);
    assertThat(boundedDisplayData, hasItem(hasDisplayItem("source", boundedSource.getClass())));
    assertThat(boundedDisplayData, hasItem(includesDisplayDataFor("source", boundedSource)));

    Set<DisplayData> unboundedDisplayData = evaluator
        .displayDataForPrimitiveSourceTransforms(unbounded);
    assertThat(unboundedDisplayData, hasItem(hasDisplayItem("source")));
    assertThat(unboundedDisplayData, hasItem(includesDisplayDataFor("source", unboundedSource)));
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testReadBoundedSourceWithoutCoder() {
    PCollection<Long> res =
        pipeline.apply(Read.from(new BoundedSourceWithoutCoder())).setCoder(VarLongCoder.of());
    PAssert.that(res).containsInAnyOrder(0L, 1L, 2L, 3L, 4L);
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testReadBoundedFromUnboundedSourceWithoutCoder() {
    PCollection<Long> res =
        pipeline.apply(
            Read.from(new UnboundedSourceWithoutCoder())
                .withMaxNumRecords(1)
                .withCoder(VarLongCoder.of()));
    PAssert.that(res).containsInAnyOrder(42L);
    pipeline.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testReadFromUnboundedSourceWithoutCoder() {
    PCollection<Long> res =
        pipeline.apply(Read.from(new UnboundedSourceWithoutCoder())).setCoder(VarLongCoder.of());
    PAssert.that(res).containsInAnyOrder(42L);
    pipeline.run();
  }

  private static class BoundedSourceWithoutCoder extends BoundedSource<Long> {
    @Override
    public List<BoundedSourceWithoutCoder> split(
        long desiredBundleSizeBytes, PipelineOptions options) {
      return Arrays.asList(this);
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return 0;
    }

    @Override
    public BoundedReader<Long> createReader(PipelineOptions options) throws IOException {
      return CountingSource.createSourceForSubrange(0, 5).createReader(options);
    }

    @Override
    public void validate() {}

    // Does not implement getDefaultOutputCoder().
  }

  private static class UnboundedSourceWithoutCoder extends UnboundedSource<Long, CheckpointMark> {
    @Override
    public List<UnboundedSourceWithoutCoder> split(int desiredNumSplits, PipelineOptions options)
        throws Exception {
      return Arrays.asList(this);
    }

    @Override
    public UnboundedReader<Long> createReader(
        PipelineOptions options, @Nullable CheckpointMark checkpointMark) throws IOException {
      return new Reader(this);
    }

    @Override
    public Coder<CheckpointMark> getCheckpointMarkCoder() {
      return null;
    }

    @Override
    public void validate() {}

    // Does not implement getDefaultOutputCoder().

    private static class Reader extends UnboundedReader<Long> {
      private final UnboundedSourceWithoutCoder source;

      private Reader(UnboundedSourceWithoutCoder source) {
        this.source = source;
      }

      @Override
      public Long getCurrent() throws NoSuchElementException {
        return 42L;
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        return Instant.now();
      }

      @Override
      public void close() throws IOException {}

      @Override
      public boolean start() throws IOException {
        return true;
      }

      @Override
      public boolean advance() throws IOException {
        return false;
      }

      @Override
      public Instant getWatermark() {
        // Terminate the pipeline immediately.
        return BoundedWindow.TIMESTAMP_MAX_VALUE;
      }

      @Override
      public CheckpointMark getCheckpointMark() {
        return new NoOpCheckpointMark();
      }

      @Override
      public UnboundedSource<Long, ?> getCurrentSource() {
        return source;
      }
    }
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
    public void validate() {}

    @Override
    public Coder<String> getDefaultOutputCoder() {
      return StringUtf8Coder.of();
    }
  }

  private static class NotSerializableBoundedSource extends CustomBoundedSource {
    @SuppressWarnings("unused")
    private final NotSerializableClass notSerializableClass = new NotSerializableClass();
  }

  private static class SerializableBoundedSource extends CustomBoundedSource {}

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
    @Nullable
    public Coder<NoOpCheckpointMark> getCheckpointMarkCoder() {
      return null;
    }

    @Override
    public void validate() {}

    @Override
    public Coder<String> getDefaultOutputCoder() {
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
