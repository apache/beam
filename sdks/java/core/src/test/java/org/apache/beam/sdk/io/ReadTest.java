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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Read}. */
@RunWith(JUnit4.class)
public class ReadTest implements Serializable {
  @Rule public transient ExpectedException thrown = ExpectedException.none();

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
