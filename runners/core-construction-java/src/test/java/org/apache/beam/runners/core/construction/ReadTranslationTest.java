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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.ReadPayload;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link ReadTranslation}. */
@RunWith(Parameterized.class)
public class ReadTranslationTest {

  @Parameters(name = "{index}: {0}")
  public static Iterable<Source<?>> data() {
    return ImmutableList.of(
        CountingSource.unbounded(),
        CountingSource.upTo(100L),
        new TestBoundedSource(),
        new TestUnboundedSource());
  }

  @Parameter(0)
  public Source<?> source;

  @Test
  public void testToFromProtoBounded() throws Exception {
    // TODO: Split into two tests.
    assumeThat(source, instanceOf(BoundedSource.class));
    BoundedSource<?> boundedSource = (BoundedSource<?>) this.source;
    Read.Bounded<?> boundedRead = Read.from(boundedSource);
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    ReadPayload payload = ReadTranslation.toProto(boundedRead, components);
    assertThat(payload.getIsBounded(), equalTo(RunnerApi.IsBounded.Enum.BOUNDED));
    BoundedSource<?> deserializedSource = ReadTranslation.boundedSourceFromProto(payload);
    assertThat(deserializedSource, equalTo(source));
  }

  @Test
  public void testToFromProtoUnbounded() throws Exception {
    assumeThat(source, instanceOf(UnboundedSource.class));
    UnboundedSource<?, ?> unboundedSource = (UnboundedSource<?, ?>) this.source;
    Read.Unbounded<?> unboundedRead = Read.from(unboundedSource);
    SdkComponents components = SdkComponents.create();
    // No environment set for unbounded sources
    ReadPayload payload = ReadTranslation.toProto(unboundedRead, components);
    assertThat(payload.getIsBounded(), equalTo(RunnerApi.IsBounded.Enum.UNBOUNDED));
    UnboundedSource<?, ?> deserializedSource = ReadTranslation.unboundedSourceFromProto(payload);
    assertThat(deserializedSource, equalTo(source));
  }

  private static class TestBoundedSource extends BoundedSource<String> {
    @Override
    public List<? extends BoundedSource<String>> split(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    public BoundedReader<String> createReader(PipelineOptions options) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Coder<String> getOutputCoder() {
      return StringUtf8Coder.of();
    }

    @Override
    public boolean equals(Object other) {
      return other != null && other.getClass().equals(TestBoundedSource.class);
    }

    @Override
    public int hashCode() {
      return TestBoundedSource.class.hashCode();
    }
  }

  private static class TestUnboundedSource extends UnboundedSource<byte[], CheckpointMark> {
    @Override
    public Coder<byte[]> getOutputCoder() {
      return ByteArrayCoder.of();
    }

    @Override
    public List<? extends UnboundedSource<byte[], CheckpointMark>> split(
        int desiredNumSplits, PipelineOptions options) throws Exception {
      throw new UnsupportedOperationException();
    }

    @Override
    public UnboundedReader<byte[]> createReader(
        PipelineOptions options, @Nullable CheckpointMark checkpointMark) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public Coder<CheckpointMark> getCheckpointMarkCoder() {
      return new TestCheckpointMarkCoder();
    }

    @Override
    public boolean equals(Object other) {
      return other != null && other.getClass().equals(TestUnboundedSource.class);
    }

    @Override
    public int hashCode() {
      return TestUnboundedSource.class.hashCode();
    }

    private static class TestCheckpointMarkCoder extends AtomicCoder<CheckpointMark> {
      @Override
      public void encode(CheckpointMark value, OutputStream outStream)
          throws CoderException, IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public CheckpointMark decode(InputStream inStream) throws CoderException, IOException {
        throw new UnsupportedOperationException();
      }
    }
  }
}
