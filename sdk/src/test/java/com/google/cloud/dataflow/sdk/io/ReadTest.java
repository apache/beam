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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.io.UnboundedSource.CheckpointMark;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.List;

import javax.annotation.Nullable;

/**
 * Tests for {@link Read}.
 */
@RunWith(JUnit4.class)
public class ReadTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

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

  private abstract static class CustomBoundedSource extends BoundedSource<String> {
    @Override
    public List<? extends BoundedSource<String>> splitIntoBundles(
        long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
      return null;
    }

    @Override
    public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
      return 0;
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return false;
    }

    @Override
    public BoundedReader<String> createReader(PipelineOptions options) throws IOException {
      return null;
    }

    @Override
    public void validate() {}

    @Override
    public Coder<String> getDefaultOutputCoder() {
      return null;
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
    public List<? extends UnboundedSource<String, NoOpCheckpointMark>> generateInitialSplits(
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
      return null;
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
