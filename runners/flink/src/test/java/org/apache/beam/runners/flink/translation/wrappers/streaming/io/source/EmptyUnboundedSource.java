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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io.source;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

public class EmptyUnboundedSource<T>
    extends UnboundedSource<T, EmptyUnboundedSource.DummyCheckpointMark> {

  Instant watermark = BoundedWindow.TIMESTAMP_MIN_VALUE;

  public static class DummyCheckpointMark implements UnboundedSource.CheckpointMark, Serializable {
    @Override
    public void finalizeCheckpoint() {}
  }

  @Override
  public List<? extends EmptyUnboundedSource<T>> split(
      int desiredNumSplits, PipelineOptions options) throws Exception {
    return Collections.singletonList(this);
  }

  @Override
  public UnboundedReader<T> createReader(
      PipelineOptions options, @Nullable DummyCheckpointMark checkpointMark) {
    return new UnboundedReader<T>() {
      @Override
      public boolean start() throws IOException {
        return advance();
      }

      @Override
      public boolean advance() {
        return false;
      }

      @Override
      public Instant getWatermark() {
        return watermark;
      }

      @Override
      public CheckpointMark getCheckpointMark() {
        return new DummyCheckpointMark();
      }

      @Override
      public UnboundedSource<T, ?> getCurrentSource() {
        return EmptyUnboundedSource.this;
      }

      @Override
      public T getCurrent() throws NoSuchElementException {
        throw new NoSuchElementException();
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        throw new NoSuchElementException();
      }

      @Override
      public void close() {}
    };
  }

  @Override
  public Coder<DummyCheckpointMark> getCheckpointMarkCoder() {
    return SerializableCoder.of(DummyCheckpointMark.class);
  }

  public void setWatermark(Instant watermark) {
    this.watermark = watermark;
  }
}
