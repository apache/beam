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

package org.apache.beam.sdk.metrics;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

/**
 * A simple unbounded counting source for test.
 * It will return BoundedWindow.TIMESTAMP_MAX_VALUE watermark when emit the number of numElements.
 */
public class UnboundedCountingTestSource extends
    UnboundedSource<Long, UnboundedCountingTestSource.CountingCheckpointMark> {

  private long numElements;

  public UnboundedCountingTestSource(long numElements) {
    this.numElements = numElements;
  }

  @Override
  public void validate() {
  }

  @Override
  public Coder<Long> getDefaultOutputCoder() {
    return VarLongCoder.of();
  }

  @Override
  public List<? extends UnboundedSource<Long, CountingCheckpointMark>> split(
      int desiredNumSplits, PipelineOptions options) throws Exception {
    return Collections.singletonList(new UnboundedCountingTestSource(numElements));
  }

  @Override
  public UnboundedReader<Long> createReader(
      PipelineOptions options, @Nullable CountingCheckpointMark checkpointMark) throws IOException {
    return new Reader(this, numElements, checkpointMark);
  }

  @Override
  public Coder<CountingCheckpointMark> getCheckpointMarkCoder() {
    return new CountingCheckpointMarkCoder();
  }

  private static class Reader extends UnboundedReader<Long> {

    private final long numElements;
    private final UnboundedSource<Long, ?> source;
    private transient long index = 0;

    Reader(UnboundedSource<Long, ?> source, long numElements,
           CountingCheckpointMark checkpointMark) {
      this.source = source;
      this.numElements = numElements;
      if (checkpointMark != null) {
        index = checkpointMark.index;
      }
    }

    @Override
    public Long getCurrent() throws NoSuchElementException {
      return index;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return new Instant(index);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      return index++ < numElements;
    }

    @Override
    public Instant getWatermark() {
      if (index < numElements) {
        return new Instant(index);
      } else {
        return BoundedWindow.TIMESTAMP_MAX_VALUE;
      }
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return new CountingCheckpointMark(index);
    }

    @Override
    public UnboundedSource<Long, ?> getCurrentSource() {
      return source;
    }
  }

  static class CountingCheckpointMark implements UnboundedSource.CheckpointMark {

    private long index;

    CountingCheckpointMark(long index) {
      this.index = index;
    }

    @Override
    public void finalizeCheckpoint() throws IOException {}

  }

  static class CountingCheckpointMarkCoder extends StructuredCoder<CountingCheckpointMark> {
    @Override
    public void encode(CountingCheckpointMark value, OutputStream outStream) throws IOException {
      VarLongCoder.of().encode(value.index, outStream);
    }

    @Override
    public CountingCheckpointMark decode(InputStream inStream) throws IOException {
      return new CountingCheckpointMark(VarLongCoder.of().decode(inStream));
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return null;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
    }
  }

}

