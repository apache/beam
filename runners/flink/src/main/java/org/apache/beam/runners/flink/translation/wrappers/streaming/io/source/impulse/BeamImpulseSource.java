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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.impulse;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Instant;

/** A Beam {@link BoundedSource} for Impulse Source. */
public class BeamImpulseSource extends BoundedSource<byte[]> {

  @Override
  public List<? extends BoundedSource<byte[]>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    // Always return a single split.
    return Collections.singletonList(this);
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return 0;
  }

  @Override
  public BoundedReader<byte[]> createReader(PipelineOptions options) throws IOException {
    return new ImpulseReader(this);
  }

  @Override
  public Coder<byte[]> getOutputCoder() {
    return ByteArrayCoder.of();
  }

  private static class ImpulseReader extends BoundedSource.BoundedReader<byte[]> {
    private final BeamImpulseSource source;
    private int index;

    private ImpulseReader(BeamImpulseSource source) {
      this.source = source;
      this.index = 0;
    }

    @Override
    public boolean start() {
      return advance();
    }

    @Override
    public boolean advance() {
      return index++ == 0;
    }

    @Override
    public byte[] getCurrent() throws NoSuchElementException {
      if (index == 1) {
        return new byte[0];
      } else {
        throw new NoSuchElementException("No element is available.");
      }
    }

    @Override
    public BoundedSource<byte[]> getCurrentSource() {
      return source;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      if (index == 1) {
        return BoundedWindow.TIMESTAMP_MIN_VALUE;
      } else {
        throw new NoSuchElementException("No element is available.");
      }
    }

    @Override
    public void close() {}
  }
}
