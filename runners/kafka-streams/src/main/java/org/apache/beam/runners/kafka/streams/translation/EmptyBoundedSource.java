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
package org.apache.beam.runners.kafka.streams.translation;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * A {@link BoundedSource} with no elements. The runner substitutes it for a {@code Flatten} of zero
 * PCollections (see {@link KafkaStreamsPipelineTranslator}): reading it produces no data and a
 * terminal watermark — exactly the semantics of an empty PCollection. The element type is never
 * observed since no element is ever produced.
 */
class EmptyBoundedSource extends BoundedSource<byte[]> {

  @Override
  public List<? extends BoundedSource<byte[]>> split(
      long desiredBundleSizeBytes, PipelineOptions options) {
    return Collections.singletonList(this);
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) {
    return 0;
  }

  @Override
  public BoundedReader<byte[]> createReader(PipelineOptions options) {
    return new EmptyReader(this);
  }

  @Override
  public Coder<byte[]> getOutputCoder() {
    return ByteArrayCoder.of();
  }

  /** A reader that is exhausted from the start. */
  private static final class EmptyReader extends BoundedReader<byte[]> {
    private final EmptyBoundedSource source;

    EmptyReader(EmptyBoundedSource source) {
      this.source = source;
    }

    @Override
    public boolean start() {
      return false;
    }

    @Override
    public boolean advance() {
      return false;
    }

    @Override
    public byte[] getCurrent() throws NoSuchElementException {
      throw new NoSuchElementException("EmptyBoundedSource has no elements");
    }

    @Override
    public void close() throws IOException {}

    @Override
    public BoundedSource<byte[]> getCurrentSource() {
      return source;
    }
  }
}
