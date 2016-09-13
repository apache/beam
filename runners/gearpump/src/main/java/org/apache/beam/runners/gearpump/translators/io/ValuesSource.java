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

package org.apache.beam.runners.gearpump.translators.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

import org.joda.time.Instant;

/**
 * unbounded source that reads from a Java {@link Iterable}.
 */
public class ValuesSource<T> extends UnboundedSource<T, UnboundedSource.CheckpointMark> {

  private final Iterable<byte[]> values;
  private final Coder<T> coder;

  public ValuesSource(Iterable<T> values, Coder<T> coder) {
    this.values = encode(values, coder);
    this.coder = coder;
  }

  private Iterable<byte[]> encode(Iterable<T> values, Coder<T> coder) {
    List<byte[]> bytes = new LinkedList<>();
    for (T t: values) {
      try {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        coder.encode(t, stream, Coder.Context.OUTER);
        bytes.add(stream.toByteArray());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return bytes;
  }

  @Override
  public java.util.List<? extends UnboundedSource<T, CheckpointMark>> generateInitialSplits(
      int desiredNumSplits, PipelineOptions options) throws Exception {
    return Collections.singletonList(this);
  }

  @Override
  public UnboundedReader<T> createReader(PipelineOptions options,
      @Nullable CheckpointMark checkpointMark) {
    return new ValuesReader<>(values, coder, this);
  }

  @Nullable
  @Override
  public Coder<CheckpointMark> getCheckpointMarkCoder() {
    return null;
  }

  @Override
  public void validate() {
  }

  @Override
  public Coder<T> getDefaultOutputCoder() {
    return coder;
  }

  private static class ValuesReader<T> extends UnboundedReader<T> implements Serializable {

    private final Iterable<byte[]> values;
    private final Coder<T> coder;
    private final UnboundedSource<T, CheckpointMark> source;
    private transient Iterator<byte[]> iterator;
    private T current;

    public ValuesReader(Iterable<byte[]> values, Coder<T> coder,
        UnboundedSource<T, CheckpointMark> source) {
      this.values = values;
      this.coder = coder;
      this.source = source;
    }

    private T decode(byte[] bytes) throws IOException {
      ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
      try {
        return coder.decode(inputStream, Coder.Context.OUTER);
      } finally {
        inputStream.close();
      }
    }

    @Override
    public boolean start() throws IOException {
      if (null == iterator) {
        iterator = values.iterator();
      }
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (iterator.hasNext()) {
        current = decode(iterator.next());
        return true;
      } else {
        return false;
      }
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      return current;
    }

    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return Instant.now();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public Instant getWatermark() {
      return Instant.now();
    }

    @Override
    public CheckpointMark getCheckpointMark() {
      return null;
    }

    @Override
    public UnboundedSource<T, ?> getCurrentSource() {
      return source;
    }
  }
}
