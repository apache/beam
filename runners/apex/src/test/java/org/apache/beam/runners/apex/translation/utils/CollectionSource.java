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
package org.apache.beam.runners.apex.translation.utils;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.joda.time.Instant;

/** collection as {@link UnboundedSource}, used for tests. */
public class CollectionSource<T> extends UnboundedSource<T, UnboundedSource.CheckpointMark> {
  private static final long serialVersionUID = 1L;
  private final Collection<T> collection;
  private final Coder<T> coder;

  public CollectionSource(Collection<T> collection, Coder<T> coder) {
    this.collection = collection;
    this.coder = coder;
  }

  @Override
  public List<? extends UnboundedSource<T, CheckpointMark>> split(
      int desiredNumSplits, PipelineOptions options) throws Exception {
    return Collections.singletonList(this);
  }

  @Override
  public UnboundedReader<T> createReader(
      PipelineOptions options, @Nullable UnboundedSource.CheckpointMark checkpointMark) {
    return new CollectionReader<>(collection, this);
  }

  @Nullable
  @Override
  public Coder<CheckpointMark> getCheckpointMarkCoder() {
    return null;
  }

  @Override
  public Coder<T> getOutputCoder() {
    return coder;
  }

  private static class CollectionReader<T> extends UnboundedSource.UnboundedReader<T>
      implements Serializable {

    private T current;
    private final CollectionSource<T> source;
    private final Collection<T> collection;
    private Iterator<T> iterator;

    public CollectionReader(Collection<T> collection, CollectionSource<T> source) {
      this.collection = collection;
      this.source = source;
    }

    @Override
    public boolean start() throws IOException {
      if (null == iterator) {
        iterator = collection.iterator();
      }
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
      if (iterator.hasNext()) {
        current = iterator.next();
        return true;
      } else {
        return false;
      }
    }

    @Override
    public Instant getWatermark() {
      return Instant.now();
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
      return null;
    }

    @Override
    public UnboundedSource<T, ?> getCurrentSource() {
      return source;
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
    public void close() throws IOException {}
  }
}
