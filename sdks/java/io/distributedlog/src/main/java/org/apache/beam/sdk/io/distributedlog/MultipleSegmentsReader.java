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
package org.apache.beam.sdk.io.distributedlog;

import com.google.common.collect.ImmutableList;
import com.twitter.distributedlog.exceptions.DLInterruptedException;
import com.twitter.distributedlog.namespace.DistributedLogNamespace;
import java.io.IOException;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;

/**
 * A bounded reader reading data from a distributedlog segment.
 */
class MultipleSegmentsReader<R> extends BoundedReader<R>{

  private final DLBoundedSource<R> source;

  private List<LogSegmentBundle> segments;
  private ListIterator<LogSegmentBundle> segmentsIterator;
  // read state
  private DistributedLogNamespace namespace;
  private SingleLogSegmentReader<R> currentReader;
  private R currentR;
  private volatile boolean done = false;

  MultipleSegmentsReader(
      DLBoundedSource<R> source,
      @Nullable LogSegmentBundle bundle) {
    this.source = source;
    if (null != bundle) {
      this.segments = ImmutableList.of(bundle);
      this.segmentsIterator = segments.listIterator();
    }
  }

  @Override
  public boolean start() throws IOException {
    namespace = source.setupNamespace();
    if (null == segmentsIterator) {
      this.segments = DLBoundedSource.getAllLogSegments(namespace, source.getStreams());
      this.segmentsIterator = this.segments.listIterator();
    }
    return advance();
  }

  @Override
  public boolean advance() throws IOException {
    try {
      if (null != currentReader && currentReader.advance()) {
        currentR = currentReader.getCurrentR();
        return true;
      }
      while (segmentsIterator.hasNext()) {
        // advance the reader and see if it has records
        LogSegmentBundle nextSegment = segmentsIterator.next();
        SingleLogSegmentReader<R> reader =
            new SingleLogSegmentReader<>(namespace, nextSegment, source.getDefaultOutputCoder());
        if (null != currentReader) {
          currentReader.close();
        }
        currentReader = reader;
        reader.start();
        if (reader.advance()) {
          currentR = reader.getCurrentR();
          return true;
        }
        currentReader.close();
        currentReader = null;
      }
      currentR = null;
      done = true;
      return false;
    } catch (DLInterruptedException dlie) {
      Thread.currentThread().interrupt();
      throw dlie;
    }
  }

  @Override
  public R getCurrent() throws NoSuchElementException {
    if (null == currentR) {
      throw new NoSuchElementException();
    }
    return currentR;
  }

  @Override
  public void close() throws IOException {
    if (null != currentReader) {
      currentReader.close();
    }
    namespace.close();
  }

  @Override
  public BoundedSource<R> getCurrentSource() {
    return source;
  }

  @Nullable
  @Override
  public Double getFractionConsumed() {
    if (null == currentReader) {
      return 0.0;
    }
    if (segments.isEmpty()) {
      return 1.0;
    }
    int index = segmentsIterator.previousIndex();
    int numReaders = segments.size();
    if (index == numReaders) {
      return 1.0;
    }
    double before = 1.0 * index / numReaders;
    double after = 1.0 * (index + 1) / numReaders;
    SingleLogSegmentReader<R> ssr = currentReader;
    if (null == ssr) {
      return before;
    }
    return before + ssr.getProgress() * (after - before);
  }

  @Override
  public long getSplitPointsRemaining() {
    if (done) {
      return 0;
    }
    // The current implementation does not currently support dynamic work
    // rebalancing.
    // TODO: will improve this later.
    return 1;
  }

  @Nullable
  @Override
  public BoundedSource<R> splitAtFraction(double fraction) {
    // The current implementation does not currently support this feature.
    // TODO: will improve this later.
    return null;
  }
}
