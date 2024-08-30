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
package org.apache.beam.runners.dataflow.worker;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecord;
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecordCoder;
import org.apache.beam.runners.dataflow.util.RandomAccessData;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;

/**
 * A {@link NativeReader} that reads Ism files.
 *
 * @param <V> the type of the value written to the sink
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class IsmReader<V> extends NativeReader<WindowedValue<IsmRecord<V>>> {
  private static final ThreadLocal<SideInputReadCounter> CURRENT_SIDE_INPUT_COUNTERS =
      new ThreadLocal<>();

  public static Closeable setSideInputReadContext(SideInputReadCounter readCounter) {
    SideInputReadCounter previousCounter = CURRENT_SIDE_INPUT_COUNTERS.get();
    CURRENT_SIDE_INPUT_COUNTERS.set(readCounter);
    return new Closeable() {
      @Override
      public void close() throws IOException {
        if (previousCounter == null) {
          CURRENT_SIDE_INPUT_COUNTERS.remove();
        } else {
          CURRENT_SIDE_INPUT_COUNTERS.set(previousCounter);
        }
      }
    };
  }

  /**
   * To be able to keep counters of bytes read and time spent reading side inputs, it is necessary
   * to track the current side input counter in a per-thread context. This is because IsmReader
   * instances are cached, and reused across multiple threads and work items (possibly at the same
   * time). When a specific thread needs to consume a side input, the IsmReader implementation will
   * check what is the side input counter in the current context. Each side input counter object is
   * associated to one step and one side input - so when the IsmReader implementation gets the read
   * counter, it will be able to identify which side input it is reading from, and associate its
   * costs to it.
   */
  public static SideInputReadCounter getCurrentSideInputCounter() {
    SideInputReadCounter currentCounter = CURRENT_SIDE_INPUT_COUNTERS.get();
    return currentCounter != null ? currentCounter : NoopSideInputReadCounter.INSTANCE;
  }

  @Override
  public abstract IsmPrefixReaderIterator iterator() throws IOException;

  /**
   * Returns a reader over a set of key components.
   *
   * <p>If the file is empty or their is no key with the same prefix, then we return an empty reader
   * iterator.
   *
   * <p>If less than the required number of shard key components is passed in, then a reader
   * iterator over all the keys is returned.
   *
   * <p>Otherwise we return a reader iterator which only iterates over keys which have the same key
   * prefix.
   *
   * <p>See {@link #overKeyComponents(List, int, RandomAccessData)} for an optimized version of this
   * method that allows the user to supply a precomputed shard id and key byte prefix.
   */
  public abstract IsmPrefixReaderIterator overKeyComponents(List<?> keyComponents)
      throws IOException;

  /**
   * Returns a reader over a set of key components. This method expects the correct shard id and key
   * prefix byte representing the key components to be passed in. If key components is empty, then
   * shard id should be 0 and key bytes should be empty.
   *
   * <p>If the file is empty or their is no key with the same prefix, then we return an empty reader
   * iterator.
   *
   * <p>If less than the required number of shard key components is passed in, then a reader
   * iterator over all the keys is returned.
   *
   * <p>Otherwise we return a reader iterator which only iterates over keys which have the same key
   * prefix.
   */
  public abstract IsmPrefixReaderIterator overKeyComponents(
      List<?> keyComponents, int shardId, RandomAccessData keyBytes) throws IOException;

  /** Returns whether this ISM reader has been initialized. */
  public abstract boolean isInitialized();

  /** Returns whether this ISM reader contains no data. */
  public abstract boolean isEmpty() throws IOException;

  /** Returns the resourceId that this ISM reader is responsible for. */
  public abstract ResourceId getResourceId();

  /** Returns the coder associated with this IsmReader. */
  public abstract IsmRecordCoder<V> getCoder();

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(IsmReader.class)
        .add("resource id", getResourceId())
        .add("coder", getCoder())
        .toString();
  }

  /** The base class of Ism reader iterators which operate over a given key prefix. */
  abstract class IsmPrefixReaderIterator extends NativeReaderIterator<WindowedValue<IsmRecord<V>>> {

    public abstract WindowedValue<IsmRecord<V>> get(List<?> additionalKeyComponents)
        throws IOException;

    public abstract WindowedValue<IsmRecord<V>> getLast() throws IOException;

    @Override
    public abstract WindowedValue<IsmRecord<V>> getCurrent();
  }
}
