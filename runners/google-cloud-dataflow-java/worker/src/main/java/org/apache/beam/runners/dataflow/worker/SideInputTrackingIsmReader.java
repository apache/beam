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
import org.apache.beam.runners.dataflow.internal.IsmFormat.IsmRecordCoder;
import org.apache.beam.runners.dataflow.util.RandomAccessData;
import org.apache.beam.sdk.io.fs.ResourceId;

/**
 * A wrapper for an {@link IsmReaderImpl} object that provides counters to measure consumption of
 * side inputs.
 *
 * <p>An {@link IsmReaderImpl} object can be reused across threads, and work items because its cost
 * of initialization is high. To be able to provide metrics that are associated to the execution of
 * a single work item, we wrap it in a {@link SideInputTrackingIsmReader}, which is not reused, and
 * thus can know all the context for a single side input element.
 */
public class SideInputTrackingIsmReader<V> extends IsmReader<V> {
  private final IsmReader<V> delegate;
  private final SideInputReadCounter readCounter;

  public SideInputTrackingIsmReader(IsmReader<V> delegate, SideInputReadCounter readCounter) {
    this.delegate = delegate;
    this.readCounter = readCounter;
  }

  @Override
  public boolean isInitialized() {
    try (Closeable counterCloser = IsmReader.setSideInputReadContext(readCounter)) {
      return delegate.isInitialized();
    } catch (IOException e) {
      // This is a checked exception that comes from the {@link Closeable#close} interface method.
      // In our case, this method only realizes a memory operation, so it should never throw an
      // {@link IOException}. This should never happen.
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isEmpty() throws IOException {
    try (Closeable counterCloser = IsmReader.setSideInputReadContext(readCounter)) {
      return delegate.isEmpty();
    }
  }

  /**
   * Returns the {@link ResourceId} associated to the IsmReader.
   *
   * <p>This is simply a getter method, so it does not call {@link
   * IsmReader#setSideInputReadContext}.
   */
  @Override
  public ResourceId getResourceId() {
    return delegate.getResourceId();
  }

  /**
   * Returns the {@link IsmRecordCoder} associated to the IsmReader.
   *
   * <p>This is simply a getter method, so it does not call {@link
   * IsmReader#setSideInputReadContext}.
   */
  @Override
  public IsmRecordCoder<V> getCoder() {
    return delegate.getCoder();
  }

  @Override
  public IsmPrefixReaderIterator iterator() throws IOException {
    try (Closeable counterCloser = IsmReader.setSideInputReadContext(readCounter)) {
      return delegate.iterator();
    }
  }

  @Override
  public IsmPrefixReaderIterator overKeyComponents(List<?> keyComponents) throws IOException {
    try (Closeable counterCloser = IsmReader.setSideInputReadContext(readCounter)) {
      return delegate.overKeyComponents(keyComponents);
    }
  }

  @Override
  public IsmPrefixReaderIterator overKeyComponents(
      List<?> keyComponents, int shardId, RandomAccessData keyBytes) throws IOException {
    try (Closeable counterCloser = IsmReader.setSideInputReadContext(readCounter)) {
      return delegate.overKeyComponents(keyComponents, shardId, keyBytes);
    }
  }
}
