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
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OperationContext;

/**
 * ApplianceShuffleReader reads chunks of data from a shuffle dataset for a position range.
 *
 * <p>It is a JNI wrapper of an equivalent C++ class.
 */
@ThreadSafe
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public final class ApplianceShuffleReader implements ShuffleReader, Closeable {
  static {
    ShuffleLibrary.load();
  }

  /** Pointer to the underlying C++ ShuffleReader object. */
  private long nativePointer;

  /** Adapter used to import counterFactory from native code. */
  private ApplianceShuffleCounters counters;

  /** Whether the underlying C++ object was already destroyed. */
  private boolean destroyed = false;

  /** Returns the total number of allocated native ShuffleReader objects. */
  public static native int getNumReaders();

  /**
   * @param shuffleReaderConfig opaque configuration for creating a shuffle reader
   * @param operationContext context of the read operation this shuffle reader is associated with
   */
  public ApplianceShuffleReader(byte[] shuffleReaderConfig, OperationContext operationContext) {
    this.nativePointer = createFromConfig(shuffleReaderConfig);
    this.counters =
        new ApplianceShuffleCounters(
            operationContext.counterFactory(), operationContext.nameContext(), getDatasetId());
  }

  public native String getDatasetId();

  @Override
  public native ReadChunkResult readIncludingPosition(byte[] startPosition, byte[] endPosition)
      throws IOException;

  /**
   * Releases resources associated with this reader.
   *
   * <p>May be called multiple times, but once it's called, no other methods can be called on this
   * object.
   */
  @Override
  public void close() {
    destroySynchronized();
  }

  @Override
  protected void finalize() {
    destroySynchronized();
  }

  /**
   * Native methods for interacting with the underlying native shuffle client code. {@code
   * createFromConfig()} returns a pointer to a newly created C++ ShuffleReader object.
   */
  private native long createFromConfig(byte[] shuffleReaderConfig);

  private native void destroy();

  /** Destroys the underlying C++ object if it hasn't yet been destroyed. */
  private synchronized void destroySynchronized() {
    if (!destroyed) {
      destroy();
      destroyed = true;
      nativePointer = 0;
    }
  }
}
