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

import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OperationContext;

/**
 * ApplianceShuffleWriter writes chunks of data to a shuffle dataset.
 *
 * <p>It is a JNI wrapper of an equivalent C++ class.
 */
@ThreadSafe
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public final class ApplianceShuffleWriter implements ShuffleWriter {
  static {
    ShuffleLibrary.load();
  }

  /** Pointer to the underlying native shuffle writer code. */
  private long nativePointer;

  /** Adapter used to import counters from native code. */
  private ApplianceShuffleCounters counters;

  /**
   * @param shuffleWriterConfig opaque configuration for creating a shuffle writer
   * @param bufferSize the writer buffer size
   * @param operationContext context of the write operation this shuffle writer is associated with
   */
  public ApplianceShuffleWriter(
      byte[] shuffleWriterConfig, long bufferSize, OperationContext operationContext) {
    this.nativePointer = createFromConfig(shuffleWriterConfig, bufferSize);
    this.counters =
        new ApplianceShuffleCounters(
            operationContext.counterFactory(), operationContext.nameContext(), getDatasetId());
  }

  @Override
  protected void finalize() {
    destroy();
  }

  /** Native methods for interacting with the underlying native shuffle writer code. */
  private native long createFromConfig(byte[] shuffleWriterConfig, long bufferSize);

  private native void destroy();

  public native String getDatasetId();

  @Override
  public native void write(byte[] chunk) throws IOException;

  @Override
  public native void close() throws IOException;
}
