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
package org.apache.beam.runners.dataflow.worker.util;

import java.lang.ref.SoftReference;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.util.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;

@Internal
@ThreadSafe
/*
 * A utility class for caching a thread-local {@link ByteStringOutputStream}.
 *
 * Example Usage:
 *   try (StreamHandle streamHandle = ThreadLocalByteStringOutputStream.acquire()) {
 *        ByteStringOutputStream stream = streamHandle.stream();
 *        stream.write(1);
 *        ByteString byteString = stream.toByteStringAndReset();
 *   }
 */
public class ThreadLocalByteStringOutputStream {

  private static final ThreadLocal<@Nullable SoftRefHolder> threadLocalSoftRefHolder =
      ThreadLocal.withInitial(SoftRefHolder::new);

  // Private constructor to prevent instantiations from outside.
  private ThreadLocalByteStringOutputStream() {}

  /** @return An AutoClosable StreamHandle that holds a cached ByteStringOutputStream. */
  public static StreamHandle acquire() {
    StreamHandle streamHandle = getStreamHandleFromThreadLocal();
    if (streamHandle.inUse) {
      // Stream is already in use, create a new uncached one
      return new StreamHandle();
    }
    streamHandle.inUse = true;
    return streamHandle; // inUse will be unset when streamHandle closes.
  }

  /**
   * Handle to a thread-local {@link ByteStringOutputStream}. If the thread local stream is already
   * in use, a new one is used. The streams are cached and reused across calls. Users should not
   * keep a reference to the stream after closing the StreamHandle.
   */
  public static class StreamHandle implements AutoCloseable {

    private final ByteStringOutputStream stream = new ByteStringOutputStream();

    private boolean inUse = false;

    /**
     * Returns the underlying cached ByteStringOutputStream. Callers should not keep a reference to
     * the stream after closing the StreamHandle.
     */
    public ByteStringOutputStream stream() {
      return stream;
    }

    @Override
    public void close() {
      stream.reset();
      inUse = false;
    }
  }

  private static class SoftRefHolder {
    private @Nullable SoftReference<StreamHandle> softReference;
  }

  private static StreamHandle getStreamHandleFromThreadLocal() {
    // softRefHolder is only set by Threadlocal initializer and should not be null
    SoftRefHolder softRefHolder =
        Preconditions.checkArgumentNotNull(threadLocalSoftRefHolder.get());
    @Nullable StreamHandle streamHandle = null;
    @Nullable SoftReference<StreamHandle> softReference = softRefHolder.softReference;
    if (softReference != null) {
      streamHandle = softReference.get();
    }
    if (streamHandle == null) {
      streamHandle = new StreamHandle();
      softRefHolder.softReference = new SoftReference<>(streamHandle);
    }
    return streamHandle;
  }
}
