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
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.checkerframework.checker.nullness.qual.Nullable;

@Internal
@ThreadSafe
/*
 * A utility class for caching a thread-local {@link ByteStringOutputStream}.
 */
public class ThreadLocalByteStringOutputStream {

  private static final ThreadLocal<@Nullable RefHolder> threadLocalRefHolder = new ThreadLocal<>();

  // Private constructor to prevent instantiations from outside.
  private ThreadLocalByteStringOutputStream() {}

  /**
   * Executes the given function with a thread-local {@link ByteStringOutputStream}. If the thread
   * local stream is already in use, a new one is used. The streams are cached and reused across
   * calls. Callers should not keep a reference to the stream after the function returns.
   */
  public static <T> T withThreadLocalStream(Function<ByteStringOutputStream, T> function) {
    RefHolder refHolder = getRefHolderFromThreadLocal();
    ByteStringOutputStream stream;
    boolean releaseThreadLocal;
    if (refHolder.inUse) {
      // If the thread local stream is already in use, create a new one
      stream = new ByteStringOutputStream();
      releaseThreadLocal = false;
    } else {
      stream = getByteStringOutputStream(refHolder);
      refHolder.inUse = true;
      releaseThreadLocal = true;
    }
    try {
      return function.apply(stream);
    } finally {
      stream.reset();
      if (releaseThreadLocal) {
        refHolder.inUse = false;
      }
    }
  }

  private static class RefHolder {

    public SoftReference<@Nullable ByteStringOutputStream> streamRef =
        new SoftReference<>(new ByteStringOutputStream());

    // Boolean is true when the thread local stream is already in use by the current thread.
    // Used to avoid reusing the same stream from nested calls if any.
    public boolean inUse = false;
  }

  private static RefHolder getRefHolderFromThreadLocal() {
    @Nullable RefHolder refHolder = threadLocalRefHolder.get();
    if (refHolder == null) {
      refHolder = new RefHolder();
      threadLocalRefHolder.set(refHolder);
    }
    return refHolder;
  }

  private static ByteStringOutputStream getByteStringOutputStream(RefHolder refHolder) {
    @Nullable
    ByteStringOutputStream stream = refHolder.streamRef == null ? null : refHolder.streamRef.get();
    if (stream == null) {
      stream = new ByteStringOutputStream();
      refHolder.streamRef = new SoftReference<>(stream);
    }
    return stream;
  }
}
