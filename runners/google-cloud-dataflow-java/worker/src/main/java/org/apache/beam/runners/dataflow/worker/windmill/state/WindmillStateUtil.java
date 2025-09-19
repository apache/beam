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
package org.apache.beam.runners.dataflow.worker.windmill.state;

import java.io.IOException;
import java.lang.ref.SoftReference;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

class WindmillStateUtil {

  private static final ThreadLocal<@Nullable RefHolder> threadLocalRefHolder = new ThreadLocal<>();

  /** Encodes the given namespace and address as {@code &lt;namespace&gt;+&lt;address&gt;}. */
  @VisibleForTesting
  static ByteString encodeKey(StateNamespace namespace, StateTag<?> address) {
    RefHolder refHolder = getRefHolderFromThreadLocal();
    // Use ByteStringOutputStream rather than concatenation and String.format. We build these keys
    // a lot, and this leads to better performance results. See associated benchmarks.
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
      // stringKey starts and ends with a slash.  We separate it from the
      // StateTag ID by a '+' (which is guaranteed not to be in the stringKey) because the
      // ID comes from the user.
      namespace.appendTo(stream);
      stream.append('+');
      address.appendTo(stream);
      return stream.toByteStringAndReset();
    } catch (IOException e) {
      throw new RuntimeException(e);
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
