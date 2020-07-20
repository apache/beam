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
package org.apache.beam.sdk.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Arrays;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A wrapper around {@link Throwable} that preserves the stack trace on serialization, unlike
 * regular {@link Throwable}.
 */
public final class SerializableThrowable implements Serializable {
  private final @Nullable Throwable throwable;
  private final StackTraceElement @Nullable [] stackTrace;

  public SerializableThrowable(@Nullable Throwable t) {
    this.throwable = t;
    this.stackTrace = (t == null) ? null : t.getStackTrace();
  }

  public @Nullable Throwable getThrowable() {
    return throwable;
  }

  private void readObject(ObjectInputStream is) throws IOException, ClassNotFoundException {
    is.defaultReadObject();
    if (throwable != null) {
      throwable.setStackTrace(stackTrace);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SerializableThrowable that = (SerializableThrowable) o;
    return Arrays.equals(stackTrace, that.stackTrace);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(stackTrace);
  }
}
