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
package org.apache.beam.sdk.values;

import java.io.Serializable;
import javax.annotation.Nullable;

/**
 * A wrapper around a {@link Throwable} for use with coders.
 *
 * <p>Though {@link Throwable} is serializable, it doesn't override {@link Object#equals(Object)},
 * which can lead to false positives in mutation detection for coders. This class provides a coder-
 * safe way to pass exceptions around without running into problems like log spam.
 *
 * <p>This class is not suitable for general-purpose equality comparison among {@link Throwable}s
 * and should only be used to pass a {@link Throwable} from one PTransform to another.
 */
public final class EncodableThrowable implements Serializable {
  private Throwable throwable;

  private EncodableThrowable() {
    // Can't set this to null without adding a pointless @Nullable annotation to the field. It also
    // needs to be set from the constructor to avoid a checkstyle violation.
    this.throwable = new Throwable();
  }

  /** Wraps {@code throwable} and returns the result. */
  public static EncodableThrowable forThrowable(Throwable throwable) {
    EncodableThrowable comparable = new EncodableThrowable();
    comparable.throwable = throwable;
    return comparable;
  }

  /** Returns the underlying {@link Throwable}. */
  public Throwable throwable() {
    return throwable;
  }

  @Override
  public int hashCode() {
    return throwable.hashCode();
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (!(obj instanceof EncodableThrowable)) {
      return false;
    }
    Throwable other = ((EncodableThrowable) obj).throwable;

    // Assuming class preservation is enough to know that serialization/deserialization worked.
    return throwable.getClass().equals(other.getClass());
  }
}
