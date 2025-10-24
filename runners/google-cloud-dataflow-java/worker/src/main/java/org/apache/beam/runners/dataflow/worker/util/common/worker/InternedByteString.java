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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Interner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Interners;

/*
 * Weakly Interned ByteStrings.
 * Used to save memory and GC pressure by sharing ByteStrings,
 * that are repeated commonly. Encoded stateTags are an example that are Interned.
 * */
@ThreadSafe
public class InternedByteString {

  private static final int MAP_CONCURRENCY =
      Math.max(4, Runtime.getRuntime().availableProcessors());
  private static final Interner<InternedByteString> ENCODED_KEY_INTERNER =
      Interners.newBuilder().weak().concurrencyLevel(MAP_CONCURRENCY).build();

  // ints don't tear and it is safe to cache without synchronization.
  // Defaults to 0.
  private int hashCode;
  private final ByteString byteString;

  private InternedByteString(ByteString byteString) {
    this.byteString = byteString;
  }

  public ByteString byteString() {
    return byteString;
  }

  @Override
  public int hashCode() {
    if (hashCode == 0) {
      hashCode = byteString.hashCode();
    }
    return hashCode;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof InternedByteString)) {
      return false;
    }
    InternedByteString that = (InternedByteString) o;
    return hashCode() == that.hashCode() && Objects.equals(byteString, that.byteString);
  }

  public static InternedByteString of(ByteString value) {
    return ENCODED_KEY_INTERNER.intern(new InternedByteString(value));
  }
}
