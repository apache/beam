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
package org.apache.beam.runners.spark.util;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.UnsignedBytes;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Serializable byte array. */
public class ByteArray implements Serializable, Comparable<ByteArray> {

  private final byte[] value;

  public ByteArray(byte[] value) {
    this.value = value;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ByteArray byteArray = (ByteArray) o;
    return Arrays.equals(value, byteArray.value);
  }

  @Override
  public int hashCode() {
    return value != null ? Arrays.hashCode(value) : 0;
  }

  @Override
  public int compareTo(ByteArray other) {
    return UnsignedBytes.lexicographicalComparator().compare(value, other.value);
  }
}
