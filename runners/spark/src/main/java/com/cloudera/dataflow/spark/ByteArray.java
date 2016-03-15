/*
 * Copyright (c) 2014, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.dataflow.spark;

import java.io.Serializable;
import java.util.Arrays;

import com.google.common.primitives.UnsignedBytes;

class ByteArray implements Serializable, Comparable<ByteArray> {

  private final byte[] value;

  ByteArray(byte[] value) {
    this.value = value;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
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
