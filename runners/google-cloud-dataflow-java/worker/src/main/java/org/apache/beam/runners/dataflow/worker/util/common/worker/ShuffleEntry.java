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

import java.util.Arrays;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Entry written to/read from a shuffle dataset. */
public class ShuffleEntry {
  final ShufflePosition position;
  final byte[] key;
  final byte[] secondaryKey;
  final byte[] value;

  public ShuffleEntry(byte[] key, byte[] secondaryKey, byte[] value) {
    this.position = null;
    this.key = key;
    this.secondaryKey = secondaryKey;
    this.value = value;
  }

  public ShuffleEntry(ShufflePosition position, byte[] key, byte[] secondaryKey, byte[] value) {
    this.position = position;
    this.key = key;
    this.secondaryKey = secondaryKey;
    this.value = value;
  }

  public ShufflePosition getPosition() {
    return position;
  }

  public byte[] getKey() {
    return key;
  }

  public byte[] getSecondaryKey() {
    return secondaryKey;
  }

  public byte[] getValue() {
    return value;
  }

  /** Returns the size of this entry in bytes, excluding {@code position}. */
  public int length() {
    return (key == null ? 0 : key.length)
        + (secondaryKey == null ? 0 : secondaryKey.length)
        + (value == null ? 0 : value.length);
  }

  @Override
  public String toString() {
    return "ShuffleEntry("
        + position.toString()
        + ","
        + byteArrayToString(key)
        + ","
        + byteArrayToString(secondaryKey)
        + ","
        + byteArrayToString(value)
        + ")";
  }

  public static String byteArrayToString(byte[] bytes) {
    // TODO: Use a more compact and readable representation,
    // particularly for (nearly-)ascii keys and values.
    return Arrays.toString(bytes);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof ShuffleEntry) {
      ShuffleEntry that = (ShuffleEntry) o;
      return (this.position == null ? that.position == null : this.position.equals(that.position))
          && (this.key == null ? that.key == null : Arrays.equals(this.key, that.key))
          && (this.secondaryKey == null
              ? that.secondaryKey == null
              : Arrays.equals(this.secondaryKey, that.secondaryKey))
          && (this.value == null ? that.value == null : Arrays.equals(this.value, that.value));
    }
    return false;
  }

  @Override
  public int hashCode() {
    return getClass().hashCode()
        + (position == null ? 0 : position.hashCode())
        + (key == null ? 0 : Arrays.hashCode(key))
        + (secondaryKey == null ? 0 : Arrays.hashCode(secondaryKey))
        + (value == null ? 0 : Arrays.hashCode(value));
  }
}
