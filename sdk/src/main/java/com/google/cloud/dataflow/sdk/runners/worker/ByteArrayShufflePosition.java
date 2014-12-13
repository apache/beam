/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.api.client.util.Base64.decodeBase64;
import static com.google.api.client.util.Base64.encodeBase64URLSafeString;

import com.google.api.client.util.Preconditions;
import com.google.cloud.dataflow.sdk.util.common.worker.ShufflePosition;
import com.google.common.primitives.UnsignedBytes;

import java.util.Arrays;

/**
 * Represents a ShufflePosition as an array of bytes.
 */
public class ByteArrayShufflePosition implements Comparable, ShufflePosition {
  private final byte[] position;

  public ByteArrayShufflePosition(byte[] position) {
    this.position = position;
  }

  public static ByteArrayShufflePosition fromBase64(String position) {
    return ByteArrayShufflePosition.of(decodeBase64(position));
  }

  public static ByteArrayShufflePosition of(byte[] position) {
    if (position == null) {
      return null;
    }
    return new ByteArrayShufflePosition(position);
  }

  public static byte[] getPosition(ShufflePosition shufflePosition) {
    if (shufflePosition == null) {
      return null;
    }
    Preconditions.checkArgument(
        shufflePosition instanceof ByteArrayShufflePosition);
    ByteArrayShufflePosition adapter = (ByteArrayShufflePosition) shufflePosition;
    return adapter.getPosition();
  }

  public byte[] getPosition() { return position; }

  public String encodeBase64() {
    return encodeBase64URLSafeString(position);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof ByteArrayShufflePosition) {
      ByteArrayShufflePosition that = (ByteArrayShufflePosition) o;
      return Arrays.equals(this.position, that.position);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(position);
  }

  @Override
  public String toString() {
    return "ShufflePosition(" + (new String(position)) + ")";
  }

  @Override
  public int compareTo(Object o) {
    if (this == o) {
      return 0;
    }
    return UnsignedBytes.lexicographicalComparator().compare(
        position, ((ByteArrayShufflePosition) o).position);
  }
}
