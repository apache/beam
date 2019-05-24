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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkState;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Locale;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.CoderUtils;

/**
 * Utility functions for dealing with key encoding. Beam requires keys to be compared in binary
 * format. The helpers here ensure that a consistent encoding is used.
 */
public class FlinkKeyUtils {

  /** Encodes a key to a byte array wrapped inside a ByteBuffer. */
  public static <K> ByteBuffer encodeKey(K key, Coder<K> keyCoder) {
    checkNotNull(keyCoder, "Provided coder must not be null");
    final byte[] keyBytes;
    try {
      keyBytes = CoderUtils.encodeToByteArray(keyCoder, key);
    } catch (Exception e) {
      throw new RuntimeException(String.format(Locale.ENGLISH, "Failed to encode key: %s", key), e);
    }
    return ByteBuffer.wrap(keyBytes);
  }

  /** Decodes a key from a ByteBuffer containing a byte array. */
  static <K> K decodeKey(ByteBuffer byteBuffer, Coder<K> keyCoder) {
    checkNotNull(byteBuffer, "Provided ByteBuffer must not be null");
    checkNotNull(keyCoder, "Provided coder must not be null");
    checkState(byteBuffer.hasArray(), "ByteBuffer key must contain an array.");
    @SuppressWarnings("ByteBufferBackingArray")
    final byte[] keyBytes = byteBuffer.array();
    try {
      return CoderUtils.decodeFromByteArray(keyCoder, keyBytes);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              Locale.ENGLISH, "Failed to decode encoded key: %s", Arrays.toString(keyBytes)),
          e);
    }
  }
}
