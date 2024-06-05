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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;

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
      keyBytes = CoderUtils.encodeToByteArray(keyCoder, key, Coder.Context.NESTED);
    } catch (Exception e) {
      throw new RuntimeException(String.format(Locale.ENGLISH, "Failed to encode key: %s", key), e);
    }
    return ByteBuffer.wrap(keyBytes);
  }

  /** Decodes a key from a ByteBuffer containing a byte array. */
  public static <K> K decodeKey(ByteBuffer byteBuffer, Coder<K> keyCoder) {
    checkNotNull(byteBuffer, "Provided ByteBuffer must not be null");
    checkNotNull(keyCoder, "Provided coder must not be null");
    checkState(byteBuffer.hasArray(), "ByteBuffer key must contain an array.");
    @SuppressWarnings("ByteBufferBackingArray")
    final byte[] keyBytes = byteBuffer.array();
    try {
      return CoderUtils.decodeFromByteArray(keyCoder, keyBytes, Coder.Context.NESTED);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              Locale.ENGLISH, "Failed to decode encoded key: %s", Arrays.toString(keyBytes)),
          e);
    }
  }

  static ByteBuffer fromEncodedKey(ByteString encodedKey) {
    return ByteBuffer.wrap(encodedKey.toByteArray());
  }

  /** The Coder for the Runner's encoded representation of a key. */
  static class ByteBufferCoder extends StructuredCoder<ByteBuffer> {

    public static ByteBufferCoder of() {
      return INSTANCE;
    }

    private static final ByteBufferCoder INSTANCE = new ByteBufferCoder();

    private ByteBufferCoder() {}

    @Override
    public void encode(ByteBuffer value, OutputStream outStream) throws IOException {
      @SuppressWarnings("ByteBufferBackingArray")
      byte[] array = value.array();
      ByteArrayCoder.of().encode(array, outStream);
    }

    @Override
    public ByteBuffer decode(InputStream inStream) throws IOException {
      byte[] decode = ByteArrayCoder.of().decode(inStream);
      return ByteBuffer.wrap(decode);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return Collections.emptyList();
    }

    @Override
    public void verifyDeterministic() {}
  }
}
