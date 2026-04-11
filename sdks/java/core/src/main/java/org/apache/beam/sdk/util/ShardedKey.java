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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

/**
 * A sharded key consisting of a user key and a shard identifier.
 *
 * <p>The shard identifier is stored as an opaque byte array. Convenience methods are provided for
 * creating sharded keys with integer shard numbers, which are encoded as 4-byte big-endian arrays.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ShardedKey<K> implements Serializable {
  private static final long serialVersionUID = 1L;

  private final K key;
  private final byte[] shardId;

  protected ShardedKey(K key, byte[] shardId) {
    this.key = key;
    this.shardId = shardId;
  }

  /**
   * Creates a ShardedKey with given key and shard id. Shard id must not be null and must not be
   * mutated.
   */
  public static <K> ShardedKey<K> of(K key, byte[] shardId) {
    checkArgument(key != null, "Key should not be null!");
    checkArgument(shardId != null, "Shard id should not be null!");
    return new ShardedKey<K>(key, shardId);
  }

  /**
   * Creates a ShardedKey with given key and integer shard number. The shard number is stored as a
   * 4-byte big-endian byte array.
   */
  public static <K> ShardedKey<K> of(K key, int shardNumber) {
    checkArgument(key != null, "Key should not be null!");
    byte[] shardId = ByteBuffer.allocate(Integer.BYTES).putInt(shardNumber).array();
    return new ShardedKey<K>(key, shardId);
  }

  public K getKey() {
    return key;
  }

  /**
   * Returns the integer shard number. This method should only be called on ShardedKeys that were
   * created with {@link #of(Object, int)}, or whose shard id is a 4-byte big-endian encoded
   * integer.
   *
   * @throws IllegalArgumentException if the shard id is not 4 bytes
   */
  public int getShardNumber() {
    checkArgument(
        shardId.length == Integer.BYTES,
        "ShardedKey was not created with an integer shard number (shard id has %s bytes,"
            + " expected %s)",
        shardId.length,
        Integer.BYTES);
    return ByteBuffer.wrap(shardId).getInt();
  }

  @Override
  public String toString() {
    if (shardId.length == Integer.BYTES) {
      return "ShardedKey{key=" + key + ", shard=" + ByteBuffer.wrap(shardId).getInt() + "}";
    }
    return "ShardedKey{key=" + key + ", shardId=" + Arrays.toString(shardId) + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof ShardedKey) {
      ShardedKey<?> that = (ShardedKey<?>) o;
      return this.key.equals(that.key) && Arrays.equals(this.shardId, that.shardId);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 1;
    hash *= 1000003;
    hash ^= key.hashCode();
    hash *= 1000003;
    hash ^= Arrays.hashCode(shardId);
    return hash;
  }

  public static class Coder<K> extends StructuredCoder<ShardedKey<K>> {

    private final ByteArrayCoder shardCoder = ByteArrayCoder.of();
    private final org.apache.beam.sdk.coders.Coder<K> keyCoder;

    private Coder(org.apache.beam.sdk.coders.Coder<K> coder) {
      keyCoder = coder;
    }

    public static <K> ShardedKey.Coder<K> of(org.apache.beam.sdk.coders.Coder<K> keyCoder) {
      return new ShardedKey.Coder<K>(keyCoder);
    }

    public org.apache.beam.sdk.coders.Coder<K> getKeyCoder() {
      return keyCoder;
    }

    @Override
    public void encode(ShardedKey<K> shardedKey, OutputStream outStream) throws IOException {
      // The encoding should follow the order:
      //   shard id byte string
      //   encoded user key
      shardCoder.encode(shardedKey.shardId, outStream);
      keyCoder.encode(shardedKey.key, outStream);
    }

    @Override
    public ShardedKey<K> decode(InputStream inStream) throws IOException {
      byte[] shardId = shardCoder.decode(inStream);
      K key = keyCoder.decode(inStream);
      return ShardedKey.of(key, shardId);
    }

    @Override
    public List<? extends org.apache.beam.sdk.coders.Coder<?>> getCoderArguments() {
      return Collections.singletonList(keyCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      verifyDeterministic(this, "Key coder must be deterministic", keyCoder);
    }

    @Override
    public boolean consistentWithEquals() {
      return keyCoder.consistentWithEquals();
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(ShardedKey<K> shardedKey) {
      return shardCoder.isRegisterByteSizeObserverCheap(shardedKey.shardId)
          && keyCoder.isRegisterByteSizeObserverCheap(shardedKey.key);
    }

    @Override
    public Object structuralValue(ShardedKey<K> shardedKey) {
      return ShardedKey.of(keyCoder.structuralValue(shardedKey.key), shardedKey.shardId);
    }

    @Override
    public void registerByteSizeObserver(ShardedKey<K> shardedKey, ElementByteSizeObserver observer)
        throws Exception {
      shardCoder.registerByteSizeObserver(shardedKey.shardId, observer);
      keyCoder.registerByteSizeObserver(shardedKey.key, observer);
    }
  }
}
