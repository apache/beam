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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

/** A sharded key consisting of a user key and an opaque shard id represented by bytes. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ShardedKey<K> {

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

  public K getKey() {
    return key;
  }

  @Override
  public String toString() {
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
