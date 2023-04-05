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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.junit.Test;

public class ShardedKeyTest {

  private static final String KEY = "key";
  private static final byte[] EMPTY_SHARD = new byte[0];
  private static final byte[] SHARD = "shard_id".getBytes(UTF_8);

  @Test
  public void testStructuralValueEqual() throws Exception {
    Coder<ShardedKey<String>> coder = ShardedKey.Coder.of(StringUtf8Coder.of());
    CoderProperties.coderSerializable(coder);
    CoderProperties.structuralValueDecodeEncodeEqual(coder, ShardedKey.of(KEY, SHARD));
    CoderProperties.structuralValueDecodeEncodeEqual(coder, ShardedKey.of(KEY, EMPTY_SHARD));
    CoderProperties.structuralValueConsistentWithEquals(
        coder,
        ShardedKey.of(KEY, "shard_id".getBytes(UTF_8)),
        ShardedKey.of(KEY, "shard_id".getBytes(UTF_8)));
    CoderProperties.structuralValueConsistentWithEquals(
        coder, ShardedKey.of(KEY, new byte[0]), ShardedKey.of(KEY, new byte[0]));
  }

  @Test
  public void testDecodeEncodeEqual() throws Exception {
    Coder<ShardedKey<String>> coder = ShardedKey.Coder.of(StringUtf8Coder.of());
    CoderProperties.coderDecodeEncodeEqual(coder, ShardedKey.of(KEY, SHARD));
    CoderProperties.coderDecodeEncodeEqual(coder, ShardedKey.of(KEY, EMPTY_SHARD));
    CoderProperties.coderConsistentWithEquals(
        coder,
        ShardedKey.of(KEY, "shard_id".getBytes(UTF_8)),
        ShardedKey.of(KEY, "shard_id".getBytes(UTF_8)));
    CoderProperties.coderConsistentWithEquals(
        coder, ShardedKey.of(KEY, new byte[0]), ShardedKey.of(KEY, new byte[0]));
    CoderProperties.coderDeterministic(
        coder,
        ShardedKey.of(KEY, "shard_id".getBytes(UTF_8)),
        ShardedKey.of(KEY, "shard_id".getBytes(UTF_8)));
    CoderProperties.coderDeterministic(
        coder, ShardedKey.of(KEY, new byte[0]), ShardedKey.of(KEY, new byte[0]));
  }

  @Test
  public void testEquality() {
    assertEquals(ShardedKey.of("key", new byte[0]), ShardedKey.of("key", new byte[0]));
    assertEquals(
        ShardedKey.of("key", "shard_id".getBytes(UTF_8)),
        ShardedKey.of("key", "shard_id".getBytes(UTF_8)));
  }
}
