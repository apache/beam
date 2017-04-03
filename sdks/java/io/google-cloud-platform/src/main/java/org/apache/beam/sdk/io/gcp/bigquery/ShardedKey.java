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

package org.apache.beam.sdk.io.gcp.bigquery;

/**
 * A key and a shard number.
 */
class ShardedKey<K> {
  private final K key;
  private final int shardNumber;

  public static <K> ShardedKey<K> of(K key, int shardNumber) {
    return new ShardedKey<>(key, shardNumber);
  }

  ShardedKey(K key, int shardNumber) {
    this.key = key;
    this.shardNumber = shardNumber;
  }

  public K getKey() {
    return key;
  }

  public int getShardNumber() {
    return shardNumber;
  }
}
