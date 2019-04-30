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
package org.apache.beam.sdk.io;

import java.io.Serializable;
import java.util.Objects;

/**
 * Default implementation of {@link ShardAwareKey} used internally by {@link WriteFiles} for default
 * random shard assignment.
 */
class DefaultShardKey implements ShardAwareKey, Serializable {
  private static final long serialVersionUID = 1L;
  private final int key;
  private final int shardNumber;

  public static DefaultShardKey of(int key, int shardNumber) {
    return new DefaultShardKey(key, shardNumber);
  }

  private DefaultShardKey(int key, int shardNumber) {
    this.key = key;
    this.shardNumber = shardNumber;
  }

  public int getKey() {
    return key;
  }

  @Override
  public int getShardNumber() {
    return shardNumber;
  }

  @Override
  public String toString() {
    return "key: " + key + " shard: " + shardNumber;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof DefaultShardKey)) {
      return false;
    }
    DefaultShardKey other = (DefaultShardKey) o;
    return Objects.equals(key, other.key) && Objects.equals(shardNumber, other.shardNumber);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, shardNumber);
  }
}
