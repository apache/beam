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
package org.apache.beam.sdk.io.aws2.kinesis;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Random;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/** Kinesis interface for custom partitioner. */
public interface KinesisPartitioner<T> extends Serializable {
  BigInteger MIN_HASH_KEY = new BigInteger("0");
  BigInteger MAX_HASH_KEY = new BigInteger(StringUtils.repeat("FF", 16), 16);

  /**
   * Determines which shard in the stream the record is assigned to. Partition keys are Unicode
   * strings with a maximum length limit of 256 characters for each key. Amazon Kinesis Data Streams
   * uses the partition key as input to a hash function that maps the partition key and associated
   * data to a specific shard.
   */
  @Nonnull
  String getPartitionKey(T record);

  /**
   * Optional hash value (128-bit integer) to determine explicitly the shard a record is assigned to
   * based on the hash key range of each shard. The explicit hash key overrides the partition key
   * hash.
   */
  default @Nullable String getExplicitHashKey(T record) {
    return null;
  }

  /**
   * An explicit partitioner that always returns a {@code Nonnull} explicit hash key. The partition
   * key is irrelevant in this case, though it cannot be {@code null}.
   */
  interface ExplicitPartitioner<T> extends KinesisPartitioner<T> {
    @Override
    default @Nonnull String getPartitionKey(T record) {
      return "a"; // will be ignored, but can't be null or empty
    }

    /**
     * Required hash value (128-bit integer) to determine explicitly the shard a record is assigned
     * to based on the hash key range of each shard. The explicit hash key overrides the partition
     * key hash.
     */
    @Override
    @Nonnull
    String getExplicitHashKey(T record);
  }

  /**
   * Explicit hash key partitioner that randomly returns one of x precalculated hash keys. Hash keys
   * are derived by equally dividing the 128-bit hash universe, assuming that hash ranges of shards
   * are also equally sized.
   *
   * <p>Note: This simple approach is likely not applicable anymore after resharding a stream. In
   * that case it is recommended to use the ListShards API to retrieve the actual hash key range of
   * each shard and partition based on that.
   *
   * @see <a
   *     href="https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListShards.html">ListShards
   *     API</a>
   */
  static <T> KinesisPartitioner<T> explicitRandomPartitioner(int shards) {
    BigInteger distance = MAX_HASH_KEY.divide(BigInteger.valueOf(shards));
    BigInteger hashKey = distance.divide(BigInteger.valueOf(2));

    String[] hashKeys = new String[shards];
    for (int i = 0; i < shards; i++) {
      hashKeys[i] = hashKey.toString();
      hashKey = hashKey.add(distance);
    }

    return new ExplicitPartitioner<T>() {
      @Nonnull
      @Override
      public String getExplicitHashKey(T record) {
        return hashKeys[new Random().nextInt(shards)];
      }
    };
  }
}
