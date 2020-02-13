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
package org.apache.beam.runners.flink;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.runners.flink.FlinkStreamingPipelineTranslator.FlinkAutoBalancedShardKeyShardingFunction;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.junit.Test;

/** Tests if overrides are properly applied. */
public class FlinkStreamingPipelineTranslatorTest {

  @Test
  public void testAutoBalanceShardKeyResolvesMaxParallelism() {

    int parallelism = 3;
    assertThat(
        new FlinkAutoBalancedShardKeyShardingFunction<>(parallelism, -1, StringUtf8Coder.of())
            .getMaxParallelism(),
        equalTo(KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism)));
    assertThat(
        new FlinkAutoBalancedShardKeyShardingFunction<>(parallelism, 0, StringUtf8Coder.of())
            .getMaxParallelism(),
        equalTo(KeyGroupRangeAssignment.computeDefaultMaxParallelism(parallelism)));
  }

  @Test
  public void testAutoBalanceShardKeyCacheIsNotSerialized() throws Exception {

    FlinkAutoBalancedShardKeyShardingFunction<String, String> fn =
        new FlinkAutoBalancedShardKeyShardingFunction<>(2, 2, StringUtf8Coder.of());

    assertNull(fn.getCache());

    fn.assignShardKey("target/destination1", "one", 10);
    fn.assignShardKey("target/destination2", "two", 10);

    assertThat(fn.getCache().size(), equalTo(2));
    assertThat(SerializableUtils.clone(fn).getCache(), nullValue());
  }

  @Test
  public void testAutoBalanceShardKeyCacheIsStable() throws Exception {

    int numShards = 50;

    FlinkAutoBalancedShardKeyShardingFunction<String, String> fn =
        new FlinkAutoBalancedShardKeyShardingFunction<>(
            numShards / 2, numShards * 2, StringUtf8Coder.of());

    List<KV<String, String>> inputs = Lists.newArrayList();
    for (int i = 0; i < numShards * 100; i++) {
      inputs.add(KV.of("target/destination/1", UUID.randomUUID().toString()));
      inputs.add(KV.of("target/destination/2", UUID.randomUUID().toString()));
      inputs.add(KV.of("target/destination/3", UUID.randomUUID().toString()));
    }

    Map<KV<String, Integer>, ShardedKey<Integer>> generatedKeys = new HashMap<>();
    for (KV<String, String> input : inputs) {
      ShardedKey<Integer> shardKey = fn.assignShardKey(input.getKey(), input.getValue(), numShards);
      generatedKeys.put(KV.of(input.getKey(), shardKey.getShardNumber()), shardKey);
    }

    // let's create new sharding function instance, shuffle inputs and check if we generate same
    // shard keys
    fn =
        new FlinkAutoBalancedShardKeyShardingFunction<>(
            numShards / 2, numShards * 2, StringUtf8Coder.of());

    Collections.shuffle(inputs);
    for (KV<String, String> input : inputs) {
      ShardedKey<Integer> shardKey = fn.assignShardKey(input.getKey(), input.getValue(), numShards);
      ShardedKey<Integer> expectedShardKey =
          generatedKeys.get(KV.of(input.getKey(), shardKey.getShardNumber()));
      if (expectedShardKey != null) {
        assertThat(shardKey, equalTo(expectedShardKey));
      }
    }
  }

  @Test
  public void testAutoBalanceShardKeyCacheMaxSize() throws Exception {

    FlinkAutoBalancedShardKeyShardingFunction<String, String> fn =
        new FlinkAutoBalancedShardKeyShardingFunction<>(2, 2, StringUtf8Coder.of());

    for (int i = 0; i < FlinkAutoBalancedShardKeyShardingFunction.CACHE_MAX_SIZE * 2; i++) {
      fn.assignShardKey(UUID.randomUUID().toString(), "one", 2);
    }

    assertThat(
        fn.getCache().size(), equalTo(FlinkAutoBalancedShardKeyShardingFunction.CACHE_MAX_SIZE));
  }
}
