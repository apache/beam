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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Assert;
import org.junit.Test;

/** Tests if overrides are properly applied. */
// TODO(https://github.com/apache/beam/issues/21230): Remove when new version of errorprone is
// released (2.11.0)
@SuppressWarnings("unused")
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

  @Test
  public void testStatefulParDoAfterCombineChaining() {
    final JobGraph stablePartitioning = getStatefulParDoAfterCombineChainingJobGraph(true);
    final JobGraph unstablePartitioning = getStatefulParDoAfterCombineChainingJobGraph(false);
    // We expect an extra shuffle stage for unstable partitioning.
    Assert.assertEquals(
        1,
        Iterables.size(unstablePartitioning.getVertices())
            - Iterables.size(stablePartitioning.getVertices()));
  }

  private JobGraph getStatefulParDoAfterCombineChainingJobGraph(boolean stablePartitioning) {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final FlinkStreamingPipelineTranslator translator =
        new FlinkStreamingPipelineTranslator(env, PipelineOptionsFactory.create(), true);
    final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    pipelineOptions.setRunner(FlinkRunner.class);
    final Pipeline pipeline = Pipeline.create(pipelineOptions);
    PCollection<KV<String, Long>> aggregate =
        pipeline
            .apply(Create.of("foo", "bar").withCoder(StringUtf8Coder.of()))
            .apply(Count.perElement());
    if (!stablePartitioning) {
      // When we insert any element-wise "map" operation between aggregation and stateful ParDo, we
      // can no longer assume that partitioning did not change, therefore we need an extra shuffle
      aggregate = aggregate.apply(ParDo.of(new StatelessIdentityDoFn<>()));
    }
    aggregate.apply(ParDo.of(new StatefulNoopDoFn<>()));
    translator.translate(pipeline);
    return env.getStreamGraph().getJobGraph();
  }

  @Test
  public void testStatefulParDoAfterGroupByKeyChaining() {
    final JobGraph stablePartitioning = getStatefulParDoAfterGroupByKeyChainingJobGraph(true);
    final JobGraph unstablePartitioning = getStatefulParDoAfterGroupByKeyChainingJobGraph(false);
    // We expect an extra shuffle stage for unstable partitioning.
    Assert.assertEquals(
        1,
        Iterables.size(unstablePartitioning.getVertices())
            - Iterables.size(stablePartitioning.getVertices()));
  }

  private JobGraph getStatefulParDoAfterGroupByKeyChainingJobGraph(boolean stablePartitioning) {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final FlinkStreamingPipelineTranslator translator =
        new FlinkStreamingPipelineTranslator(env, PipelineOptionsFactory.create(), true);
    final PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
    pipelineOptions.setRunner(FlinkRunner.class);
    final Pipeline pipeline = Pipeline.create(pipelineOptions);
    PCollection<KV<String, Iterable<Long>>> aggregate =
        pipeline
            .apply(
                Create.of(KV.of("foo", 1L), KV.of("bar", 1L))
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of())))
            .apply(GroupByKey.create());
    if (!stablePartitioning) {
      // When we insert any element-wise "map" operation between aggregation and stateful ParDo, we
      // can no longer assume that partitioning did not change, therefore we need an extra shuffle
      aggregate = aggregate.apply(ParDo.of(new StatelessIdentityDoFn<>()));
    }
    aggregate.apply(ParDo.of(new StatefulNoopDoFn<>()));
    translator.translate(pipeline);
    return env.getStreamGraph().getJobGraph();
  }

  private static class StatelessIdentityDoFn<KeyT, ValueT>
      extends DoFn<KV<KeyT, ValueT>, KV<KeyT, ValueT>> {

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      ctx.output(ctx.element());
    }
  }

  private static class StatefulNoopDoFn<KeyT, ValueT> extends DoFn<KV<KeyT, ValueT>, Void> {

    @TimerId("my-timer")
    private final TimerSpec myTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void processElement() {
      // noop
    }

    @OnTimer("my-timer")
    public void onMyTimer() {
      // noop
    }
  }
}
