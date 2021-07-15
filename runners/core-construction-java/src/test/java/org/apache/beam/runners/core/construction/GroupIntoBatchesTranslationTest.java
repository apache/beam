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
package org.apache.beam.runners.core.construction;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.GroupIntoBatchesPayload;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValues;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link GroupIntoBatchesTranslation}. */
@RunWith(Parameterized.class)
public class GroupIntoBatchesTranslationTest {
  @Parameters(name = "{index}: {0}")
  public static Iterable<GroupIntoBatches<?, ?>> transform() {
    return ImmutableList.of(
        GroupIntoBatches.ofSize(5),
        GroupIntoBatches.ofSize(5).withMaxBufferingDuration(Duration.ZERO),
        GroupIntoBatches.ofSize(5).withMaxBufferingDuration(Duration.standardSeconds(10)),
        GroupIntoBatches.ofByteSize(10).withMaxBufferingDuration(Duration.standardSeconds(10)));
  }

  @Parameter(0)
  public GroupIntoBatches<String, Integer> groupIntoBatches;

  public static TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  public void testToProto() throws Exception {
    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("a", 2)));
    PCollection<KV<String, Iterable<Integer>>> output = input.apply(groupIntoBatches);

    AppliedPTransform<?, ?, GroupIntoBatches<String, Integer>> appliedTransform =
        AppliedPTransform.of(
            "foo",
            PValues.expandInput(input),
            PValues.expandOutput(output),
            groupIntoBatches,
            ResourceHints.create(),
            p);

    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    RunnerApi.FunctionSpec spec =
        PTransformTranslation.toProto(appliedTransform, components).getSpec();

    assertThat(spec.getUrn(), equalTo(PTransformTranslation.GROUP_INTO_BATCHES_URN));
    verifyPayload(
        groupIntoBatches.getBatchingParams(), GroupIntoBatchesPayload.parseFrom(spec.getPayload()));
  }

  @Test
  public void testWithShardedKeyToProto() throws Exception {
    PCollection<KV<String, Integer>> input =
        p.apply(Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("a", 2)));
    GroupIntoBatches<String, Integer>.WithShardedKey transform = groupIntoBatches.withShardedKey();
    PCollection<KV<ShardedKey<String>, Iterable<Integer>>> output = input.apply(transform);

    AppliedPTransform<?, ?, GroupIntoBatches<String, Integer>.WithShardedKey> appliedTransform =
        AppliedPTransform.of(
            "bar",
            PValues.expandInput(input),
            PValues.expandOutput(output),
            transform,
            ResourceHints.create(),
            p);

    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    RunnerApi.FunctionSpec spec =
        PTransformTranslation.toProto(appliedTransform, components).getSpec();

    assertThat(
        spec.getUrn(), equalTo(PTransformTranslation.GROUP_INTO_BATCHES_WITH_SHARDED_KEY_URN));
    verifyPayload(
        transform.getBatchingParams(), GroupIntoBatchesPayload.parseFrom(spec.getPayload()));
  }

  private void verifyPayload(
      GroupIntoBatches.BatchingParams<?> params, RunnerApi.GroupIntoBatchesPayload payload) {
    assertThat(payload.getBatchSize(), equalTo(params.getBatchSize()));
    assertThat(payload.getBatchSizeBytes(), equalTo(params.getBatchSizeBytes()));
    assertThat(
        payload.getMaxBufferingDurationMillis(),
        equalTo(params.getMaxBufferingDuration().getStandardSeconds() * 1000));
  }
}
