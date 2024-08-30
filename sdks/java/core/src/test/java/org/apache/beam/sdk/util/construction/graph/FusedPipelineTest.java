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
package org.apache.beam.sdk.util.construction.graph;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FusedPipeline}. */
@RunWith(JUnit4.class)
public class FusedPipelineTest implements Serializable {
  @Test
  public void testToProto() {
    Pipeline p = Pipeline.create();
    p.apply("impulse", Impulse.create())
        .apply("map", MapElements.into(TypeDescriptors.integers()).via(bytes -> bytes.length))
        .apply("key", WithKeys.of("foo"))
        .apply("gbk", GroupByKey.create())
        .apply("values", Values.create());

    RunnerApi.Pipeline protoPipeline = PipelineTranslation.toProto(p);
    checkState(
        protoPipeline
            .getRootTransformIdsList()
            .containsAll(ImmutableList.of("impulse", "map", "key", "gbk", "values")),
        "Unexpected Root Transform IDs %s",
        protoPipeline.getRootTransformIdsList());

    FusedPipeline fused = GreedyPipelineFuser.fuse(protoPipeline);
    checkState(
        fused.getRunnerExecutedTransforms().size() == 2,
        "Unexpected number of runner transforms %s",
        fused.getRunnerExecutedTransforms());
    checkState(
        fused.getFusedStages().size() == 2,
        "Unexpected number of fused stages %s",
        fused.getFusedStages());
    RunnerApi.Pipeline fusedPipelineProto = fused.toPipeline();

    assertThat(
        "Root Transforms should all be present in the Pipeline Components",
        fusedPipelineProto.getComponents().getTransformsMap().keySet(),
        hasItems(fusedPipelineProto.getRootTransformIdsList().toArray(new String[0])));
    assertThat(
        "Should contain Impulse, GroupByKey, and two Environment Stages",
        fusedPipelineProto.getRootTransformIdsCount(),
        equalTo(4));
    assertThat(fusedPipelineProto.getRootTransformIdsList(), hasItems("impulse", "gbk"));
    assertRootsInTopologicalOrder(fusedPipelineProto);
    // Since MapElements, WithKeys, and Values are all composites of a ParDo, we do prefix matching
    // instead of looking at the inside of their expansions
    assertThat(
        "Fused transforms should be present in the components",
        fusedPipelineProto.getComponents().getTransformsMap(),
        allOf(hasKey(startsWith("map")), hasKey(startsWith("key")), hasKey(startsWith("values"))));
    assertThat(
        "Fused transforms shouldn't be present in the root IDs",
        fusedPipelineProto.getRootTransformIdsList(),
        not(hasItems(startsWith("map"), startsWith("key"), startsWith("values"))));

    // The other components should be those of the original pipeline.
    assertThat(
        fusedPipelineProto.getComponents().getCodersMap(),
        equalTo(protoPipeline.getComponents().getCodersMap()));
    assertThat(
        fusedPipelineProto.getComponents().getWindowingStrategiesMap(),
        equalTo(protoPipeline.getComponents().getWindowingStrategiesMap()));
    assertThat(
        fusedPipelineProto.getComponents().getEnvironmentsMap(),
        equalTo(protoPipeline.getComponents().getEnvironmentsMap()));
    assertThat(
        fusedPipelineProto.getComponents().getPcollectionsMap(),
        equalTo(protoPipeline.getComponents().getPcollectionsMap()));
  }

  // For each transform in the root transforms, asserts that all consumed PCollections have been
  // produced, and no produced PCollection has been consumed
  private void assertRootsInTopologicalOrder(RunnerApi.Pipeline fusedProto) {
    Set<String> consumedPCollections = new HashSet<>();
    Set<String> producedPCollections = new HashSet<>();
    for (int i = 0; i < fusedProto.getRootTransformIdsCount(); i++) {
      PTransform rootTransform =
          fusedProto.getComponents().getTransformsOrThrow(fusedProto.getRootTransformIds(i));
      assertThat(
          String.format(
              "All %s consumed by %s must be produced before it",
              PCollection.class.getSimpleName(), fusedProto.getRootTransformIds(i)),
          producedPCollections,
          hasItems(rootTransform.getInputsMap().values().toArray(new String[0])));
      for (String consumed : consumedPCollections) {
        assertThat(
            String.format(
                "%s %s was consumed before all of its producers produced it",
                PCollection.class.getSimpleName(), consumed),
            rootTransform.getOutputsMap().values(),
            not(hasItem(consumed)));
      }
      consumedPCollections.addAll(rootTransform.getInputsMap().values());
      producedPCollections.addAll(rootTransform.getOutputsMap().values());
    }
  }
}
