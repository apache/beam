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
package org.apache.beam.runners.direct.portable;

import static org.junit.Assert.assertThat;

import java.io.Serializable;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.IsBounded.Enum;
import org.apache.beam.runners.core.construction.graph.PipelineNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.runners.direct.portable.CommittedResult.OutputType;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CommittedResult}. */
@RunWith(JUnit4.class)
public class CommittedResultTest implements Serializable {

  @Rule
  public transient TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  private transient PCollectionNode created =
      PipelineNode.pCollection(
          "created", RunnerApi.PCollection.newBuilder().setUniqueName("created").build());
  private transient PTransformNode transform =
      PipelineNode.pTransform("foo", RunnerApi.PTransform.getDefaultInstance());
  private transient BundleFactory bundleFactory = ImmutableListBundleFactory.create();

  @Test
  public void getTransformExtractsFromResult() {
    CommittedResult<PTransformNode> result =
        CommittedResult.create(
            StepTransformResult.withoutHold(transform).build(),
            Optional.absent(),
            Collections.emptyList(),
            EnumSet.noneOf(OutputType.class));

    assertThat(result.getExecutable(), Matchers.equalTo(transform));
  }

  @Test
  public void getUncommittedElementsEqualInput() {
    CommittedBundle<Integer> bundle =
        bundleFactory
            .<Integer>createBundle(created)
            .add(WindowedValue.valueInGlobalWindow(2))
            .commit(Instant.now());
    CommittedResult<PTransformNode> result =
        CommittedResult.create(
            StepTransformResult.withoutHold(transform).build(),
            Optional.of(bundle),
            Collections.emptyList(),
            EnumSet.noneOf(OutputType.class));

    assertThat(result.getUnprocessedInputs().get(), Matchers.equalTo(bundle));
  }

  @Test
  public void getUncommittedElementsNull() {
    CommittedResult<PTransformNode> result =
        CommittedResult.create(
            StepTransformResult.withoutHold(transform).build(),
            Optional.absent(),
            Collections.emptyList(),
            EnumSet.noneOf(OutputType.class));

    assertThat(result.getUnprocessedInputs(), Matchers.equalTo(Optional.absent()));
  }

  @Test
  public void getOutputsEqualInput() {
    List<? extends CommittedBundle<Integer>> outputs =
        ImmutableList.of(
            bundleFactory
                .<Integer>createBundle(
                    PipelineNode.pCollection(
                        "bounded",
                        RunnerApi.PCollection.newBuilder()
                            .setUniqueName("bounded")
                            .setIsBounded(Enum.BOUNDED)
                            .build()))
                .commit(Instant.now()),
            bundleFactory
                .<Integer>createBundle(
                    PipelineNode.pCollection(
                        "unbounded",
                        RunnerApi.PCollection.newBuilder()
                            .setUniqueName("unbounded")
                            .setIsBounded(Enum.UNBOUNDED)
                            .build()))
                .commit(Instant.now()));
    CommittedResult<PTransformNode> result =
        CommittedResult.create(
            StepTransformResult.withoutHold(transform).build(),
            Optional.absent(),
            outputs,
            EnumSet.of(OutputType.BUNDLE, OutputType.PCOLLECTION_VIEW));

    assertThat(result.getOutputs(), Matchers.containsInAnyOrder(outputs.toArray()));
  }
}
