/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.inprocess;

import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PBegin;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.common.collect.ImmutableList;

import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link CommittedResult}.
 */
@RunWith(JUnit4.class)
public class CommittedResultTest implements Serializable {
  private transient TestPipeline p = TestPipeline.create();
  private transient AppliedPTransform<?, ?, ?> transform =
      AppliedPTransform.of("foo", p.begin(), PDone.in(p), new PTransform<PBegin, PDone>() {
      });
  private transient BundleFactory bundleFactory = InProcessBundleFactory.create();

  @Test
  public void getTransformExtractsFromResult() {
    CommittedResult result =
        CommittedResult.create(StepTransformResult.withoutHold(transform).build(),
            Collections.<InProcessPipelineRunner.CommittedBundle<?>>emptyList());

    assertThat(result.getTransform(), Matchers.<AppliedPTransform<?, ?, ?>>equalTo(transform));
  }

  @Test
  public void getOutputsEqualInput() {
    List<? extends InProcessPipelineRunner.CommittedBundle<?>> outputs =
        ImmutableList.of(bundleFactory.createRootBundle(PCollection.createPrimitiveOutputInternal(p,
            WindowingStrategy.globalDefault(),
            PCollection.IsBounded.BOUNDED)).commit(Instant.now()),
            bundleFactory.createRootBundle(PCollection.createPrimitiveOutputInternal(p,
                WindowingStrategy.globalDefault(),
                PCollection.IsBounded.UNBOUNDED)).commit(Instant.now()));
    CommittedResult result =
        CommittedResult.create(StepTransformResult.withoutHold(transform).build(), outputs);

    assertThat(result.getOutputs(), Matchers.containsInAnyOrder(outputs.toArray()));
  }
}
