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

package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import org.apache.beam.runners.direct.CommittedResult.OutputType;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link CommittedResult}.
 */
@RunWith(JUnit4.class)
public class CommittedResultTest implements Serializable {

  @Rule
  public transient TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  private transient PCollection<Integer> created = p.apply(Create.of(1, 2));
  private transient AppliedPTransform<?, ?, ?> transform =
      AppliedPTransform.<PBegin, PDone, PTransform<PBegin, PDone>>of(
          "foo",
          p.begin().expand(),
          PDone.in(p).expand(),
          new PTransform<PBegin, PDone>() {
            @Override
            public PDone expand(PBegin begin) {
              throw new IllegalArgumentException("Should never be applied");
            }
          },
          p);
  private transient BundleFactory bundleFactory = ImmutableListBundleFactory.create();

  @Test
  public void getTransformExtractsFromResult() {
    CommittedResult result =
        CommittedResult.create(
            StepTransformResult.withoutHold(transform).build(),
            bundleFactory.createBundle(created).commit(Instant.now()),
            Collections.<DirectRunner.CommittedBundle<?>>emptyList(),
            EnumSet.noneOf(OutputType.class));

    assertThat(result.getTransform(), Matchers.<AppliedPTransform<?, ?, ?>>equalTo(transform));
  }

  @Test
  public void getUncommittedElementsEqualInput() {
    DirectRunner.CommittedBundle<Integer> bundle =
        bundleFactory.createBundle(created)
            .add(WindowedValue.valueInGlobalWindow(2))
            .commit(Instant.now());
    CommittedResult result =
        CommittedResult.create(
            StepTransformResult.withoutHold(transform).build(),
            bundle,
            Collections.<DirectRunner.CommittedBundle<?>>emptyList(),
            EnumSet.noneOf(OutputType.class));

    assertThat(result.getUnprocessedInputs(),
        Matchers.<DirectRunner.CommittedBundle<?>>equalTo(bundle));
  }

  @Test
  public void getUncommittedElementsNull() {
    CommittedResult result =
        CommittedResult.create(
            StepTransformResult.withoutHold(transform).build(),
            null,
            Collections.<DirectRunner.CommittedBundle<?>>emptyList(),
            EnumSet.noneOf(OutputType.class));

    assertThat(result.getUnprocessedInputs(), nullValue());
  }

  @Test
  public void getOutputsEqualInput() {
    List<? extends DirectRunner.CommittedBundle<?>> outputs =
        ImmutableList.of(bundleFactory.createBundle(PCollection.createPrimitiveOutputInternal(p,
            WindowingStrategy.globalDefault(),
            PCollection.IsBounded.BOUNDED)).commit(Instant.now()),
            bundleFactory.createBundle(PCollection.createPrimitiveOutputInternal(p,
                WindowingStrategy.globalDefault(),
                PCollection.IsBounded.UNBOUNDED)).commit(Instant.now()));
    CommittedResult result =
        CommittedResult.create(
            StepTransformResult.withoutHold(transform).build(),
            bundleFactory.createBundle(created).commit(Instant.now()),
            outputs,
            EnumSet.of(OutputType.BUNDLE, OutputType.PCOLLECTION_VIEW));

    assertThat(result.getOutputs(), Matchers.containsInAnyOrder(outputs.toArray()));
  }
}
