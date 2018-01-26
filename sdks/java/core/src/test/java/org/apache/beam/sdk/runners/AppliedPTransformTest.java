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

package org.apache.beam.sdk.runners;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import com.google.common.testing.EqualsTester;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Create.Values;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link AppliedPTransform}.
 */
@RunWith(JUnit4.class)
public class AppliedPTransformTest {
  @Test
  public void testEquals() {
    EqualsTester equalsTester = new EqualsTester();

    Pipeline disPipeline = Pipeline.create();
    Pipeline datPipeline = Pipeline.create();

    final Values<Integer> create = Create.of(1, 2, 3);
    PCollection<Integer> disInts = disPipeline.apply("Create", create);
    PCollection<Integer> datInts = datPipeline.apply("Create", create);

    AppliedPTransform<PBegin, PCollection<Integer>, Values<Integer>> disCreate =
        AppliedPTransform.of(
            "Create", PBegin.in(disPipeline).expand(), disInts.expand(), create, disPipeline);
    final AtomicReference<AppliedPTransform> nodeApplication = new AtomicReference<>();
    disPipeline.traverseTopologically(new PipelineVisitor.Defaults() {
      @Override
      public CompositeBehavior enterCompositeTransform(Node node) {
        if (node.getTransform() != null && node.getTransform().equals(create)) {
          nodeApplication.set(node.toAppliedPTransform(getPipeline()));
        }
        return CompositeBehavior.ENTER_TRANSFORM;
      }
    });

    AppliedPTransform<PBegin, PCollection<Integer>, Values<Integer>> datCreate =
        AppliedPTransform.of(
            "Create", PBegin.in(datPipeline).expand(), datInts.expand(), create, datPipeline);

    equalsTester.addEqualityGroup(disCreate, nodeApplication.get());
    equalsTester.addEqualityGroup(datCreate);

    // The two Create applications have the same name, transform, and inputs, but are
    // in different pipelines and therefore not equal
    assertThat(disCreate.getTransform(), equalTo(datCreate.getTransform()));
    assertThat(disCreate.getInputs(), equalTo(datCreate.getInputs()));
    assertThat(disCreate.getFullName(), equalTo(datCreate.getFullName()));

    IdentityPTransform firstIdentity = new IdentityPTransform();
    IdentityPTransform secondIdentity = new IdentityPTransform();

    AppliedPTransform firstIdentityApplication =
        AppliedPTransform.of(
            "Identity",
            Collections.<TupleTag<?>, PValue>emptyMap(),
            Collections.<TupleTag<?>, PValue>emptyMap(),
            firstIdentity,
            disPipeline);
    AppliedPTransform secondIdentityApplication =
        AppliedPTransform.of(
            "Identity",
            Collections.<TupleTag<?>, PValue>emptyMap(),
            Collections.<TupleTag<?>, PValue>emptyMap(),
            secondIdentity,
            disPipeline);
    AppliedPTransform otherIdentityApplication =
        AppliedPTransform.of(
            "OtherIdentity",
            Collections.<TupleTag<?>, PValue>emptyMap(),
            Collections.<TupleTag<?>, PValue>emptyMap(),
            firstIdentity,
            disPipeline);

    // Transforms with different application names are different regardless of their transform;
    // Transforms in the same pipeline with the same node name are the same
    assertThat(
        firstIdentityApplication.getTransform(),
        not(equalTo(secondIdentityApplication.getTransform())));
    assertThat(
        firstIdentityApplication.getTransform(),
        equalTo(otherIdentityApplication.getTransform()));

    equalsTester.addEqualityGroup(firstIdentityApplication, secondIdentityApplication);
    equalsTester.addEqualityGroup(otherIdentityApplication);

    equalsTester.testEquals();
  }

  private static class IdentityPTransform extends PTransform<PBegin, PDone> {
    @Override
    public PDone expand(PBegin input) {
      return PDone.in(input.getPipeline());
    }

    @Override
    public boolean equals(Object other) {
      return other == this;
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(this);
    }
  }
}
