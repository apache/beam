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
package org.apache.beam.sdk.util.construction;

import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TransformInputs}. */
@RunWith(JUnit4.class)
public class TransformInputsTest {
  @Rule public TestPipeline pipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void nonAdditionalInputsWithNoInputSucceeds() {
    AppliedPTransform<PInput, POutput, TestTransform> transform =
        AppliedPTransform.of(
            "input-free",
            Collections.emptyMap(),
            Collections.emptyMap(),
            new TestTransform(),
            ResourceHints.create(),
            pipeline);

    assertThat(TransformInputs.nonAdditionalInputs(transform), Matchers.empty());
  }

  @Test
  public void nonAdditionalInputsWithOneMainInputSucceeds() {
    PCollection<Long> input = pipeline.apply(GenerateSequence.from(1L));
    AppliedPTransform<PInput, POutput, TestTransform> transform =
        AppliedPTransform.of(
            "input-single",
            Collections.singletonMap(new TupleTag<Long>() {}, input),
            Collections.emptyMap(),
            new TestTransform(),
            ResourceHints.create(),
            pipeline);

    assertThat(TransformInputs.nonAdditionalInputs(transform), Matchers.containsInAnyOrder(input));
  }

  @Test
  public void nonAdditionalInputsWithMultipleNonAdditionalInputsSucceeds() {
    Map<TupleTag<?>, PCollection<?>> allInputs = new HashMap<>();
    PCollection<Integer> mainInts = pipeline.apply("MainInput", Create.of(12, 3));
    allInputs.put(new TupleTag<Integer>() {}, mainInts);
    PCollection<Void> voids = pipeline.apply("VoidInput", Create.empty(VoidCoder.of()));
    allInputs.put(new TupleTag<Void>() {}, voids);
    AppliedPTransform<PInput, POutput, TestTransform> transform =
        AppliedPTransform.of(
            "additional-free",
            allInputs,
            Collections.emptyMap(),
            new TestTransform(),
            ResourceHints.create(),
            pipeline);

    assertThat(
        TransformInputs.nonAdditionalInputs(transform),
        Matchers.containsInAnyOrder(voids, mainInts));
  }

  @Test
  public void nonAdditionalInputsWithAdditionalInputsSucceeds() {
    Map<TupleTag<?>, PValue> additionalInputs = new HashMap<>();
    additionalInputs.put(new TupleTag<String>() {}, pipeline.apply(Create.of("1, 2", "3")));
    additionalInputs.put(new TupleTag<Long>() {}, pipeline.apply(GenerateSequence.from(3L)));

    Map<TupleTag<?>, PCollection<?>> allInputs = new HashMap<>();
    PCollection<Integer> mainInts = pipeline.apply("MainInput", Create.of(12, 3));
    allInputs.put(new TupleTag<Integer>() {}, mainInts);
    PCollection<Void> voids = pipeline.apply("VoidInput", Create.empty(VoidCoder.of()));
    allInputs.put(new TupleTag<Void>() {}, voids);
    allInputs.putAll((Map) additionalInputs);

    AppliedPTransform<PInput, POutput, TestTransform> transform =
        AppliedPTransform.of(
            "additional",
            allInputs,
            Collections.emptyMap(),
            new TestTransform(additionalInputs),
            ResourceHints.create(),
            pipeline);

    assertThat(
        TransformInputs.nonAdditionalInputs(transform),
        Matchers.containsInAnyOrder(mainInts, voids));
  }

  @Test
  public void nonAdditionalInputsWithOnlyAdditionalInputsThrows() {
    Map<TupleTag<?>, PCollection<?>> additionalInputs = new HashMap<>();
    additionalInputs.put(new TupleTag<String>() {}, pipeline.apply(Create.of("1, 2", "3")));
    additionalInputs.put(new TupleTag<Long>() {}, pipeline.apply(GenerateSequence.from(3L)));

    AppliedPTransform<PInput, POutput, TestTransform> transform =
        AppliedPTransform.of(
            "additional-only",
            additionalInputs,
            Collections.emptyMap(),
            new TestTransform((Map) additionalInputs),
            ResourceHints.create(),
            pipeline);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("at least one");
    TransformInputs.nonAdditionalInputs(transform);
  }

  private static class TestTransform extends PTransform<PInput, POutput> {
    private final Map<TupleTag<?>, PValue> additionalInputs;

    private TestTransform() {
      this(Collections.emptyMap());
    }

    private TestTransform(Map<TupleTag<?>, PValue> additionalInputs) {
      this.additionalInputs = additionalInputs;
    }

    @Override
    public POutput expand(PInput input) {
      return PDone.in(input.getPipeline());
    }

    @Override
    public Map<TupleTag<?>, PValue> getAdditionalInputs() {
      return additionalInputs;
    }
  }
}
