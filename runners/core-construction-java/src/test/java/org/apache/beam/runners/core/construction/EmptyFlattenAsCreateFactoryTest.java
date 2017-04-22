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

import static org.hamcrest.Matchers.emptyIterable;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.PTransformReplacement;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Flatten.PCollections;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;
import org.apache.beam.sdk.values.TupleTag;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link EmptyFlattenAsCreateFactory}.
 */
@RunWith(JUnit4.class)
public class EmptyFlattenAsCreateFactoryTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();

  private EmptyFlattenAsCreateFactory<Long> factory = EmptyFlattenAsCreateFactory.instance();

  @Test
  public void getInputEmptySucceeds() {
    PTransformReplacement<PCollectionList<Long>, PCollection<Long>> replacement =
        factory.getReplacementTransform(
            AppliedPTransform.<PCollectionList<Long>, PCollection<Long>, PCollections<Long>>of(
                "nonEmptyInput",
                Collections.<TupleTag<?>, PValue>emptyMap(),
                Collections.<TupleTag<?>, PValue>emptyMap(),
                Flatten.<Long>pCollections(),
                pipeline));
    assertThat(replacement.getInput().getAll(), emptyIterable());
  }

  @Test
  public void getInputNonEmptyThrows() {
    PCollectionList<Long> nonEmpty =
        PCollectionList.of(pipeline.apply(CountingInput.unbounded()))
            .and(pipeline.apply(CountingInput.upTo(100L)));
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(nonEmpty.expand().toString());
    thrown.expectMessage(EmptyFlattenAsCreateFactory.class.getSimpleName());
    factory.getReplacementTransform(
        AppliedPTransform.<PCollectionList<Long>, PCollection<Long>, Flatten.PCollections<Long>>of(
            "nonEmptyInput",
            nonEmpty.expand(),
            Collections.<TupleTag<?>, PValue>emptyMap(),
            Flatten.<Long>pCollections(),
            pipeline));
  }

  @Test
  public void mapOutputsSucceeds() {
    PCollection<Long> original = pipeline.apply("Original", CountingInput.unbounded());
    PCollection<Long> replacement = pipeline.apply("Replacement", CountingInput.unbounded());
    Map<PValue, ReplacementOutput> mapping = factory.mapOutputs(original.expand(), replacement);

    assertThat(
        mapping,
        Matchers.<PValue, ReplacementOutput>hasEntry(
            replacement,
            ReplacementOutput.of(
                TaggedPValue.ofExpandedValue(original),
                TaggedPValue.ofExpandedValue(replacement))));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testOverride() {
    PCollectionList<Long> empty = PCollectionList.empty(pipeline);
    PCollection<Long> emptyFlattened =
        empty.apply(
            factory
                .getReplacementTransform(
                    AppliedPTransform
                        .<PCollectionList<Long>, PCollection<Long>, Flatten.PCollections<Long>>of(
                            "nonEmptyInput",
                            Collections.<TupleTag<?>, PValue>emptyMap(),
                            Collections.<TupleTag<?>, PValue>emptyMap(),
                            Flatten.<Long>pCollections(),
                            pipeline))
                .getTransform());
    PAssert.that(emptyFlattened).empty();
    pipeline.run();
  }
}
