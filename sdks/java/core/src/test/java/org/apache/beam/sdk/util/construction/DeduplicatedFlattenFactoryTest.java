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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

import org.apache.beam.sdk.Pipeline.PipelineVisitor.Defaults;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.construction.DeduplicatedFlattenFactory.FlattenWithoutDuplicateInputs;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PValues;
import org.apache.beam.sdk.values.TaggedPValue;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DeduplicatedFlattenFactory}. */
@RunWith(JUnit4.class)
public class DeduplicatedFlattenFactoryTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  private PCollection<String> first = pipeline.apply("FirstCreate", Create.of("one"));
  private PCollection<String> second = pipeline.apply("SecondCreate", Create.of("two"));
  private DeduplicatedFlattenFactory<String> factory = DeduplicatedFlattenFactory.create();

  @Test
  public void duplicatesInsertsMultipliers() {
    PTransform<PCollectionList<String>, PCollection<String>> replacement =
        new DeduplicatedFlattenFactory.FlattenWithoutDuplicateInputs<>();
    final PCollectionList<String> inputList =
        PCollectionList.of(first).and(second).and(first).and(first);
    inputList.apply(replacement);
    pipeline.traverseTopologically(
        new Defaults() {
          @Override
          public void visitPrimitiveTransform(TransformHierarchy.Node node) {
            if (node.getTransform() instanceof Flatten.PCollections) {
              assertThat(node.getInputs(), not(equalTo(inputList.expand())));
            }
          }
        });
  }

  @Test
  @Category(NeedsRunner.class)
  public void testOverride() {
    final PCollectionList<String> inputList =
        PCollectionList.of(first).and(second).and(first).and(first);
    PTransform<PCollectionList<String>, PCollection<String>> replacement =
        new FlattenWithoutDuplicateInputs<>();
    PCollection<String> flattened = inputList.apply(replacement);

    PAssert.that(flattened).containsInAnyOrder("one", "two", "one", "one");
    pipeline.run();
  }

  @Test
  public void outputMapping() {
    final PCollectionList<String> inputList =
        PCollectionList.of(first).and(second).and(first).and(first);
    PCollection<String> original = inputList.apply(Flatten.pCollections());
    PCollection<String> replacement = inputList.apply(new FlattenWithoutDuplicateInputs<>());

    assertThat(
        factory.mapOutputs(PValues.expandOutput(original), replacement),
        Matchers.hasEntry(
            replacement,
            ReplacementOutput.of(
                TaggedPValue.ofExpandedValue(original),
                TaggedPValue.ofExpandedValue(replacement))));
  }
}
