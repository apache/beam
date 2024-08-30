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

import java.io.Serializable;
import java.util.Map;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory.ReplacementOutput;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PValues;
import org.apache.beam.sdk.values.TaggedPValue;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SingleInputOutputOverrideFactory}. */
@RunWith(JUnit4.class)
public class SingleInputOutputOverrideFactoryTest implements Serializable {
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Rule
  public transient TestPipeline pipeline =
      TestPipeline.create().enableAbandonedNodeEnforcement(false);

  private transient SingleInputOutputOverrideFactory<
          PCollection<? extends Integer>, PCollection<Integer>, MapElements<Integer, Integer>>
      factory =
          new SingleInputOutputOverrideFactory<
              PCollection<? extends Integer>,
              PCollection<Integer>,
              MapElements<Integer, Integer>>() {
            @Override
            public PTransformReplacement<PCollection<? extends Integer>, PCollection<Integer>>
                getReplacementTransform(
                    AppliedPTransform<
                            PCollection<? extends Integer>,
                            PCollection<Integer>,
                            MapElements<Integer, Integer>>
                        transform) {
              return PTransformReplacement.of(
                  PTransformReplacements.getSingletonMainInput(transform),
                  transform.getTransform());
            }
          };

  private SimpleFunction<Integer, Integer> fn =
      new SimpleFunction<Integer, Integer>() {
        @Override
        public Integer apply(Integer input) {
          return input - 1;
        }
      };

  @Test
  public void testMapOutputs() {
    PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3));
    PCollection<Integer> output = input.apply("Map", MapElements.via(fn));
    PCollection<Integer> reappliedOutput = input.apply("ReMap", MapElements.via(fn));
    Map<PCollection<?>, ReplacementOutput> replacementMap =
        factory.mapOutputs(PValues.expandOutput(output), reappliedOutput);
    assertThat(
        replacementMap,
        Matchers.hasEntry(
            reappliedOutput,
            ReplacementOutput.of(
                TaggedPValue.ofExpandedValue(output),
                TaggedPValue.ofExpandedValue(reappliedOutput))));
  }

  @Test
  public void testMapOutputsMultipleOriginalOutputsFails() {
    PCollection<Integer> input = pipeline.apply(Create.of(1, 2, 3));
    PCollection<Integer> output = input.apply("Map", MapElements.via(fn));
    PCollection<Integer> reappliedOutput = input.apply("ReMap", MapElements.via(fn));
    thrown.expect(IllegalArgumentException.class);
    factory.mapOutputs(
        PValues.expandOutput(PCollectionList.of(output).and(input).and(reappliedOutput)),
        reappliedOutput);
  }
}
