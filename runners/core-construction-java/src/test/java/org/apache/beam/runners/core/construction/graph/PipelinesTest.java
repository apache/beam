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

package org.apache.beam.runners.core.construction.graph;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Coder;
import org.apache.beam.model.pipeline.v1.RunnerApi.Components;
import org.apache.beam.model.pipeline.v1.RunnerApi.Environment;
import org.apache.beam.model.pipeline.v1.RunnerApi.PTransform;
import org.apache.beam.model.pipeline.v1.RunnerApi.WindowingStrategy;
import org.apache.beam.runners.core.construction.PipelineTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Pipelines}. */
@RunWith(JUnit4.class)
public class PipelinesTest implements Serializable {
  @Test
  public void retainOnlyPrimitivesWithOnlyPrimitivesUnchanged() {
    Pipeline p = Pipeline.create();
    p.apply("Read", Read.from(CountingSource.unbounded()))
        .apply(
            "multi-do",
            ParDo.of(
                    new DoFn<Long, Long>() {
                      @ProcessElement
                      public void process(ProcessContext ctxt) {
                        ctxt.output(ctxt.element() + 1L);
                      }
                    })
                .withOutputTags(new TupleTag<>(), TupleTagList.empty()));

    Components originalComponents = PipelineTranslation.toProto(p).getComponents();
    Components primitiveComponents = Pipelines.retainOnlyPrimitives(originalComponents);

    assertThat(primitiveComponents, equalTo(originalComponents));
  }

  @Test
  public void retainOnlyPrimitivesComposites() {
    Pipeline p = Pipeline.create();
    p.apply(
        new org.apache.beam.sdk.transforms.PTransform<PBegin, PCollection<Long>>() {
          @Override
          public PCollection<Long> expand(PBegin input) {
            return input
                .apply(GenerateSequence.from(2L))
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(5L))))
                .apply(MapElements.into(TypeDescriptors.longs()).via(l -> l + 1));
          }
        });

    Components originalComponents = PipelineTranslation.toProto(p).getComponents();
    Components primitiveComponents = Pipelines.retainOnlyPrimitives(originalComponents);

    // Read, Window.Assign, ParDo. This will need to be updated if the expansions change.
    assertThat(primitiveComponents.getTransformsCount(), equalTo(3));
    for (Map.Entry<String, PTransform> transformEntry :
        primitiveComponents.getTransformsMap().entrySet()) {
      assertThat(
          originalComponents.getTransformsMap(),
          hasEntry(transformEntry.getKey(), transformEntry.getValue()));
    }

    // Other components should be unchanged
    assertThat(
        primitiveComponents.getPcollectionsCount(),
        equalTo(originalComponents.getPcollectionsCount()));
    assertThat(
        primitiveComponents.getWindowingStrategiesCount(),
        equalTo(originalComponents.getWindowingStrategiesCount()));
    assertThat(
        primitiveComponents.getCodersCount(),
        equalTo(originalComponents.getCodersCount()));
    assertThat(
        primitiveComponents.getEnvironmentsCount(),
        equalTo(originalComponents.getEnvironmentsCount()));
  }

  /**
   * This method doesn't do any pruning for reachability, but this may not require a test.
   */
  @Test
  public void retainOnlyPrimitivesIgnoresUnreachableNodes() {
    Pipeline p = Pipeline.create();
    p.apply(
        new org.apache.beam.sdk.transforms.PTransform<PBegin, PCollection<Long>>() {
          @Override
          public PCollection<Long> expand(PBegin input) {
            return input
                .apply(GenerateSequence.from(2L))
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(5L))))
                .apply(MapElements.into(TypeDescriptors.longs()).via(l -> l + 1));
          }
        });

    Components augmentedComponents =
        PipelineTranslation.toProto(p)
            .getComponents()
            .toBuilder()
            .putCoders("extra-coder", Coder.getDefaultInstance())
            .putWindowingStrategies(
                "extra-windowing-strategy", WindowingStrategy.getDefaultInstance())
            .putEnvironments("extra-env", Environment.getDefaultInstance())
            .putPcollections("extra-pc", RunnerApi.PCollection.getDefaultInstance())
            .build();
    Components primitiveComponents = Pipelines.retainOnlyPrimitives(augmentedComponents);

    // Other components should be unchanged
    assertThat(
        primitiveComponents.getPcollectionsCount(),
        equalTo(augmentedComponents.getPcollectionsCount()));
    assertThat(
        primitiveComponents.getWindowingStrategiesCount(),
        equalTo(augmentedComponents.getWindowingStrategiesCount()));
    assertThat(primitiveComponents.getCodersCount(), equalTo(augmentedComponents.getCodersCount()));
    assertThat(
        primitiveComponents.getEnvironmentsCount(),
        equalTo(augmentedComponents.getEnvironmentsCount()));
  }
}
