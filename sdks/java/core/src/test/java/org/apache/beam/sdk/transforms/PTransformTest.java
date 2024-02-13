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
package org.apache.beam.sdk.transforms;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.sdk.values.TypeDescriptors.integers;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PTransform} base class. */
@RunWith(JUnit4.class)
public class PTransformTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testPopulateDisplayDataDefaultBehavior() {
    PTransform<PCollection<String>, PCollection<String>> transform =
        new PTransform<PCollection<String>, PCollection<String>>() {
          @Override
          public PCollection<String> expand(PCollection<String> begin) {
            throw new IllegalArgumentException("Should never be applied");
          }
        };
    DisplayData displayData = DisplayData.from(transform);
    assertThat(displayData.items(), empty());
  }

  @Test
  public void testSetDisplayData() {
    PTransform<PCollection<String>, PCollection<String>> transform =
        new PTransform<PCollection<String>, PCollection<String>>() {
          @Override
          public PCollection<String> expand(PCollection<String> begin) {
            throw new IllegalArgumentException("Should never be applied");
          }
        };
    transform.setDisplayData(
        ImmutableList.of(DisplayData.item("key1", "value1"), DisplayData.item("key2", 2L)));

    final DisplayData displayData = DisplayData.from(transform);
    assertThat(displayData.items(), hasSize(2));
    assertThat(displayData, hasDisplayItem("key1", "value1"));
    assertThat(displayData, hasDisplayItem("key2", 2L));
  }

  @Test
  public void testNamedCompose() {
    PTransform<PCollection<Integer>, PCollection<Integer>> composed =
        PTransform.compose("MyName", (PCollection<Integer> numbers) -> numbers);
    assertEquals("MyName", composed.name);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testComposeBasicSerializableFunction() throws Exception {
    PCollection<Integer> output =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(
                PTransform.compose(
                    (PCollection<Integer> numbers) -> {
                      PCollection<Integer> inverted =
                          numbers.apply(MapElements.into(integers()).via(input -> -input));
                      return PCollectionList.of(numbers)
                          .and(inverted)
                          .apply(Flatten.pCollections());
                    }));

    PAssert.that(output).containsInAnyOrder(-2, -1, -3, 2, 1, 3);
    pipeline.run();
  }
}
