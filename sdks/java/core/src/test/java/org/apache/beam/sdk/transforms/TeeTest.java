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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Multimaps;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for Tee. */
@RunWith(JUnit4.class)
public class TeeTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testTee() {
    List<String> elements = Arrays.asList("a", "b", "c");
    CollectToMemory<String> collector = new CollectToMemory<>();
    PCollection<String> output = p.apply(Create.of(elements)).apply(Tee.of(collector));

    PAssert.that(output).containsInAnyOrder(elements);
    p.run().waitUntilFinish();

    // Here we assert that this "sink" had the correct side effects.
    assertThat(collector.get(), containsInAnyOrder(elements.toArray(new String[3])));
  }

  private static class CollectToMemory<T> extends PTransform<PCollection<T>, PCollection<Void>> {

    private static final Multimap<UUID, Object> ALL_ELEMENTS =
        Multimaps.synchronizedMultimap(HashMultimap.<UUID, Object>create());

    UUID uuid = UUID.randomUUID();

    @Override
    public PCollection<Void> expand(PCollection<T> input) {
      return input.apply(
          ParDo.of(
              new DoFn<T, Void>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  ALL_ELEMENTS.put(uuid, c.element());
                }
              }));
    }

    @SuppressWarnings("unchecked")
    public Collection<T> get() {
      return (Collection<T>) ALL_ELEMENTS.get(uuid);
    }
  }
}
