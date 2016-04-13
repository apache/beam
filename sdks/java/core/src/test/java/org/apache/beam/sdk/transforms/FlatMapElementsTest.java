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
package com.google.cloud.dataflow.sdk.transforms;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.PAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Tests for {@link FlatMapElements}.
 */
@RunWith(JUnit4.class)
public class FlatMapElementsTest implements Serializable {

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  /**
   * Basic test of {@link FlatMapElements} with a {@link SimpleFunction}.
   */
  @Test
  public void testFlatMapBasic() throws Exception {
    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> output = pipeline
        .apply(Create.of(1, 2, 3))

        // Note that FlatMapElements takes a SimpleFunction<InputT, ? extends Iterable<OutputT>>
        // so the use of List<Integer> here (as opposed to Iterable<Integer>) deliberately exercises
        // the use of an upper bound.
        .apply(FlatMapElements.via(new SimpleFunction<Integer, List<Integer>>() {
          @Override
          public List<Integer> apply(Integer input) {
            return ImmutableList.of(-input, input);
          }
        }));

    PAssert.that(output).containsInAnyOrder(1, -2, -1, -3, 2, 3);
    pipeline.run();
  }

  /**
   * Tests that when built with a concrete subclass of {@link SimpleFunction}, the type descriptor
   * of the output reflects its static type.
   */
  @Test
  public void testFlatMapFnOutputTypeDescriptor() throws Exception {
    Pipeline pipeline = TestPipeline.create();
    PCollection<String> output = pipeline
        .apply(Create.of("hello"))
        .apply(FlatMapElements.via(new SimpleFunction<String, Set<String>>() {
          @Override
          public Set<String> apply(String input) {
            return ImmutableSet.copyOf(input.split(""));
          }
        }));

    assertThat(output.getTypeDescriptor(),
        equalTo((TypeDescriptor<String>) new TypeDescriptor<String>() {}));
    assertThat(pipeline.getCoderRegistry().getDefaultCoder(output.getTypeDescriptor()),
        equalTo(pipeline.getCoderRegistry().getDefaultCoder(new TypeDescriptor<String>() {})));

    // Make sure the pipeline runs
    pipeline.run();
  }

  @Test
  public void testVoidValues() throws Exception {
    Pipeline pipeline = TestPipeline.create();
    pipeline
        .apply(Create.of("hello"))
        .apply(WithKeys.<String, String>of("k"))
        .apply(new VoidValues<String, String>() {});
    // Make sure the pipeline runs
    pipeline.run();
  }

  static class VoidValues<K, V>
      extends PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Void>>> {

    @Override
    public PCollection<KV<K, Void>> apply(PCollection<KV<K, V>> input) {
      return input.apply(FlatMapElements.<KV<K, V>, KV<K, Void>>via(
          new SimpleFunction<KV<K, V>, Iterable<KV<K, Void>>>() {
            @Override
            public Iterable<KV<K, Void>> apply(KV<K, V> input) {
              return Collections.singletonList(KV.<K, Void>of(input.getKey(), null));
            }
          }));
    }
  }
}
