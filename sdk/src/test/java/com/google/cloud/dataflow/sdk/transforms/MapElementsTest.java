/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

/**
 * Tests for {@link MapElements}.
 */
@RunWith(JUnit4.class)
public class MapElementsTest implements Serializable {

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  /**
   * Basic test of {@link MapElements} with a {@link SimpleFunction}.
   */
  @Test
  public void testMapBasic() throws Exception {
    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> output = pipeline
        .apply(Create.of(1, 2, 3))
        .apply(MapElements.via(new SimpleFunction<Integer, Integer>() {
          @Override
          public Integer apply(Integer input) {
            return -input;
          }
        }));

    DataflowAssert.that(output).containsInAnyOrder(-2, -1, -3);
    pipeline.run();
  }

  /**
   * Basic test of {@link MapElements} with a {@link SerializableFunction}. This style is
   * generally discouraged in Java 7, in favor of {@link SimpleFunction}.
   */
  @Test
  public void testMapBasicSerializableFunction() throws Exception {
    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> output = pipeline
        .apply(Create.of(1, 2, 3))
        .apply(MapElements.via(new SerializableFunction<Integer, Integer>() {
          @Override
          public Integer apply(Integer input) {
            return -input;
          }
        }).withOutputType(new TypeDescriptor<Integer>() {}));

    DataflowAssert.that(output).containsInAnyOrder(-2, -1, -3);
    pipeline.run();
  }

  /**
   * Tests that when built with a concrete subclass of {@link SimpleFunction}, the type descriptor
   * of the output reflects its static type.
   */
  @Test
  public void testSimpleFunctionOutputTypeDescriptor() throws Exception {
    Pipeline pipeline = TestPipeline.create();
    PCollection<String> output = pipeline
        .apply(Create.of("hello"))
        .apply(MapElements.via(new SimpleFunction<String, String>() {
          @Override
          public String apply(String input) {
            return input;
          }
        }));
    assertThat(output.getTypeDescriptor(),
        equalTo((TypeDescriptor<String>) new TypeDescriptor<String>() {}));
    assertThat(pipeline.getCoderRegistry().getDefaultCoder(output.getTypeDescriptor()),
        equalTo(pipeline.getCoderRegistry().getDefaultCoder(new TypeDescriptor<String>() {})));

    // Make sure the pipelien runs too
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
      return input.apply(MapElements.<KV<K, V>, KV<K, Void>>via(
          new SimpleFunction<KV<K, V>, KV<K, Void>>() {
            @Override
            public KV<K, Void> apply(KV<K, V> input) {
              return KV.of(input.getKey(), null);
            }
          }));
    }
  }
}
