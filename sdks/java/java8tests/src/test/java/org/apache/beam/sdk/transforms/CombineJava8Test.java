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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

import com.google.common.collect.Iterables;
import java.io.Serializable;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Java 8 Tests for {@link Combine}.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class CombineJava8Test implements Serializable {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  /**
   * Class for use in testing use of Java 8 method references.
   */
  private static class Summer implements Serializable {
    public int sum(Iterable<Integer> integers) {
      int sum = 0;
      for (int i : integers) {
        sum += i;
      }
      return sum;
    }
  }

  /**
   * Tests creation of a global {@link Combine} via Java 8 lambda.
   */
  @Test
  public void testCombineGloballyLambda() {

    PCollection<Integer> output = pipeline
        .apply(Create.of(1, 2, 3, 4))
        .apply(Combine.globally(integers -> {
          int sum = 0;
          for (int i : integers) {
            sum += i;
          }
          return sum;
        }));

    PAssert.that(output).containsInAnyOrder(10);
    pipeline.run();
  }

  /**
   * Tests creation of a global {@link Combine} via a Java 8 method reference.
   */
  @Test
  public void testCombineGloballyInstanceMethodReference() {

    PCollection<Integer> output = pipeline
        .apply(Create.of(1, 2, 3, 4))
        .apply(Combine.globally(new Summer()::sum));

    PAssert.that(output).containsInAnyOrder(10);
    pipeline.run();
  }

  /**
   * Tests creation of a per-key {@link Combine} via a Java 8 lambda.
   */
  @Test
  public void testCombinePerKeyLambda() {

    PCollection<KV<String, Integer>> output = pipeline
        .apply(Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("a", 3), KV.of("c", 4)))
        .apply(Combine.perKey(integers -> {
          int sum = 0;
          for (int i : integers) {
            sum += i;
          }
          return sum;
        }));

    PAssert.that(output).containsInAnyOrder(
        KV.of("a", 4),
        KV.of("b", 2),
        KV.of("c", 4));
    pipeline.run();
  }

  /**
   * Tests creation of a per-key {@link Combine} via a Java 8 method reference.
   */
  @Test
  public void testCombinePerKeyInstanceMethodReference() {

    PCollection<KV<String, Integer>> output = pipeline
        .apply(Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("a", 3), KV.of("c", 4)))
        .apply(Combine.perKey(new Summer()::sum));

    PAssert.that(output).containsInAnyOrder(
        KV.of("a", 4),
        KV.of("b", 2),
        KV.of("c", 4));
    pipeline.run();
  }

  /**
   * Tests that we can serialize {@link Combine.CombineFn CombineFns} constructed from a lambda.
   * Lambdas can be problematic because the {@link Class} object is synthetic and cannot be
   * deserialized.
   */
  @Test
  public void testLambdaSerialization() {
    SerializableFunction<Iterable<Object>, Object> combiner = xs -> Iterables.getFirst(xs, 0);

    boolean lambdaClassSerializationThrows;
    try {
      SerializableUtils.clone(combiner.getClass());
      lambdaClassSerializationThrows = false;
    } catch (IllegalArgumentException e) {
      // Expected
      lambdaClassSerializationThrows = true;
    }
    Assume.assumeTrue("Expected lambda class serialization to fail. "
        + "If it's fixed, we can remove special behavior in Combine.",
        lambdaClassSerializationThrows);


    Combine.Globally<?, ?> combine = Combine.globally(combiner);
    SerializableUtils.clone(combine); // should not throw.
  }

  @Test
  public void testLambdaDisplayData() {
    Combine.Globally<?, ?> combine = Combine.globally(xs -> Iterables.getFirst(xs, 0));
    DisplayData displayData = DisplayData.from(combine);
    assertThat(displayData.items(), not(empty()));
  }
}
