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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

/**
 * Java 8 Tests for {@link Combine}.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class CombineJava8Test implements Serializable {

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
    Pipeline pipeline = TestPipeline.create();

    PCollection<Integer> output = pipeline
        .apply(Create.of(1, 2, 3, 4))
        .apply(Combine.globally(integers -> {
          int sum = 0;
          for (int i : integers) {
            sum += i;
          }
          return sum;
        }));

    DataflowAssert.that(output).containsInAnyOrder(10);
    pipeline.run();
  }

  /**
   * Tests creation of a global {@link Combine} via a Java 8 method reference.
   */
  @Test
  public void testCombineGloballyInstanceMethodReference() {
    Pipeline pipeline = TestPipeline.create();

    PCollection<Integer> output = pipeline
        .apply(Create.of(1, 2, 3, 4))
        .apply(Combine.globally(new Summer()::sum));

    DataflowAssert.that(output).containsInAnyOrder(10);
    pipeline.run();
  }

  /**
   * Tests creation of a per-key {@link Combine} via a Java 8 lambda.
   */
  @Test
  public void testCombinePerKeyLambda() {
    Pipeline pipeline = TestPipeline.create();

    PCollection<KV<String, Integer>> output = pipeline
        .apply(Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("a", 3), KV.of("c", 4)))
        .apply(Combine.perKey(integers -> {
          int sum = 0;
          for (int i : integers) {
            sum += i;
          }
          return sum;
        }));

    DataflowAssert.that(output).containsInAnyOrder(
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
    Pipeline pipeline = TestPipeline.create();

    PCollection<KV<String, Integer>> output = pipeline
        .apply(Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("a", 3), KV.of("c", 4)))
        .apply(Combine.perKey(new Summer()::sum));

    DataflowAssert.that(output).containsInAnyOrder(
        KV.of("a", 4),
        KV.of("b", 2),
        KV.of("c", 4));
    pipeline.run();
  }
}
