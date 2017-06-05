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

import java.io.Serializable;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Java 8 tests for {@link MapElements}.
 */
@RunWith(JUnit4.class)
public class MapElementsJava8Test implements Serializable {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  /**
   * Basic test of {@link MapElements} with a lambda (which is instantiated as a {@link
   * SerializableFunction}).
   */
  @Test
  public void testMapLambda() throws Exception {

    PCollection<Integer> output = pipeline
        .apply(Create.of(1, 2, 3))
        .apply(MapElements
            // Note that the type annotation is required.
            .into(TypeDescriptors.integers())
            .via((Integer i) -> i * 2));

    PAssert.that(output).containsInAnyOrder(6, 2, 4);
    pipeline.run();
  }

  /**
   * Basic test of {@link MapElements} with a lambda wrapped into a {@link SimpleFunction} to
   * remember its type.
   */
  @Test
  public void testMapWrappedLambda() throws Exception {

    PCollection<Integer> output =
        pipeline
            .apply(Create.of(1, 2, 3))
            .apply(
                MapElements
                    .via(new SimpleFunction<Integer, Integer>((Integer i) -> i * 2) {}));

    PAssert.that(output).containsInAnyOrder(6, 2, 4);
    pipeline.run();
  }

  /**
   * Basic test of {@link MapElements} with a method reference.
   */
  @Test
  public void testMapMethodReference() throws Exception {

    PCollection<Integer> output = pipeline
        .apply(Create.of(1, 2, 3))
        .apply(MapElements
            // Note that the type annotation is required.
            .into(TypeDescriptors.integers())
            .via(new Doubler()::doubleIt));

    PAssert.that(output).containsInAnyOrder(6, 2, 4);
    pipeline.run();
  }

  private static class Doubler implements Serializable {
    public int doubleIt(int val) {
      return val * 2;
    }
  }
}
