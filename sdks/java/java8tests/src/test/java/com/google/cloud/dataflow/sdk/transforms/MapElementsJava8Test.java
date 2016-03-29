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
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

/**
 * Java 8 tests for {@link MapElements}.
 */
@RunWith(JUnit4.class)
public class MapElementsJava8Test implements Serializable {

  /**
   * Basic test of {@link MapElements} with a lambda (which is instantiated as a
   * {@link SerializableFunction}).
   */
  @Test
  public void testMapBasic() throws Exception {
    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> output = pipeline
        .apply(Create.of(1, 2, 3))
        .apply(MapElements
            // Note that the type annotation is required (for Java, not for Dataflow)
            .via((Integer i) -> i * 2)
            .withOutputType(new TypeDescriptor<Integer>() {}));

    DataflowAssert.that(output).containsInAnyOrder(6, 2, 4);
    pipeline.run();
  }

  /**
   * Basic test of {@link MapElements} with a method reference.
   */
  @Test
  public void testMapMethodReference() throws Exception {
    Pipeline pipeline = TestPipeline.create();
    PCollection<Integer> output = pipeline
        .apply(Create.of(1, 2, 3))
        .apply(MapElements
            // Note that the type annotation is required (for Java, not for Dataflow)
            .via(new Doubler()::doubleIt)
            .withOutputType(new TypeDescriptor<Integer>() {}));

    DataflowAssert.that(output).containsInAnyOrder(6, 2, 4);
    pipeline.run();
  }

  private static class Doubler implements Serializable {
    public int doubleIt(int val) {
      return val * 2;
    }
  }
}
