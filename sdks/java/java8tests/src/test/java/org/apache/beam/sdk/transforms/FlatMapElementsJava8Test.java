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

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Java 8 Tests for {@link FlatMapElements}.
 */
@RunWith(JUnit4.class)
public class FlatMapElementsJava8Test implements Serializable {

  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  /**
   * Basic test of {@link FlatMapElements} with a lambda (which is instantiated as a
   * {@link SerializableFunction}).
   */
  @Test
  public void testFlatMapBasic() throws Exception {
    PCollection<Integer> output = pipeline
        .apply(Create.of(1, 2, 3))
        .apply(FlatMapElements
            // Note that the input type annotation is required.
            .into(TypeDescriptors.integers())
            .via((Integer i) -> ImmutableList.of(i, -i)));

    PAssert.that(output).containsInAnyOrder(1, 3, -1, -3, 2, -2);
    pipeline.run();
  }

  /**
   * Basic test of {@link FlatMapElements} with a method reference.
   */
  @Test
  public void testFlatMapMethodReference() throws Exception {

    PCollection<Integer> output = pipeline
        .apply(Create.of(1, 2, 3))
        .apply(FlatMapElements
            // Note that the input type annotation is required.
            .into(TypeDescriptors.integers())
            .via(new Negater()::numAndNegation));

    PAssert.that(output).containsInAnyOrder(1, 3, -1, -3, 2, -2);
    pipeline.run();
  }

  private static class Negater implements Serializable {
    public List<Integer> numAndNegation(int input) {
      return ImmutableList.of(input, -input);
    }
  }
}
