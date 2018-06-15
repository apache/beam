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
package org.apache.beam.sdk.extensions.euphoria.beam;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.Dataset;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.ListDataSource;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.CountByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

/**
 * A group of test focused at {@link BeamPTransform}.
 */
public class BeamPTransformTest implements Serializable {

  private static final String BASE_STRING =
      "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Curabitur et imperdiet nulla,"
          + " vulputate luctus risus. In sed suscipit purus. Curabitur dui eros, eleifend sed "
          + "dignissim eget, euismod sed lorem.";
  @Rule
  public final transient TestPipeline pipeline = TestPipeline.create();


  @Test
  public void basicBeamTransformTest() {

    final List<String> words = Arrays.asList(BASE_STRING.split(" "));
    final List<String> upperCaseWords = Arrays.asList(BASE_STRING.toUpperCase().split(" "));

    final PCollection<String> pCollection =
        pipeline
            .apply("Create", Create.of(words).withCoder(StringUtf8Coder.of()))
            .apply("To-UpperCase", BeamPTransform.of(
                input -> MapElements
                    .of(input)
                    .using(s -> s.toUpperCase())
                    .output()
            ));

    PAssert.that(pCollection).containsInAnyOrder(upperCaseWords);

    pipeline.run();
  }

  @Test
  public void testChainedOperations() {

    String inStr = "a b c a a c x";
    final List<String> words = Arrays.asList(inStr.split(" "));

    final PCollection<Pair<String, Long>> pCollection =
        pipeline
            .apply("Create", Create.of(words).withCoder(StringUtf8Coder.of()))
            .apply("To-UpperCase", BeamPTransform.of((Dataset<String> input) -> {
                  Dataset<String> upperCase = MapElements
                      .of(input)
                      .using(s -> s.toUpperCase())
                      .output();

                  return CountByKey
                      .of(upperCase)
                      .keyBy(e -> e)
                      .output();
                }
            ));

    PAssert.that(pCollection).containsInAnyOrder(Pair.of("A", 3L), Pair.of("B", 1L),
        Pair.of("C", 2L), Pair.of("X", 1L));

    pipeline.run();
  }

  @Test
  public void testBeamTransformWhenFlowIsExecuted() {

    final List<String> words = Arrays.asList(BASE_STRING.split(" "));
    final List<String> upperCaseWords = Arrays.asList(BASE_STRING.toUpperCase().split(" "));

    final PCollection<String> pCollection =
        pipeline
            .apply("Create", Create.of(words).withCoder(StringUtf8Coder.of()))
            .apply("To-UpperCase", BeamPTransform.of(
                input -> MapElements
                    .of(input)
                    .using(s -> s.toUpperCase())
                    .output()
            ));

    PAssert.that(pCollection).containsInAnyOrder(upperCaseWords);

    BeamFlow flow = BeamFlow.of(pipeline);
    BeamExecutor executor = TestUtils.createExecutor();
    executor.execute(flow);
  }

  @Test
  public void testFlowToPCollection() {

    BeamFlow flow = BeamFlow.of(pipeline);

    final ListDataSource<Integer> input =
        ListDataSource.unbounded(Arrays.asList(1, 2, 3), Arrays.asList(4, 5, 6));
    Dataset<Integer> inputDataset = flow.createInput(input);

    //final ListDataSink<Integer> output = ListDataSink.get();
    Dataset<Integer> plusOne = MapElements.of(inputDataset).using(i -> i + 1).output();

    PCollection<Integer> unwrapped = flow.unwrapped(plusOne);

    PCollection<Integer> twicePcollection = unwrapped.apply("Twice", BeamPTransform.of(
        in -> MapElements.of(in).using(i -> 2 * i).output()
    ));

    PAssert.that(twicePcollection).containsInAnyOrder(4, 6, 8, 10, 12, 14);

    pipeline.run();
  }

}
