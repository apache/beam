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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.transforms.Partition.PartitionWithSideInputsFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Partition}. */
@RunWith(JUnit4.class)
public class PartitionTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  static class ModFn implements PartitionFn<Integer> {
    @Override
    public int partitionFor(Integer elem, int numPartitions) {
      return elem % numPartitions;
    }
  }

  static class IdentityFn implements PartitionFn<Integer> {
    @Override
    public int partitionFor(Integer elem, int numPartitions) {
      return elem;
    }
  }

  static class IdentitySideViewFn implements PartitionWithSideInputsFn<Integer> {
    @Override
    public int partitionFor(Integer elem, int numPartitions, Contextful.Fn.Context c) {
      return elem;
    }
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPartitionWithSideInputs() {

    PCollectionView<Integer> gradesView =
        pipeline.apply("grades", Create.of(50)).apply(View.asSingleton());

    Create.Values<Integer> studentsPercentage = Create.of(5, 45, 90, 29, 55, 65);
    PCollectionList<Integer> studentsGrades =
        pipeline
            .apply(studentsPercentage)
            .apply(
                Partition.of(
                    2,
                    ((elem, numPartitions, ct) -> {
                      Integer grades = ct.sideInput(gradesView);
                      return elem < grades ? 0 : 1;
                    }),
                    Requirements.requiresSideInputs(gradesView)));

    assertTrue(studentsGrades.size() == 2);
    PAssert.that(studentsGrades.get(0)).containsInAnyOrder(5, 29, 45);
    PAssert.that(studentsGrades.get(1)).containsInAnyOrder(55, 65, 90);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testEvenOddPartition() {

    PCollectionList<Integer> outputs =
        pipeline
            .apply(Create.of(591, 11789, 1257, 24578, 24799, 307))
            .apply(Partition.of(2, new ModFn()));
    assertTrue(outputs.size() == 2);
    PAssert.that(outputs.get(0)).containsInAnyOrder(24578);
    PAssert.that(outputs.get(1)).containsInAnyOrder(591, 11789, 1257, 24799, 307);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testModPartition() {

    PCollectionList<Integer> outputs =
        pipeline.apply(Create.of(1, 2, 4, 5)).apply(Partition.of(3, new ModFn()));
    assertTrue(outputs.size() == 3);
    PAssert.that(outputs.get(0)).empty();
    PAssert.that(outputs.get(1)).containsInAnyOrder(1, 4);
    PAssert.that(outputs.get(2)).containsInAnyOrder(2, 5);
    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testOutOfBoundsPartitions() {

    pipeline.apply(Create.of(-1)).apply(Partition.of(5, new IdentityFn()));

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Partition function returned out of bounds index: -1 not in [0..5)");
    pipeline.run();
  }

  @Test
  public void testZeroNumPartitions() {

    PCollection<Integer> input = pipeline.apply(Create.of(591));

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("numPartitions must be > 0");
    input.apply(Partition.of(0, new IdentityFn()));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDroppedPartition() {

    // Compute the set of integers either 1 or 2 mod 3, the hard way.
    PCollectionList<Integer> outputs =
        pipeline
            .apply(Create.of(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
            .apply(Partition.of(3, new ModFn()));

    List<PCollection<Integer>> outputsList = new ArrayList<>(outputs.getAll());
    outputsList.remove(0);
    outputs = PCollectionList.of(outputsList);
    assertTrue(outputs.size() == 2);

    PCollection<Integer> output = outputs.apply(Flatten.pCollections());
    PAssert.that(output).containsInAnyOrder(2, 4, 5, 7, 8, 10, 11);
    pipeline.run();
  }

  @Test
  public void testPartitionGetName() {
    assertEquals("Partition", Partition.of(3, new ModFn()).getName());
  }

  @Test
  public void testDisplayData() {
    Partition<?> partition = Partition.of(123, new IdentityFn());
    DisplayData displayData = DisplayData.from(partition);

    assertThat(displayData, hasDisplayItem("numPartitions", 123));
    assertThat(displayData, hasDisplayItem("partitionFn", IdentityFn.class));
  }

  @Test
  public void testDisplayDataOfSideViewFunction() {
    Partition<?> partition = Partition.of(123, new IdentitySideViewFn(), Requirements.empty());
    DisplayData displayData = DisplayData.from(partition);

    assertThat(displayData, hasDisplayItem("numPartitions", 123));
    assertThat(displayData, hasDisplayItem("partitionFn", IdentitySideViewFn.class));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testModPartitionWithLambda() {

    PCollectionList<Integer> outputs =
        pipeline
            .apply(Create.of(1, 2, 4, 5))
            .apply(Partition.of(3, (element, numPartitions) -> element % numPartitions));
    assertEquals(3, outputs.size());
    PAssert.that(outputs.get(0)).empty();
    PAssert.that(outputs.get(1)).containsInAnyOrder(1, 4);
    PAssert.that(outputs.get(2)).containsInAnyOrder(2, 5);
    pipeline.run();
  }

  /**
   * Confirms that in Java 8 style, where a lambda results in a rawtype, the output type token is
   * not useful. If this test ever fails there may be simplifications available to us.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testPartitionFnOutputTypeDescriptorRaw() throws Exception {

    PCollectionList<String> output =
        pipeline.apply(Create.of("hello")).apply(Partition.of(1, (element, numPartitions) -> 0));

    thrown.expect(CannotProvideCoderException.class);
    pipeline.getCoderRegistry().getCoder(output.get(0).getTypeDescriptor());
  }
}
