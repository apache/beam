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

import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Partition.PartitionFn;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link Partition}.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class PartitionTest implements Serializable {
  @Rule public ExpectedException expectedException = ExpectedException.none();

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

  @Test
  @Category(RunnableOnService.class)
  public void testEvenOddPartition() {
    TestPipeline p = TestPipeline.create();

    PCollectionList<Integer> outputs = p
        .apply(Create.of(591, 11789, 1257, 24578, 24799, 307))
        .apply(Partition.of(2, new ModFn()));
    assertTrue(outputs.size() == 2);
    DataflowAssert.that(outputs.get(0)).containsInAnyOrder(24578);
    DataflowAssert.that(outputs.get(1)).containsInAnyOrder(591, 11789, 1257,
        24799, 307);
    p.run();
  }

  @Test
  public void testModPartition() {
    TestPipeline p = TestPipeline.create();

    PCollectionList<Integer> outputs = p
        .apply(Create.of(1, 2, 4, 5))
        .apply(Partition.of(3, new ModFn()));
    assertTrue(outputs.size() == 3);
    DataflowAssert.that(outputs.get(0)).containsInAnyOrder();
    DataflowAssert.that(outputs.get(1)).containsInAnyOrder(1, 4);
    DataflowAssert.that(outputs.get(2)).containsInAnyOrder(2, 5);
    p.run();
  }

  @Test
  public void testOutOfBoundsPartitions() {
    TestPipeline p = TestPipeline.create();

    p
    .apply(Create.of(-1))
    .apply(Partition.of(5, new IdentityFn()));

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(
        "Partition function returned out of bounds index: -1 not in [0..5)");
    p.run();
  }

  @Test
  public void testZeroNumPartitions() {
    TestPipeline p = TestPipeline.create();

    PCollection<Integer> input = p.apply(Create.of(591));

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("numPartitions must be > 0");
    input.apply(Partition.of(0, new IdentityFn()));
  }

  @Test
  public void testDroppedPartition() {
    TestPipeline p = TestPipeline.create();

    // Compute the set of integers either 1 or 2 mod 3, the hard way.
    PCollectionList<Integer> outputs = p
        .apply(Create.of(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
        .apply(Partition.of(3, new ModFn()));

    List<PCollection<Integer>> outputsList = new ArrayList<>(outputs.getAll());
    outputsList.remove(0);
    outputs = PCollectionList.of(outputsList);
    assertTrue(outputs.size() == 2);

    PCollection<Integer> output = outputs.apply(Flatten.<Integer>pCollections());
    DataflowAssert.that(output).containsInAnyOrder(2, 4, 5, 7, 8, 10, 11);
    p.run();
  }
}
