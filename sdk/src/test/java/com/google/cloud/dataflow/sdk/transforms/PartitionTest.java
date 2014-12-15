/*
 * Copyright (C) 2014 Google Inc.
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

import static com.google.cloud.dataflow.sdk.TestUtils.createInts;
import static com.google.cloud.dataflow.sdk.transforms.Partition.PartitionFn;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link Partition}.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("serial")
public class PartitionTest implements Serializable {
  static class ModFn implements PartitionFn<Integer> {
    public int partitionFor(Integer elem, int numPartitions) {
      return elem % numPartitions;
    }
  }

  static class IdentityFn implements PartitionFn<Integer> {
    public int partitionFor(Integer elem, int numPartitions) {
      return elem;
    }
  }

  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
  public void testEvenOddPartition() {
    TestPipeline p = TestPipeline.create();

    PCollection<Integer> input =
        createInts(p, Arrays.asList(591, 11789, 1257, 24578, 24799, 307));

    PCollectionList<Integer> outputs = input.apply(Partition.of(2, new ModFn()));
    assertTrue(outputs.size() == 2);
    DataflowAssert.that(outputs.get(0)).containsInAnyOrder(24578);
    DataflowAssert.that(outputs.get(1)).containsInAnyOrder(591, 11789, 1257,
        24799, 307);
    p.run();
  }

  @Test
  public void testModPartition() {
    TestPipeline p = TestPipeline.create();

    PCollection<Integer> input =
        createInts(p, Arrays.asList(1, 2, 4, 5));

    PCollectionList<Integer> outputs = input.apply(Partition.of(3, new ModFn()));
    assertTrue(outputs.size() == 3);
    DataflowAssert.that(outputs.get(0)).containsInAnyOrder();
    DataflowAssert.that(outputs.get(1)).containsInAnyOrder(1, 4);
    DataflowAssert.that(outputs.get(2)).containsInAnyOrder(2, 5);
    p.run();
  }

  @Test
  public void testOutOfBoundsPartitions() {
    TestPipeline p = TestPipeline.create();

    PCollection<Integer> input = createInts(p, Arrays.asList(-1));

    input.apply(Partition.of(5, new IdentityFn()));

    try {
      p.run();
    } catch (RuntimeException e) {
      assertThat(e.toString(), containsString(
          "Partition function returned out of bounds index: -1 not in [0..5)"));
    }
  }

  @Test
  public void testZeroNumPartitions() {
    TestPipeline p = TestPipeline.create();

    PCollection<Integer> input = createInts(p, Arrays.asList(591));

    try {
      input.apply(Partition.of(0, new IdentityFn()));
      fail("should have failed");
    } catch (IllegalArgumentException exn) {
      assertThat(exn.toString(), containsString("numPartitions must be > 0"));
    }
  }

  @Test
  public void testDroppedPartition() {
    TestPipeline p = TestPipeline.create();

    PCollection<Integer> input = createInts(p,
        Arrays.asList(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12));

    // Compute the set of integers either 1 or 2 mod 3, the hard way.
    PCollectionList<Integer> outputs =
        input.apply(Partition.of(3, new ModFn()));

    List<PCollection<Integer>> outputsList = new ArrayList<>(outputs.getAll());
    outputsList.remove(0);
    outputs = PCollectionList.of(outputsList);
    assertTrue(outputs.size() == 2);

    PCollection<Integer> output = outputs.apply(Flatten.<Integer>create());
    DataflowAssert.that(output).containsInAnyOrder(2, 4, 5, 7, 8, 10, 11);
    p.run();
  }
}
