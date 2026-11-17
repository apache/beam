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
package org.apache.beam.runners.flink.translation.functions;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.FlinkTestPipeline;
import org.apache.beam.runners.flink.translation.functions.DeterministicRebalance.DeterministicRoundRobinPartitioner;
import org.apache.beam.runners.flink.translation.functions.DeterministicRebalance.SenderKey;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

public class DeterministicRebalanceTest {

  /**
   * Returns the number of elements each receiver gets when the {@code k}-th of {@code numElements}
   * elements starts on sender {@code k mod numSenders}, as after a perfect round-robin.
   */
  private static int[] receiverCounts(int numElements, int numSenders, int numReceivers) {
    int[] counts = new int[numReceivers];
    for (int sender = 0; sender < numSenders; sender++) {
      // Each sender subtask has its own partitioner instance.
      DeterministicRoundRobinPartitioner partitioner = new DeterministicRoundRobinPartitioner();
      long key = SenderKey.of(sender, numSenders);
      for (int k = sender; k < numElements; k += numSenders) {
        counts[partitioner.partition(key, numReceivers)]++;
      }
    }
    return counts;
  }

  /**
   * Returns the number of elements each receiver gets when sender {@code i} emits {@code
   * elementsPerSender[i]} elements.
   */
  private static int[] receiverCounts(int[] elementsPerSender, int numReceivers) {
    int[] counts = new int[numReceivers];
    for (int sender = 0; sender < elementsPerSender.length; sender++) {
      DeterministicRoundRobinPartitioner partitioner = new DeterministicRoundRobinPartitioner();
      long key = SenderKey.of(sender, elementsPerSender.length);
      for (int k = 0; k < elementsPerSender[sender]; k++) {
        counts[partitioner.partition(key, numReceivers)]++;
      }
    }
    return counts;
  }

  /**
   * Returns per-sender element counts where only senders {@code first} to {@code first + numBusy -
   * 1} of {@code numSenders} have elements, {@code numElements} in total, dealt round-robin.
   */
  private static int[] consecutiveBusySenders(
      int numSenders, int first, int numBusy, int numElements) {
    int[] elementsPerSender = new int[numSenders];
    for (int k = 0; k < numElements; k++) {
      elementsPerSender[first + k % numBusy]++;
    }
    return elementsPerSender;
  }

  private static void assertBalanced(int[] counts, int numElements) {
    assertEquals(numElements, Arrays.stream(counts).sum());
    int min = Arrays.stream(counts).min().getAsInt();
    int max = Arrays.stream(counts).max().getAsInt();
    assertTrue(
        "receiver counts should differ by at most one, got min " + min + " and max " + max,
        max - min <= 1);
  }

  @Test
  public void testFewElementsPerSenderStayBalanced() {
    // About 2.6 elements per sender, as when 17,776 files are reshuffled at parallelism 6,900:
    // random start channels leave about 7.6% of the receivers empty.
    assertBalanced(receiverCounts(17_776, 6_900, 6_900), 17_776);
  }

  @Test
  public void testSingleSenderRoundRobinsOverAllReceivers() {
    int[] counts = receiverCounts(70, 1, 7);
    int[] expected = new int[7];
    Arrays.fill(expected, 10);
    assertArrayEquals(expected, counts);
  }

  @Test
  public void testFewerSendersThanReceivers() {
    assertBalanced(receiverCounts(25, 5, 20), 25);
    assertBalanced(receiverCounts(10_003, 3, 10), 10_003);
  }

  @Test
  public void testMoreSendersThanReceiversStayCloseToBalanced() {
    // With more senders than receivers, groups of about M / N senders share a start channel, so
    // the counts can differ by about one element per sender in a group. Random start channels
    // would give a Poisson spread instead: here a standard deviation of about 2.5 around 6.2.
    int numSenders = 20;
    int numReceivers = 6;
    int[] counts = receiverCounts(37, numSenders, numReceivers);
    assertEquals(37, Arrays.stream(counts).sum());
    int min = Arrays.stream(counts).min().getAsInt();
    int max = Arrays.stream(counts).max().getAsInt();
    int bound = (numSenders + numReceivers - 1) / numReceivers;
    assertTrue("got min " + min + " and max " + max, min > 0 && max - min <= bound);
  }

  @Test
  public void testConsecutiveBusySendersSpreadOverAllReceivers() {
    // 48 file patterns statically assigned to the first 48 of 1,760 subtasks match 4,885 files,
    // about 102 per busy sender. Start channels floor(i * N / M) = i made their round-robin ranges
    // overlap on receivers 0 to 147, up to 48 files each, while the other receivers got none.
    int numSenders = 1_760;
    int numElements = 4_885;
    int fairShare = (numElements + numSenders - 1) / numSenders;
    for (int first : new int[] {0, 800, numSenders - 48}) {
      int[] counts =
          receiverCounts(consecutiveBusySenders(numSenders, first, 48, numElements), numSenders);
      assertEquals(numElements, Arrays.stream(counts).sum());
      int min = Arrays.stream(counts).min().getAsInt();
      int max = Arrays.stream(counts).max().getAsInt();
      assertTrue(
          "busy senders from " + first + ": got min " + min + " and max " + max,
          min > 0 && max <= fairShare + 2);
    }
  }

  @Test
  public void testBlockOfBusySendersStaysCloseToBalanced() {
    // 200 consecutive busy senders of 2,000 with 100 elements each: 10 per receiver would be even.
    int[] counts = receiverCounts(consecutiveBusySenders(2_000, 0, 200, 20_000), 2_000);
    int min = Arrays.stream(counts).min().getAsInt();
    int max = Arrays.stream(counts).max().getAsInt();
    assertTrue("got min " + min + " and max " + max, min >= 7 && max <= 13);
  }

  @Test
  public void testStartChannelsArePermutationOfEvenlySpacedChannels() {
    // With as many receivers as senders, every sender must start on a different channel, which
    // keeps inputs where every sender has elements exactly as balanced as evenly spaced starts.
    List<Integer> sizes = new ArrayList<>();
    for (int m = 1; m <= 600; m++) {
      sizes.add(m);
    }
    sizes.addAll(Arrays.asList(1_760, 6_900, 13_800, 65_536));
    for (int numSenders : sizes) {
      boolean[] seen = new boolean[numSenders];
      for (int sender = 0; sender < numSenders; sender++) {
        int channel = DeterministicRebalance.startChannel(sender, numSenders, numSenders);
        assertTrue("duplicate start channel for " + numSenders + " senders", !seen[channel]);
        seen[channel] = true;
      }
    }
  }

  @Test
  public void testReshuffleInDataStreamBatchKeepsAllElements() {
    FlinkTestPipeline p = FlinkTestPipeline.createForBatch();
    FlinkPipelineOptions options = p.getOptions().as(FlinkPipelineOptions.class);
    options.setParallelism(4);
    options.setUseDataStreamForBatch(true);
    List<Integer> input = new ArrayList<>();
    for (int i = 0; i < 1_000; i++) {
      input.add(i);
    }

    PCollection<Integer> output = p.apply(Create.of(input)).apply(Reshuffle.viaRandomKey());

    PAssert.that(output).containsInAnyOrder(input);
    p.run();
  }

  @Test
  public void testSenderKeyRoundTrips() {
    long key = SenderKey.of(6_899, 13_800);
    assertEquals(6_899, SenderKey.senderIndex(key));
    assertEquals(13_800, SenderKey.numSenders(key));
  }
}
