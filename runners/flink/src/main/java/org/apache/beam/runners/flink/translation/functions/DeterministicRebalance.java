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

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * Round-robin redistribution where each sender starts at a channel derived from its own subtask
 * index rather than at a random one.
 *
 * <p>Flink's {@link DataStream#rebalance()} starts each sender's round-robin at a random channel.
 * That is even when senders emit many elements, but when they emit only a few each, as when a
 * {@code Reshuffle} redistributes file names or the restrictions of a splittable DoFn, the
 * assignment is close to random: with {@code k} elements per sender on average, the number of
 * elements per receiver follows a Poisson distribution of mean {@code k}, so some receivers get
 * none and others several times the mean.
 *
 * <p>Here sender {@code i} of {@code M} sends its {@code j}-th element to channel {@code
 * (floor(s(i) * N / M) + j) mod N} for {@code N} receivers, where {@code s(i) = i * g mod M} for a
 * multiplier {@code g} coprime to {@code M}. The start channels are spread evenly, so an input that
 * is already balanced stays balanced, and a single sender's elements still go round-robin over all
 * receivers.
 *
 * <p>Often only some senders have elements, and they are consecutive subtasks: a small {@code
 * Create} or a few file patterns are assigned to the first subtasks, so only they emit the matched
 * files. If sender {@code i} started at {@code floor(i * N / M)}, consecutive senders would start
 * on neighbouring channels, and once each emits more than {@code N / M} elements their round-robin
 * ranges would overlap and pile onto the same few receivers. {@code s} is a permutation of the
 * sender indices that sends consecutive indices far apart: {@code g} is close to {@code M} times
 * the golden ratio conjugate, so the start channels of any run of consecutive senders are spread
 * almost evenly over all receivers.
 *
 * <p>A {@link Partitioner} is not told which sender it runs in, so each element is first tagged
 * with its sender's index and parallelism. The tagging and untagging maps are chained to their
 * neighbours, so the only cost is the tag in the serialized elements.
 */
public final class DeterministicRebalance {

  private DeterministicRebalance() {}

  /** Redistributes {@code input} round-robin from deterministic per-sender start channels. */
  public static <T> DataStream<T> apply(DataStream<T> input, String name) {
    TypeInformation<T> typeInfo = input.getType();
    TypeInformation<Tuple2<Long, T>> taggedTypeInfo =
        new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, typeInfo);
    return input
        .map(new TagWithSender<T>())
        .name(name + "/TagWithSender")
        .returns(taggedTypeInfo)
        .partitionCustom(new DeterministicRoundRobinPartitioner(), tagged -> tagged.f0)
        .map(tagged -> tagged.f1)
        .name(name + "/StripSender")
        .returns(typeInfo);
  }

  /**
   * Encodes a sender's subtask index and the senders' parallelism in one key: the sender index in
   * the upper 32 bits, the number of senders in the lower 32 bits.
   */
  static final class SenderKey {

    private SenderKey() {}

    static long of(int senderIndex, int numSenders) {
      return ((long) senderIndex << 32) | (numSenders & 0xFFFFFFFFL);
    }

    static int senderIndex(long senderKey) {
      return (int) (senderKey >>> 32);
    }

    static int numSenders(long senderKey) {
      return (int) senderKey;
    }
  }

  /** Tags each element with its {@link SenderKey}, which is constant for a sender subtask. */
  static class TagWithSender<T> extends RichMapFunction<T, Tuple2<Long, T>> {

    // Transient fields are reset on deserialization, so the key is computed on first use.
    private transient boolean keyed;
    private transient long key;

    @Override
    public Tuple2<Long, T> map(T value) {
      if (!keyed) {
        TaskInfo taskInfo = getRuntimeContext().getTaskInfo();
        key =
            SenderKey.of(taskInfo.getIndexOfThisSubtask(), taskInfo.getNumberOfParallelSubtasks());
        keyed = true;
      }
      return Tuple2.of(key, value);
    }
  }

  /** {@code (sqrt(5) - 1) / 2}. Multiples of it are spread almost evenly modulo one. */
  private static final double GOLDEN_RATIO_CONJUGATE = 0.6180339887498949;

  /**
   * Returns the multiplier {@code g} of the permutation {@code i -> i * g mod numSenders} that
   * assigns start slots to senders: the integer closest to {@code numSenders} times the golden
   * ratio conjugate that is coprime to {@code numSenders}, so that the map is a permutation.
   */
  static int scatterMultiplier(int numSenders) {
    if (numSenders <= 2) {
      return 1;
    }
    long target = Math.round(numSenders * GOLDEN_RATIO_CONJUGATE);
    for (long distance = 0; distance < numSenders; distance++) {
      long above = target + distance;
      if (above < numSenders && gcd(above, numSenders) == 1) {
        return (int) above;
      }
      long below = target - distance;
      if (below > 0 && gcd(below, numSenders) == 1) {
        return (int) below;
      }
    }
    return 1;
  }

  /** Returns the channel at which sender {@code senderIndex} of {@code numSenders} starts. */
  static int startChannel(int senderIndex, int numSenders, int numPartitions) {
    int senders = Math.max(1, numSenders);
    long slot = (long) senderIndex * scatterMultiplier(senders) % senders;
    return (int) (slot * numPartitions / senders % numPartitions);
  }

  private static long gcd(long a, long b) {
    while (b != 0) {
      long t = a % b;
      a = b;
      b = t;
    }
    return a;
  }

  /**
   * Sends the {@code j}-th element of sender {@code i} of {@code M} to channel {@code (floor(s(i) *
   * N / M) + j) mod N}, where {@code s(i) = i * g mod M} (see {@link #startChannel}).
   *
   * <p>Stateful: Flink gives each sender subtask its own deserialized partitioner, which counts
   * that sender's elements.
   */
  static class DeterministicRoundRobinPartitioner implements Partitioner<Long> {

    // Transient fields are reset on deserialization, so the start channel is set on first use.
    private transient boolean started;
    private transient int nextChannel;

    @Override
    public int partition(Long key, int numPartitions) {
      if (!started) {
        nextChannel =
            startChannel(SenderKey.senderIndex(key), SenderKey.numSenders(key), numPartitions);
        started = true;
      }
      int channel = nextChannel;
      nextChannel = (nextChannel + 1) % numPartitions;
      return channel;
    }
  }
}
