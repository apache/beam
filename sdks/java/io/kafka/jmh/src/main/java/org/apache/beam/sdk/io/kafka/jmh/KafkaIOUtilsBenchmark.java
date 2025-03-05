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
package org.apache.beam.sdk.io.kafka.jmh;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.io.kafka.KafkaIOUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.infra.ThreadParams;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(batchSize = KafkaIOUtilsBenchmark.SIZE)
@Measurement(batchSize = KafkaIOUtilsBenchmark.SIZE)
public class KafkaIOUtilsBenchmark {
  static final int READERS = 2;
  static final int WRITERS = 1;
  static final int SIZE = 1024;

  @State(Scope.Thread)
  public static class ProducerState {
    private int[] values;
    private int idx;

    @Setup(Level.Iteration)
    public void setup(final IterationParams ip, final ThreadParams tp) {
      values =
          new Random(299792458 + ip.getCount() + tp.getThreadIndex())
              .ints(KafkaIOUtilsBenchmark.SIZE, 0, 100)
              .toArray();
      idx = 0;
    }

    @TearDown(Level.Invocation)
    public void tearDown(final IterationParams ip, final ThreadParams tp) {
      idx = (idx + 1) % KafkaIOUtilsBenchmark.SIZE;
    }
  }

  @State(Scope.Group)
  public static class PlainAccumulatorState {
    // As implemented before 2.64.0.
    // Note that numUpdates may overflow and count back from Long.MIN_VALUE.
    static class MovingAvg {
      private static final int MOVING_AVG_WINDOW = 1000;
      private double avg = 0;
      private long numUpdates = 0;

      void update(double quantity) {
        numUpdates++;
        avg += (quantity - avg) / Math.min(MOVING_AVG_WINDOW, numUpdates);
      }

      double get() {
        return avg;
      }
    }

    private MovingAvg accumulator;

    @Setup(Level.Iteration)
    public void setup(final IterationParams ip, final ThreadParams tp) {
      accumulator = new MovingAvg();
    }
  }

  @State(Scope.Group)
  public static class AtomicAccumulatorState {
    private final KafkaIOUtils.MovingAvg accumulator = new KafkaIOUtils.MovingAvg();
  }

  @Benchmark
  @Group("Plain")
  @GroupThreads(KafkaIOUtilsBenchmark.WRITERS)
  public void plainWrite(final PlainAccumulatorState as, final ProducerState ps) {
    as.accumulator.update(ps.values[ps.idx]);
  }

  @Benchmark
  @Group("Plain")
  @GroupThreads(KafkaIOUtilsBenchmark.READERS)
  public double plainRead(final PlainAccumulatorState as) {
    return as.accumulator.get();
  }

  @Benchmark
  @Group("Atomic")
  @GroupThreads(KafkaIOUtilsBenchmark.WRITERS)
  public void atomicWrite(final AtomicAccumulatorState as, final ProducerState ps) {
    as.accumulator.update(ps.values[ps.idx]);
  }

  @Benchmark
  @Group("Atomic")
  @GroupThreads(KafkaIOUtilsBenchmark.READERS)
  public double atomicRead(final AtomicAccumulatorState as) {
    return as.accumulator.get();
  }
}
