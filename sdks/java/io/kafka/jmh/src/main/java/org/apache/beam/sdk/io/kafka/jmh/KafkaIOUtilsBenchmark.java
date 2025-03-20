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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.infra.ThreadParams;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Threads(Threads.MAX)
public class KafkaIOUtilsBenchmark {
  private static final int SIZE = 1024;

  @State(Scope.Thread)
  public static class ProducerState {
    private int[] values;
    private int idx;

    @Setup(Level.Iteration)
    public void setup(final IterationParams ip, final ThreadParams tp) {
      values = new Random(299792458 + ip.getCount()).ints(SIZE, 0, 100).toArray();
      idx = 0;
    }

    int next() {
      final int value = values[idx];
      idx = (idx + 1) % SIZE;
      return value;
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

    MovingAvg accumulator;

    @Setup(Level.Trial)
    public void setup() {
      accumulator = new MovingAvg();
    }
  }

  @State(Scope.Group)
  public static class AtomicAccumulatorState {
    KafkaIOUtils.MovingAvg accumulator;

    @Setup(Level.Trial)
    public void setup() {
      accumulator = new KafkaIOUtils.MovingAvg();
    }
  }

  @State(Scope.Group)
  public static class VolatileAccumulatorState {
    // Atomic accumulator using only volatile reads and writes.
    static class MovingAvg {
      private static final int MOVING_AVG_WINDOW = 1000;

      private volatile double avg = 0;
      private long numUpdates = 0;

      void update(final double quantity) {
        final double prevAvg = avg;
        numUpdates = Math.min(MOVING_AVG_WINDOW, numUpdates + 1);
        avg = prevAvg + (quantity - prevAvg) / numUpdates;
      }

      double get() {
        return avg;
      }
    }

    MovingAvg accumulator;

    @Setup(Level.Trial)
    public void setup() {
      accumulator = new MovingAvg();
    }
  }

  @Benchmark
  @Group("WritePlain")
  public void plainWrite(final PlainAccumulatorState as, final ProducerState ps) {
    as.accumulator.update(ps.next());
  }

  @Benchmark
  @Group("ReadPlain")
  public double plainRead(final PlainAccumulatorState as) {
    return as.accumulator.get();
  }

  @Benchmark
  @Group("ReadAndWritePlain")
  public void plainWriteWhileReading(final PlainAccumulatorState as, final ProducerState ps) {
    as.accumulator.update(ps.next());
  }

  @Benchmark
  @Group("ReadAndWritePlain")
  public double plainReadWhileWriting(final PlainAccumulatorState as) {
    return as.accumulator.get();
  }

  @Benchmark
  @Group("WriteSynchronizedPlain")
  public void synchronizedPlainWrite(final PlainAccumulatorState as, final ProducerState ps) {
    final PlainAccumulatorState.MovingAvg accumulator = as.accumulator;
    final int value = ps.next();
    synchronized (accumulator) {
      accumulator.update(value);
    }
  }

  @Benchmark
  @Group("ReadSynchronizedPlain")
  public double synchronizedPlainRead(final PlainAccumulatorState as) {
    final PlainAccumulatorState.MovingAvg accumulator = as.accumulator;
    synchronized (accumulator) {
      return accumulator.get();
    }
  }

  @Benchmark
  @Group("ReadAndWriteSynchronizedPlain")
  public void synchronizedPlainWriteWhileReading(
      final PlainAccumulatorState as, final ProducerState ps) {
    final PlainAccumulatorState.MovingAvg accumulator = as.accumulator;
    final int value = ps.next();
    synchronized (accumulator) {
      accumulator.update(value);
    }
  }

  @Benchmark
  @Group("ReadAndWriteSynchronizedPlain")
  public double synchronizedPlainReadWhileWriting(final PlainAccumulatorState as) {
    final PlainAccumulatorState.MovingAvg accumulator = as.accumulator;
    synchronized (accumulator) {
      return accumulator.get();
    }
  }

  @Benchmark
  @Group("WriteAtomic")
  public void atomicWrite(final AtomicAccumulatorState as, final ProducerState ps) {
    as.accumulator.update(ps.next());
  }

  @Benchmark
  @Group("ReadAtomic")
  public double atomicRead(final AtomicAccumulatorState as) {
    return as.accumulator.get();
  }

  @Benchmark
  @Group("ReadAndWriteAtomic")
  public void atomicWriteWhileReading(final AtomicAccumulatorState as, final ProducerState ps) {
    as.accumulator.update(ps.next());
  }

  @Benchmark
  @Group("ReadAndWriteAtomic")
  public double atomicReadWhileWriting(final AtomicAccumulatorState as) {
    return as.accumulator.get();
  }

  @Benchmark
  @Group("WriteVolatile")
  public void volatileWrite(final VolatileAccumulatorState as, final ProducerState ps) {
    as.accumulator.update(ps.next());
  }

  @Benchmark
  @Group("ReadVolatile")
  public double volatileRead(final VolatileAccumulatorState as) {
    return as.accumulator.get();
  }

  @Benchmark
  @Group("ReadAndWriteVolatile")
  public void volatileWriteWhileReading(final VolatileAccumulatorState as, final ProducerState ps) {
    as.accumulator.update(ps.next());
  }

  @Benchmark
  @Group("ReadAndWriteVolatile")
  public double volatileReadWhileWriting(final VolatileAccumulatorState as) {
    return as.accumulator.get();
  }
}
