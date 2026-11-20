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
package org.apache.beam.sdk.jmh.util;

import java.util.NoSuchElementException;
import java.util.Observer;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterable;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterator;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Microbenchmarks for {@link ElementByteSizeObservableIterator}, {@link
 * ElementByteSizeObservableIterable}, and {@link ElementByteSizeObserver}.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ElementByteSizeObservableBenchmark {

  private static final class BenchIterator extends ElementByteSizeObservableIterator<Integer> {
    private final int limit;
    private int index;

    BenchIterator(int limit) {
      this.limit = limit;
    }

    void fire(long byteSize) {
      notifyValueReturned(byteSize);
    }

    @Override
    public boolean hasNext() {
      return index < limit;
    }

    @Override
    public Integer next() {
      if (index >= limit) {
        throw new NoSuchElementException();
      }
      notifyValueReturned(512L);
      return index++;
    }
  }

  private static final class BenchIterable
      extends ElementByteSizeObservableIterable<Integer, BenchIterator> {
    private final int limit;

    BenchIterable(int limit) {
      this.limit = limit;
    }

    @Override
    protected BenchIterator createIterator() {
      return new BenchIterator(limit);
    }
  }

  private static final class AccumulatingByteSizeObserver extends ElementByteSizeObserver {
    private long reportedSum;

    @Override
    protected void reportElementSize(long elementByteSize) {
      reportedSum += elementByteSize;
    }

    long getReportedSum() {
      return reportedSum;
    }
  }

  @State(Scope.Thread)
  public static class ObservableState {
    BenchIterator noObserverIterator;
    BenchIterator byteSizeObserverIterator;
    BenchIterator genericObserverIterator;
    AccumulatingByteSizeObserver byteSizeObserver;
    long genericSum;

    @Setup
    public void setUp() {
      noObserverIterator = new BenchIterator(0);
      byteSizeObserverIterator = new BenchIterator(0);
      byteSizeObserver = new AccumulatingByteSizeObserver();
      byteSizeObserverIterator.addObserver(byteSizeObserver);
      genericObserverIterator = new BenchIterator(0);
      Observer genericObserver = (obs, arg) -> genericSum += ((Long) arg).longValue();
      genericObserverIterator.addObserver(genericObserver);
    }
  }

  @Benchmark
  public void notifyValueReturned_noObservers(ObservableState state) {
    state.noObserverIterator.fire(1024L);
  }

  @Benchmark
  public long notifyValueReturned_singleElementByteSizeObserver(ObservableState state) {
    state.byteSizeObserverIterator.fire(1024L);
    return state.byteSizeObserver.getReportedSum();
  }

  @Benchmark
  public void notifyValueReturned_singleGenericObserver(ObservableState state) {
    state.genericObserverIterator.fire(1024L);
  }

  @Benchmark
  public void iterableCreateAndIterate_noObservers(Blackhole bh) {
    BenchIterable iterable = new BenchIterable(16);
    BenchIterator it = iterable.iterator();
    int sum = 0;
    while (it.hasNext()) {
      sum += it.next();
    }
    bh.consume(sum);
  }
}
