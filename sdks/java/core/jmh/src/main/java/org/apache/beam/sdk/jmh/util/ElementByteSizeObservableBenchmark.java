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
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterable;
import org.apache.beam.sdk.util.common.ElementByteSizeObservableIterator;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Microbenchmarks for {@link ElementByteSizeObservableIterator}, {@link
 * ElementByteSizeObservableIterable}, {@link ElementByteSizeObserver}, and {@link
 * org.apache.beam.sdk.coders.IterableLikeCoder} lazy element byte-size observation.
 *
 * <p>Run with {@code -prof gc} to verify per-element allocation rates ({@code gc.alloc.rate.norm}).
 * {@link BenchIterator#fire(long)} is marked {@link CompilerControl.Mode#DONT_INLINE} so C2 still
 * inlines {@code notifyValueReturned} + {@code update(long)} inside {@code fire}, while preventing
 * dead-loop elimination ({@code noObservers}) and LICM hoisting of {@code observers}, {@code
 * changed}, {@code instanceof}, and {@code scalingFactor} across the 1024-element batch loop.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class ElementByteSizeObservableBenchmark {

  /** Number of per-element notifications per invocation to amortize JMH loop-control overhead. */
  static final int BATCH_SIZE = 1024;

  private static final Integer ELEMENT = 42;

  private static final class BenchIterator extends ElementByteSizeObservableIterator<Integer> {
    private final long[] sizes;
    private final int limit;
    private int index;

    BenchIterator(long[] sizes, int limit) {
      this.sizes = sizes;
      this.limit = limit;
    }

    @CompilerControl(CompilerControl.Mode.DONT_INLINE)
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
      notifyValueReturned(sizes[index++]);
      return ELEMENT;
    }
  }

  private static final class BenchIterable
      extends ElementByteSizeObservableIterable<Integer, BenchIterator> {
    private final long[] sizes;
    private final int limit;

    BenchIterable(long[] sizes, int limit) {
      this.sizes = sizes;
      this.limit = limit;
    }

    @Override
    protected BenchIterator createIterator() {
      return new BenchIterator(sizes, limit);
    }
  }

  private abstract static class BaseAccumulatingByteSizeObserver extends ElementByteSizeObserver {
    private long reportedSum;

    @Override
    protected void reportElementSize(long elementByteSize) {
      reportedSum += elementByteSize;
    }

    long takeReportedSum() {
      long result = reportedSum;
      reportedSum = 0L;
      return result;
    }

    void reset() {
      advance();
      reportedSum = 0L;
    }
  }

  private static final class AccumulatingByteSizeObserver
      extends BaseAccumulatingByteSizeObserver {}

  /** Second distinct concrete observer class for multi-observer / polymorphic benchmarks. */
  private static final class SecondaryByteSizeObserver extends BaseAccumulatingByteSizeObserver {}

  /** Third distinct concrete observer class to exercise megamorphic observer hierarchies. */
  private static final class TertiaryByteSizeObserver extends BaseAccumulatingByteSizeObserver {}

  /** Thread-local pre-initialized iterators, observers, and non-constant byte sizes. */
  @State(Scope.Thread)
  public static class ObservableState {
    long[] sizes;
    int iterateLimit = 16;
    IterableCoder<Integer> iterableCoder;

    BenchIterator noObserverIterator;

    BenchIterator singleByteSizeIterator;
    AccumulatingByteSizeObserver singleByteSizeObserver;

    BenchIterator scaledByteSizeIterator;
    AccumulatingByteSizeObserver scaledByteSizeObserver;

    BenchIterator twoByteSizeIterator;
    AccumulatingByteSizeObserver primaryObserver;
    SecondaryByteSizeObserver secondaryObserver;

    BenchIterator mixedMultiIterator;
    AccumulatingByteSizeObserver mixedOuterObserver;
    SecondaryByteSizeObserver mixedSecondaryObserver;
    long mixedGenericSum;

    BenchIterator[] polymorphicIterators;
    AccumulatingByteSizeObserver polyIterableOuterObserver;
    AccumulatingByteSizeObserver polyObs1;
    SecondaryByteSizeObserver polyObs2;
    TertiaryByteSizeObserver polyObs3;

    BenchIterator genericObserverIterator;
    long genericSum;
    Blackhole activeBlackhole;

    BenchIterator iterableLikeIterator;
    AccumulatingByteSizeObserver iterableLikeOuterObserver;

    /** Allocates iterators and fills a mixed array of cached and uncached {@code long} sizes. */
    @Setup(Level.Trial)
    public void setUpTrial() throws Exception {
      sizes = new long[BATCH_SIZE];
      for (int i = 0; i < BATCH_SIZE; i++) {
        // Mix 12.5% small (<= 127) and 87.5% uncached (> 127) sizes.
        sizes[i] = (i & 7) == 0 ? 32L + (i & 63) : 256L + ((i * 37L) & 0xFFFL);
      }

      iterableCoder = IterableCoder.of(VarIntCoder.of());
      noObserverIterator = new BenchIterator(sizes, 0);

      singleByteSizeIterator = new BenchIterator(sizes, 0);
      singleByteSizeObserver = new AccumulatingByteSizeObserver();
      singleByteSizeIterator.addObserver(singleByteSizeObserver);

      scaledByteSizeIterator = new BenchIterator(sizes, 0);
      scaledByteSizeObserver = new AccumulatingByteSizeObserver();
      scaledByteSizeObserver.setScalingFactor(1.5);
      scaledByteSizeIterator.addObserver(scaledByteSizeObserver);

      twoByteSizeIterator = new BenchIterator(sizes, 0);
      primaryObserver = new AccumulatingByteSizeObserver();
      secondaryObserver = new SecondaryByteSizeObserver();
      twoByteSizeIterator.addObserver(primaryObserver);
      twoByteSizeIterator.addObserver(secondaryObserver);

      // Register via IterableCoder so IterableLikeCoder.IteratorObserver wraps the outer observer
      // identically to StateBackedIterable.
      iterableLikeOuterObserver = new AccumulatingByteSizeObserver();
      BenchIterable backingIterable = new BenchIterable(sizes, 0);
      iterableCoder.registerByteSizeObserver(backingIterable, iterableLikeOuterObserver);
      iterableLikeIterator = backingIterable.iterator();

      // Mixed multi-observer: IteratorObserver + ElementByteSizeObserver + generic Observer.
      mixedOuterObserver = new AccumulatingByteSizeObserver();
      mixedSecondaryObserver = new SecondaryByteSizeObserver();
      BenchIterable mixedIterable = new BenchIterable(sizes, 0);
      iterableCoder.registerByteSizeObserver(mixedIterable, mixedOuterObserver);
      mixedIterable.addObserver(mixedSecondaryObserver);
      mixedIterable.addObserver(
          (obs, arg) -> {
            mixedGenericSum += ((Long) arg).longValue();
            if (activeBlackhole != null) {
              activeBlackhole.consume(arg);
            }
          });
      mixedMultiIterator = mixedIterable.iterator();

      // Polymorphic cycle of 4 distinct observer configurations (IteratorObserver, 3
      // ElementByteSizeObserver subclasses, plus unobserved) to stress C2 type profiles.
      polyIterableOuterObserver = new AccumulatingByteSizeObserver();
      BenchIterable polyBacking = new BenchIterable(sizes, 0);
      iterableCoder.registerByteSizeObserver(polyBacking, polyIterableOuterObserver);
      BenchIterator polyIt0 = polyBacking.iterator();

      polyObs1 = new AccumulatingByteSizeObserver();
      BenchIterator polyIt1 = new BenchIterator(sizes, 0);
      polyIt1.addObserver(polyObs1);

      polyObs2 = new SecondaryByteSizeObserver();
      BenchIterator polyIt2 = new BenchIterator(sizes, 0);
      polyIt2.addObserver(polyObs2);

      polyObs3 = new TertiaryByteSizeObserver();
      BenchIterator polyIt3 = new BenchIterator(sizes, 0);
      polyIt3.addObserver(polyObs3);

      polymorphicIterators =
          new BenchIterator[] {polyIt0, polyIt1, polyIt2, polyIt3, noObserverIterator};

      genericObserverIterator = new BenchIterator(sizes, 0);
      Observer genericObserver =
          (obs, arg) -> {
            genericSum += ((Long) arg).longValue();
            if (activeBlackhole != null) {
              activeBlackhole.consume(arg);
            }
          };
      genericObserverIterator.addObserver(genericObserver);
    }

    /** Resets accumulated sums before each measurement iteration. */
    @Setup(Level.Iteration)
    public void resetIteration() {
      singleByteSizeObserver.reset();
      scaledByteSizeObserver.reset();
      primaryObserver.reset();
      secondaryObserver.reset();
      iterableLikeOuterObserver.reset();
      mixedOuterObserver.reset();
      mixedSecondaryObserver.reset();
      polyIterableOuterObserver.reset();
      polyObs1.reset();
      polyObs2.reset();
      polyObs3.reset();
      genericSum = 0L;
      mixedGenericSum = 0L;
    }
  }

  @Benchmark
  @OperationsPerInvocation(BATCH_SIZE)
  public long notifyValueReturned_noObservers(ObservableState state) {
    BenchIterator it = state.noObserverIterator;
    long[] sizes = state.sizes;
    for (int i = 0; i < sizes.length; i++) {
      it.fire(sizes[i]);
    }
    return it.hasChanged() ? 1L : 0L;
  }

  @Benchmark
  @OperationsPerInvocation(BATCH_SIZE)
  public long notifyValueReturned_singleElementByteSizeObserver(ObservableState state) {
    BenchIterator it = state.singleByteSizeIterator;
    long[] sizes = state.sizes;
    for (int i = 0; i < sizes.length; i++) {
      it.fire(sizes[i]);
    }
    state.singleByteSizeObserver.advance();
    return state.singleByteSizeObserver.takeReportedSum();
  }

  @Benchmark
  @OperationsPerInvocation(BATCH_SIZE)
  public long notifyValueReturned_singleElementByteSizeObserverScaled(ObservableState state) {
    BenchIterator it = state.scaledByteSizeIterator;
    long[] sizes = state.sizes;
    for (int i = 0; i < sizes.length; i++) {
      it.fire(sizes[i]);
    }
    state.scaledByteSizeObserver.advance();
    return state.scaledByteSizeObserver.takeReportedSum();
  }

  @Benchmark
  @OperationsPerInvocation(BATCH_SIZE)
  public long notifyValueReturned_twoElementByteSizeObservers(ObservableState state) {
    BenchIterator it = state.twoByteSizeIterator;
    long[] sizes = state.sizes;
    for (int i = 0; i < sizes.length; i++) {
      it.fire(sizes[i]);
    }
    state.primaryObserver.advance();
    state.secondaryObserver.advance();
    return state.primaryObserver.takeReportedSum() + state.secondaryObserver.takeReportedSum();
  }

  @Benchmark
  @OperationsPerInvocation(BATCH_SIZE)
  public long notifyValueReturned_iterableLikeCoderIteratorObserver(ObservableState state) {
    BenchIterator it = state.iterableLikeIterator;
    long[] sizes = state.sizes;
    for (int i = 0; i < sizes.length; i++) {
      it.fire(sizes[i]);
    }
    state.iterableLikeOuterObserver.advance();
    return state.iterableLikeOuterObserver.takeReportedSum();
  }

  @Benchmark
  @OperationsPerInvocation(BATCH_SIZE)
  public long notifyValueReturned_mixedMultiObserver(ObservableState state, Blackhole bh) {
    state.activeBlackhole = bh;
    BenchIterator it = state.mixedMultiIterator;
    long[] sizes = state.sizes;
    for (int i = 0; i < sizes.length; i++) {
      it.fire(sizes[i]);
    }
    state.mixedOuterObserver.advance();
    state.mixedSecondaryObserver.advance();
    long gSum = state.mixedGenericSum;
    state.mixedGenericSum = 0L;
    return state.mixedOuterObserver.takeReportedSum()
        + state.mixedSecondaryObserver.takeReportedSum()
        + gSum;
  }

  @Benchmark
  @OperationsPerInvocation(BATCH_SIZE)
  public long notifyValueReturned_polymorphicObservers(ObservableState state) {
    BenchIterator[] iterators = state.polymorphicIterators;
    long[] sizes = state.sizes;
    int count = iterators.length;
    for (int i = 0, idx = 0; i < sizes.length; i++) {
      iterators[idx].fire(sizes[i]);
      if (++idx == count) {
        idx = 0;
      }
    }
    state.polyIterableOuterObserver.advance();
    state.polyObs1.advance();
    state.polyObs2.advance();
    state.polyObs3.advance();
    return state.polyIterableOuterObserver.takeReportedSum()
        + state.polyObs1.takeReportedSum()
        + state.polyObs2.takeReportedSum()
        + state.polyObs3.takeReportedSum();
  }

  @Benchmark
  @OperationsPerInvocation(BATCH_SIZE)
  public long notifyValueReturned_singleGenericObserver(ObservableState state, Blackhole bh) {
    state.activeBlackhole = bh;
    BenchIterator it = state.genericObserverIterator;
    long[] sizes = state.sizes;
    for (int i = 0; i < sizes.length; i++) {
      it.fire(sizes[i]);
    }
    long sum = state.genericSum;
    state.genericSum = 0L;
    return sum;
  }

  @Benchmark
  public long iterableCreateAndIterate_noObservers(ObservableState state, Blackhole bh) {
    BenchIterable iterable = new BenchIterable(state.sizes, state.iterateLimit);
    BenchIterator it = iterable.iterator();
    long sum = 0L;
    while (it.hasNext()) {
      sum += it.next().intValue();
    }
    // Consume iterable and iterator so C2 cannot scalar-replace dead Observable/Vector overhead.
    bh.consume(iterable);
    bh.consume(it);
    return sum;
  }

  @Benchmark
  public long iterableCreateAndIterate_singleObserver(ObservableState state, Blackhole bh) {
    BenchIterable iterable = new BenchIterable(state.sizes, state.iterateLimit);
    AccumulatingByteSizeObserver observer = state.singleByteSizeObserver;
    iterable.addObserver(observer);
    BenchIterator it = iterable.iterator();
    long sum = 0L;
    while (it.hasNext()) {
      sum += it.next().intValue();
    }
    observer.advance();
    bh.consume(iterable);
    bh.consume(it);
    return sum + observer.takeReportedSum();
  }

  @Benchmark
  public long iterableCreateAndIterate_iterableLikeCoder(ObservableState state, Blackhole bh)
      throws Exception {
    BenchIterable iterable = new BenchIterable(state.sizes, state.iterateLimit);
    AccumulatingByteSizeObserver observer = state.iterableLikeOuterObserver;
    state.iterableCoder.registerByteSizeObserver(iterable, observer);
    BenchIterator it = iterable.iterator();
    long sum = 0L;
    while (it.hasNext()) {
      sum += it.next().intValue();
    }
    observer.advance();
    bh.consume(iterable);
    bh.consume(it);
    return sum + observer.takeReportedSum();
  }

  @Benchmark
  public long iterableCreateAndIterate_twoObservers(ObservableState state, Blackhole bh) {
    BenchIterable iterable = new BenchIterable(state.sizes, state.iterateLimit);
    AccumulatingByteSizeObserver o1 = state.primaryObserver;
    SecondaryByteSizeObserver o2 = state.secondaryObserver;
    iterable.addObserver(o1);
    iterable.addObserver(o2);
    BenchIterator it = iterable.iterator();
    long sum = 0L;
    while (it.hasNext()) {
      sum += it.next().intValue();
    }
    o1.advance();
    o2.advance();
    bh.consume(iterable);
    bh.consume(it);
    return sum + o1.takeReportedSum() + o2.takeReportedSum();
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  private static void registerObserverOpaque(
      IterableCoder<Integer> coder, BenchIterable iterable, ElementByteSizeObserver observer)
      throws Exception {
    coder.registerByteSizeObserver(iterable, observer);
  }

  /**
   * Multi-threaded {@code IterableLikeCoder} lifecycle + notification benchmark using opaque
   * registration and non-inlined {@link BenchIterator#fire(long)} boundaries across 4 threads so C2
   * cannot statically fold subtype checks, directly exposing {@code JDK-8180450} secondary
   * supertype cache contention.
   */
  @Benchmark
  @Threads(4)
  public long concurrentIterableLikeCoder_createAndIterate(ObservableState state, Blackhole bh)
      throws Exception {
    BenchIterable iterable = new BenchIterable(state.sizes, state.iterateLimit);
    AccumulatingByteSizeObserver observer = state.iterableLikeOuterObserver;
    registerObserverOpaque(state.iterableCoder, iterable, observer);
    BenchIterator it = iterable.iterator();
    long[] sizes = state.sizes;
    int limit = state.iterateLimit;
    for (int i = 0; i < limit; i++) {
      it.fire(sizes[i]);
    }
    observer.advance();
    bh.consume(iterable);
    bh.consume(it);
    return observer.takeReportedSum();
  }
}
