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
package org.apache.beam.sdk.fn.stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.fn.test.TestExecutors;
import org.apache.beam.sdk.fn.test.TestExecutors.TestExecutorService;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ArrayListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DirectStreamObserver}. */
@RunWith(JUnit4.class)
public class DirectStreamObserverTest {
  @Rule public TestExecutorService executor = TestExecutors.from(Executors.newCachedThreadPool());

  @Test
  public void testThreadSafety() throws Exception {
    final List<String> onNextValues = new ArrayList<>();
    AdvancingPhaser phaser = new AdvancingPhaser(1);
    final AtomicBoolean isCriticalSectionShared = new AtomicBoolean();
    final DirectStreamObserver<String> streamObserver =
        new DirectStreamObserver<>(
            phaser,
            TestStreams.withOnNext(
                    (String t) -> {
                      // Use the atomic boolean to detect if multiple threads are in this
                      // critical section. Any thread that enters purposefully blocks by sleeping
                      // to increase the contention between threads artificially.
                      assertFalse(isCriticalSectionShared.getAndSet(true));
                      Uninterruptibles.sleepUninterruptibly(50, TimeUnit.MILLISECONDS);
                      onNextValues.add(t);
                      assertTrue(isCriticalSectionShared.getAndSet(false));
                    })
                .build());

    List<String> prefixes = ImmutableList.of("0", "1", "2", "3", "4");
    List<Callable<String>> tasks = new ArrayList<>();
    for (final String prefix : prefixes) {
      tasks.add(
          () -> {
            for (int i = 0; i < 10; i++) {
              streamObserver.onNext(prefix + i);
            }
            return prefix;
          });
    }
    executor.invokeAll(tasks);
    streamObserver.onCompleted();

    // Check that order was maintained per writer.
    int[] prefixesIndex = new int[prefixes.size()];
    assertEquals(50, onNextValues.size());
    for (String onNextValue : onNextValues) {
      int prefix = Integer.parseInt(onNextValue.substring(0, 1));
      int suffix = Integer.parseInt(onNextValue.substring(1, 2));
      assertEquals(prefixesIndex[prefix], suffix);
      prefixesIndex[prefix] += 1;
    }
  }

  @Test
  public void testIsReadyIsHonored() throws Exception {
    AdvancingPhaser phaser = new AdvancingPhaser(1);
    final AtomicBoolean elementsAllowed = new AtomicBoolean();
    final DirectStreamObserver<String> streamObserver =
        new DirectStreamObserver<>(
            phaser,
            TestStreams.withOnNext((String t) -> assertTrue(elementsAllowed.get()))
                .withIsReady(elementsAllowed::get)
                .build(),
            0);

    // Start all the tasks
    List<Future<String>> results = new ArrayList<>();
    for (final String prefix : ImmutableList.of("0", "1", "2", "3", "4")) {
      results.add(
          executor.submit(
              () -> {
                for (int i = 0; i < 10; i++) {
                  streamObserver.onNext(prefix + i);
                }
                return prefix;
              }));
    }

    // Have them wait and then flip that we do allow elements and wake up those awaiting
    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    elementsAllowed.set(true);
    phaser.arrive();

    for (Future<String> result : results) {
      result.get();
    }
    streamObserver.onCompleted();
  }

  @Test
  public void testIsReadyIsHonoredTermination() throws Exception {
    AdvancingPhaser phaser = new AdvancingPhaser(1);
    final AtomicBoolean elementsAllowed = new AtomicBoolean();
    final DirectStreamObserver<String> streamObserver =
        new DirectStreamObserver<>(
            phaser,
            TestStreams.withOnNext(
                    (String t) -> {
                      if (phaser.isTerminated()) {
                        throw new RuntimeException("Test stream terminated.");
                      }
                      assertTrue(elementsAllowed.get());
                    })
                .withIsReady(elementsAllowed::get)
                .build(),
            0);

    // Start all the tasks
    List<Future<String>> results = new ArrayList<>();
    for (final String prefix : ImmutableList.of("0", "1", "2", "3", "4")) {
      results.add(
          executor.submit(
              () -> {
                for (int i = 0; i < 10; i++) {
                  streamObserver.onNext(prefix + i);
                }
                return prefix;
              }));
    }

    // Have them wait and then terminate the phaser and ensure sends occur.
    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    phaser.forceTermination();

    Assert.assertThrows(
        "Test stream terminated.", RuntimeException.class, () -> streamObserver.onNext("100"));

    for (Future<String> result : results) {
      Assert.assertThrows(ExecutionException.class, () -> result.get());
    }
    streamObserver.onCompleted();
  }

  /**
   * This test specifically covers the case if the outbound observer is being invoked on the same
   * thread that the inbound observer is. gRPC documentation states:
   *
   * <p><i>Note: the onReadyHandler's invocation is serialized on the same thread pool as the
   * incoming StreamObserver's onNext(), onError(), and onComplete() handlers. Blocking the
   * onReadyHandler will prevent additional messages from being processed by the incoming
   * StreamObserver. The onReadyHandler must return in a timely manor or else message processing
   * throughput will suffer. </i>
   */
  @Test
  public void testIsReadyCheckDoesntBlockIfPhaserCallbackNeverHappens() throws Exception {
    // Note that we never advance the phaser in this test.
    final AtomicBoolean elementsAllowed = new AtomicBoolean();
    final DirectStreamObserver<String> streamObserver =
        new DirectStreamObserver<>(
            new AdvancingPhaser(1),
            TestStreams.withOnNext((String t) -> assertTrue(elementsAllowed.get()))
                .withIsReady(elementsAllowed::get)
                .build(),
            0);

    // Start all the tasks
    List<Future<String>> results = new ArrayList<>();
    for (final String prefix : ImmutableList.of("0", "1", "2", "3", "4")) {
      results.add(
          executor.submit(
              () -> {
                for (int i = 0; i < 10; i++) {
                  streamObserver.onNext(prefix + i);
                }
                return prefix;
              }));
    }

    // Have them wait and then flip that we do allow elements and wake up those awaiting
    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
    elementsAllowed.set(true);

    for (Future<String> result : results) {
      result.get();
    }
    streamObserver.onCompleted();
  }

  @Test
  public void testMessageCheckInterval() throws Exception {
    final AtomicInteger index = new AtomicInteger();
    ArrayListMultimap<Integer, String> values = ArrayListMultimap.create();
    final DirectStreamObserver<String> streamObserver =
        new DirectStreamObserver<>(
            new AdvancingPhaser(1),
            TestStreams.withOnNext((String t) -> assertTrue(values.put(index.get(), t)))
                .withIsReady(
                    () -> {
                      index.incrementAndGet();
                      return true;
                    })
                .build(),
            10);

    List<String> prefixes = ImmutableList.of("0", "1", "2", "3", "4");
    List<Future<String>> results = new ArrayList<>();
    for (final String prefix : prefixes) {
      results.add(
          executor.submit(
              () -> {
                for (int i = 0; i < 10; i++) {
                  streamObserver.onNext(prefix + i);
                }
                return prefix;
              }));
    }
    for (Future<?> result : results) {
      result.get();
    }
    assertEquals(50, values.size());
    for (Collection<String> valuesPerMessageCheck : values.asMap().values()) {
      assertThat(valuesPerMessageCheck, hasSize(10));
    }

    // Check that order was maintained per writer.
    int[] prefixesIndex = new int[prefixes.size()];
    for (String onNextValue : values.values()) {
      int prefix = Integer.parseInt(onNextValue.substring(0, 1));
      int suffix = Integer.parseInt(onNextValue.substring(1, 2));
      assertEquals(prefixesIndex[prefix], suffix);
      prefixesIndex[prefix] += 1;
    }
  }

  @Test
  public void testPhaserTermination() throws Exception {
    final AtomicInteger index = new AtomicInteger();
    ArrayListMultimap<Integer, String> values = ArrayListMultimap.create();
    final DirectStreamObserver<String> streamObserver =
        new DirectStreamObserver<>(
            new AdvancingPhaser(1),
            TestStreams.withOnNext((String t) -> assertTrue(values.put(index.get(), t)))
                .withIsReady(
                    () -> {
                      index.incrementAndGet();
                      return true;
                    })
                .build(),
            10);

    List<String> prefixes = ImmutableList.of("0", "1", "2", "3", "4");
    List<Future<String>> results = new ArrayList<>();
    for (final String prefix : prefixes) {
      results.add(
          executor.submit(
              () -> {
                for (int i = 0; i < 10; i++) {
                  streamObserver.onNext(prefix + i);
                }
                return prefix;
              }));
    }
    for (Future<?> result : results) {
      result.get();
    }
    assertEquals(50, values.size());
    for (Collection<String> valuesPerMessageCheck : values.asMap().values()) {
      assertThat(valuesPerMessageCheck, hasSize(10));
    }

    // Check that order was maintained per writer.
    int[] prefixesIndex = new int[prefixes.size()];
    for (String onNextValue : values.values()) {
      int prefix = Integer.parseInt(onNextValue.substring(0, 1));
      int suffix = Integer.parseInt(onNextValue.substring(1, 2));
      assertEquals(prefixesIndex[prefix], suffix);
      prefixesIndex[prefix] += 1;
    }
  }
}
