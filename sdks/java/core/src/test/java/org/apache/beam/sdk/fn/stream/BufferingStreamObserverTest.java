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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.fn.test.TestExecutors;
import org.apache.beam.sdk.fn.test.TestExecutors.TestExecutorService;
import org.apache.beam.sdk.fn.test.TestStreams;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BufferingStreamObserver}. */
@RunWith(JUnit4.class)
public class BufferingStreamObserverTest {
  @Rule public TestExecutorService executor = TestExecutors.from(Executors.newCachedThreadPool());

  @Test
  public void testThreadSafety() throws Exception {
    final List<String> onNextValues = new ArrayList<>();
    AdvancingPhaser phaser = new AdvancingPhaser(1);
    final AtomicBoolean isCriticalSectionShared = new AtomicBoolean();
    final BufferingStreamObserver<String> streamObserver =
        new BufferingStreamObserver<>(
            phaser,
            TestStreams.withOnNext(
                    (String t) -> {
                      // Use the atomic boolean to detect if multiple threads are in this
                      // critical section. Any thread that enters purposefully blocks by sleeping
                      // to increase the contention between threads artificially.
                      assertFalse(isCriticalSectionShared.getAndSet(true));
                      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
                      onNextValues.add(t);
                      assertTrue(isCriticalSectionShared.getAndSet(false));
                    })
                .build(),
            executor,
            3);

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
    List<Future<String>> results = executor.invokeAll(tasks);
    for (Future<String> result : results) {
      result.get();
    }
    streamObserver.onCompleted();

    // Check that order was maintained.
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
    final BufferingStreamObserver<String> streamObserver =
        new BufferingStreamObserver<>(
            phaser,
            TestStreams.withOnNext((String t) -> assertTrue(elementsAllowed.get()))
                .withIsReady(elementsAllowed::get)
                .build(),
            executor,
            3);

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
    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
    elementsAllowed.set(true);
    phaser.arrive();

    for (Future<String> result : results) {
      result.get();
    }
    streamObserver.onCompleted();
  }
}
