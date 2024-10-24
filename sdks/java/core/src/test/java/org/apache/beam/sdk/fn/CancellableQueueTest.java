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
package org.apache.beam.sdk.fn;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.beam.sdk.fn.test.TestExecutors;
import org.apache.beam.sdk.fn.test.TestExecutors.TestExecutorService;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CancellableQueue}. */
@RunWith(JUnit4.class)
public class CancellableQueueTest {
  @Rule
  public final TestExecutorService executor = TestExecutors.from(Executors::newCachedThreadPool);

  private static final int MAX_ELEMENTS = 10_000;

  @Test(timeout = 10_000)
  public void runTestForMultipleConsumersAndProducers() throws Exception {
    CancellableQueue<String> queue = new CancellableQueue<>(100);
    runTestForMultipleConsumersAndProducers(queue);
    queue.reset();
    runTestForMultipleConsumersAndProducers(queue);
  }

  public void runTestForMultipleConsumersAndProducers(CancellableQueue<String> queue)
      throws Exception {
    List<Future<?>> futures = new ArrayList<>();
    // Start some producers
    futures.add(
        executor.submit(
            () -> {
              for (int i = 0; i < MAX_ELEMENTS; i++) {
                queue.put("A" + i);
              }
              return null;
            }));
    futures.add(
        executor.submit(
            () -> {
              for (int i = 0; i < MAX_ELEMENTS; i++) {
                queue.put("B" + i);
              }
              return null;
            }));
    futures.add(
        executor.submit(
            () -> {
              for (int i = 0; i < MAX_ELEMENTS; i++) {
                queue.put("C" + i);
              }
              return null;
            }));

    // Start some consumers
    List<String> valuesReadX = new ArrayList<>();
    futures.add(
        executor.submit(
            () -> {
              for (int i = 0; i < MAX_ELEMENTS; i++) {
                valuesReadX.add(queue.take());
              }
              return null;
            }));
    List<String> valuesReadY = new ArrayList<>();
    futures.add(
        executor.submit(
            () -> {
              for (int i = 0; i < MAX_ELEMENTS; i++) {
                valuesReadY.add(queue.take());
              }
              return null;
            }));
    List<String> valuesReadZ = new ArrayList<>();
    futures.add(
        executor.submit(
            () -> {
              for (int i = 0; i < MAX_ELEMENTS; i++) {
                valuesReadZ.add(queue.take());
              }
              return null;
            }));

    for (Future<?> future : futures) {
      future.get();
    }

    assertEquals(3 * MAX_ELEMENTS, valuesReadX.size() + valuesReadY.size() + valuesReadZ.size());

    Set<String> allValues = new HashSet<>();
    allValues.addAll(valuesReadX);
    allValues.addAll(valuesReadY);
    allValues.addAll(valuesReadZ);
    Set<String> expectedValues = new HashSet<>();
    for (int i = 0; i < MAX_ELEMENTS; ++i) {
      expectedValues.add("A" + i);
      expectedValues.add("B" + i);
      expectedValues.add("C" + i);
    }

    // Use set difference to print what was unexpected instead of containsInAnyOrder
    assertThat(Sets.difference(allValues, expectedValues), empty());
    assertThat(Sets.difference(expectedValues, allValues), empty());
  }

  @Test(timeout = 10_000)
  public void testCancellation() throws Exception {
    CancellableQueue<String> queue = new CancellableQueue<>(100);
    List<Future<?>> futures = new ArrayList<>();
    // Start some producers that won't finish.
    futures.add(
        executor.submit(
            () -> {
              while (true) {
                queue.put("A");
              }
            }));
    futures.add(
        executor.submit(
            () -> {
              while (true) {
                queue.put("B");
              }
            }));
    futures.add(
        executor.submit(
            () -> {
              while (true) {
                queue.put("C");
              }
            }));

    // Start some consumers where one at random will cancel the queue due to thread timing
    futures.add(
        executor.submit(
            () -> {
              for (int i = 0; i < MAX_ELEMENTS; i++) {
                queue.take();
              }
              queue.cancel(new IllegalStateException("test cancel"));
              queue.take();
              return null;
            }));
    futures.add(
        executor.submit(
            () -> {
              for (int i = 0; i < MAX_ELEMENTS; i++) {
                queue.take();
              }
              queue.cancel(new IllegalStateException("test cancel"));
              queue.take();
              return null;
            }));
    futures.add(
        executor.submit(
            () -> {
              for (int i = 0; i < MAX_ELEMENTS; i++) {
                queue.take();
              }
              queue.cancel(new IllegalStateException("test cancel"));
              queue.take();
              return null;
            }));

    for (Future<?> future : futures) {
      assertThrows(
          "test cancel",
          IllegalStateException.class,
          () -> {
            try {
              future.get();
            } catch (ExecutionException e) {
              // We have to get the underlying cause of the execution as it is wrapped.
              throw e.getCause();
            }
          });
    }

    // Ensure that after cancellation the queue can be reset and re-used.
    queue.reset();
    runTestForMultipleConsumersAndProducers(queue);
  }

  @Test(timeout = 10_000)
  public void testFirstCancellationError() throws Exception {
    CancellableQueue<String> queue = new CancellableQueue<>(100);
    queue.cancel(new RuntimeException("First cancel exception"));
    assertThrows("First cancel exception", RuntimeException.class, () -> queue.take());
    queue.cancel(new RuntimeException("Second cancel exception"));
    assertThrows("First cancel exception", RuntimeException.class, () -> queue.take());
  }

  @Test
  public void testMemoryReferenceOnTake() throws Exception {
    String s1 = new String("test1");
    String s2 = new String("test2");
    WeakReference<String> weakReference1 = new WeakReference<>(s1);
    WeakReference<String> weakReference2 = new WeakReference<>(s2);
    CancellableQueue<String> queue = new CancellableQueue<>(100);
    queue.put(s1);
    queue.put(s2);
    s1 = null;
    s2 = null;
    System.gc();
    assertTrue(weakReference1.get() != null);
    assertTrue(weakReference2.get() != null);

    assertEquals("test1", queue.take());
    System.gc();
    assertTrue(weakReference1.get() == null);
    assertTrue(weakReference2.get() != null);

    queue.reset();
    System.gc();
    assertTrue(weakReference1.get() == null);
    assertTrue(weakReference2.get() == null);
  }
}
