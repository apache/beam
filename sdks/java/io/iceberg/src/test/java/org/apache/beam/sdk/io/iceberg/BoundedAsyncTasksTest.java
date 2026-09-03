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
package org.apache.beam.sdk.io.iceberg;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BoundedAsyncTasksTest {
  private final BoundedAsyncTasks<String> tasks = new BoundedAsyncTasks<>(2, 4);

  @After
  public void teardown() {
    tasks.shutdown();
  }

  @Test
  public void testConstructorValidatesArguments() {
    assertThrows(IllegalArgumentException.class, () -> new BoundedAsyncTasks<>(0, 4));
    assertThrows(IllegalArgumentException.class, () -> new BoundedAsyncTasks<>(2, 0));
  }

  @Test
  public void testAllResultsAreDelivered() throws Exception {
    List<String> delivered = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      String value = "t" + i;
      tasks.submit(() -> value, delivered::add);
    }
    tasks.awaitAll(delivered::add);
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      expected.add("t" + i);
    }
    assertThat(delivered, containsInAnyOrder(expected.toArray(new String[0])));
  }

  @Test
  public void testOutOfOrderCompletionDrainsFinishedTasks() throws Exception {
    CountDownLatch slowTaskHold = new CountDownLatch(1);
    CountDownLatch fastTaskDone = new CountDownLatch(1);
    List<String> delivered = new ArrayList<>();

    // Task 0: slow, waiting on latch
    tasks.submit(
        () -> {
          slowTaskHold.await();
          return "slow";
        },
        delivered::add);

    // Task 1: fast, signals when completed
    tasks.submit(
        () -> {
          fastTaskDone.countDown();
          return "fast";
        },
        delivered::add);

    // Wait until fast task has finished running in the background
    fastTaskDone.await();

    // Trigger a drain by submitting another task
    tasks.submit(() -> "noop", delivered::add);

    // "fast" should have been drained while "slow" is still blocked
    assertTrue(delivered.contains("fast"));
    assertFalse(delivered.contains("slow"));

    slowTaskHold.countDown();
    tasks.awaitAll(delivered::add);
    assertTrue(delivered.contains("slow"));
  }

  @Test
  public void testSubmitBlocksAtMaxInFlight() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch fifthExecuted = new CountDownLatch(1);
    CountDownLatch fifthReturned = new CountDownLatch(1);
    List<String> delivered = Collections.synchronizedList(new ArrayList<>());

    for (int i = 0; i < 4; i++) {
      tasks.submit(
          () -> {
            release.await();
            return "blocked";
          },
          delivered::add);
    }

    // The fifth submit must block because 4 tasks are already outstanding.
    Thread fifthSubmitter =
        new Thread(
            () -> {
              try {
                tasks.submit(
                    () -> {
                      fifthExecuted.countDown();
                      return "fifth";
                    },
                    delivered::add);
                fifthReturned.countDown();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    fifthSubmitter.start();

    // Verify submit() is blocked and has not completed while 4 tasks are in flight
    assertFalse(
        "submit() must block while maxInFlight tasks are outstanding",
        fifthReturned.await(50, TimeUnit.MILLISECONDS));
    assertEquals(
        "fifth task must not execute before oldest task completes", 1, fifthExecuted.getCount());

    // Release the blocked tasks; fifth submit should unblock and complete
    release.countDown();
    fifthReturned.await();
    fifthExecuted.await();
    fifthSubmitter.join();

    tasks.awaitAll(delivered::add);
    assertEquals(5, delivered.size());
    assertTrue(delivered.contains("fifth"));
  }

  @Test
  public void testAwaitAllFailureCancelsRemainingTasks() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    List<String> delivered = new ArrayList<>();
    tasks.submit(
        () -> {
          release.await();
          return "slow";
        },
        delivered::add);
    tasks.submit(
        () -> {
          release.await();
          throw new IllegalStateException("boom");
        },
        delivered::add);
    // Submit a trailing task that would remain in the queue if awaitAll does not clean up on
    // failure
    tasks.submit(() -> "trailing", delivered::add);

    // awaitAll delivers in order: the slow task first, then the failure propagates.
    release.countDown();
    ExecutionException failure =
        assertThrows(ExecutionException.class, () -> tasks.awaitAll(delivered::add));
    assertTrue(failure.getCause() instanceof IllegalStateException);
    assertEquals(Arrays.asList("slow"), delivered);

    // Subsequent submissions on this instance should only see their own results, not "trailing"
    List<String> freshBatch = new ArrayList<>();
    tasks.submit(() -> "fresh", freshBatch::add);
    tasks.awaitAll(freshBatch::add);
    assertEquals(Arrays.asList("fresh"), freshBatch);
  }

  @Test
  public void testFailureOnSubmitCancelsOutstandingTasks() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    List<String> delivered = new ArrayList<>();
    // The failure surfaces from whichever submit first drains the failed task.
    boolean failed = false;
    try {
      tasks.submit(
          () -> {
            throw new IllegalStateException("boom");
          },
          delivered::add);
      for (int i = 0; i < 5; i++) {
        tasks.submit(
            () -> {
              release.await();
              return "never";
            },
            delivered::add);
      }
    } catch (ExecutionException e) {
      failed = true;
    }
    assertTrue(failed);
    release.countDown();
    tasks.awaitAll(delivered::add);
    assertTrue(delivered.isEmpty());
  }
}
