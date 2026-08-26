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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
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
  public void testResultsAreDeliveredInSubmissionOrder() throws Exception {
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
    assertEquals(expected, delivered);
  }

  @Test
  public void testSubmitBlocksAtMaxInFlight() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    List<String> delivered = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      tasks.submit(
          () -> {
            release.await();
            return "blocked";
          },
          delivered::add);
    }
    // The fifth submit must wait for the oldest task; release it from another thread.
    Thread releaser =
        new Thread(
            () -> {
              try {
                Thread.sleep(200);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              release.countDown();
            });
    releaser.start();
    tasks.submit(() -> "fifth", delivered::add);
    releaser.join();
    tasks.awaitAll(delivered::add);
    assertEquals(Arrays.asList("blocked", "blocked", "blocked", "blocked", "fifth"), delivered);
  }

  /**
   * A runner may reuse a DoFn instance after a bundle fails. The failing bundle's outstanding tasks
   * must not surface in the next bundle, or their files would be registered twice.
   */
  @Test
  public void testFailedBundleLeavesNothingForTheNextBundle() throws Exception {
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
          throw new IllegalStateException("boom");
        },
        delivered::add);
    // awaitAll delivers in order: the slow task first, then the failure propagates.
    release.countDown();
    ExecutionException failure =
        assertThrows(ExecutionException.class, () -> tasks.awaitAll(delivered::add));
    assertTrue(failure.getCause() instanceof IllegalStateException);
    assertEquals(Arrays.asList("slow"), delivered);

    // next bundle: only its own results
    List<String> nextBundle = new ArrayList<>();
    tasks.submit(() -> "fresh", nextBundle::add);
    tasks.awaitAll(nextBundle::add);
    assertEquals(Arrays.asList("fresh"), nextBundle);
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
