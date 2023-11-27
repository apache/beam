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
package org.apache.beam.runners.dataflow.worker.streaming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WeightBoundedQueueTest {
  private static final int MAX_WEIGHT = 10;

  @Test
  public void testPut_hasCapacity() {
    WeightedBoundedQueue<Integer> queue =
        WeightedBoundedQueue.create(MAX_WEIGHT, i -> Math.min(MAX_WEIGHT, i));

    int insertedValue = 1;

    queue.put(insertedValue);

    assertEquals(insertedValue, queue.queuedElementsWeight());
    assertEquals(1, queue.size());
    assertEquals(insertedValue, (int) queue.poll());
  }

  @Test
  public void testPut_noCapacity() throws InterruptedException {
    WeightedBoundedQueue<Integer> queue =
        WeightedBoundedQueue.create(MAX_WEIGHT, i -> Math.min(MAX_WEIGHT, i));

    // Insert value that takes all the capacity into the queue.
    queue.put(MAX_WEIGHT);

    // Try to insert another value into the queue. This will block since there is no capacity in the
    // queue.
    Thread putThread =
        new Thread(
            () -> {
              try {
                Thread.sleep(100);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              queue.put(MAX_WEIGHT);
            });
    putThread.start();

    // Should only see the first value in the queue, since the queue is at capacity.  thread2
    // should be blocked.
    assertEquals(MAX_WEIGHT, queue.queuedElementsWeight());
    assertEquals(1, queue.size());

    // Poll the queue, pulling off the only value inside and freeing up the capacity in the queue.
    queue.poll();

    // Wait for the putThread which was previously blocked due to the queue being at capacity.
    putThread.join();

    assertEquals(MAX_WEIGHT, queue.queuedElementsWeight());
    assertEquals(1, queue.size());
  }

  @Test
  public void testPoll() {
    WeightedBoundedQueue<Integer> queue =
        WeightedBoundedQueue.create(MAX_WEIGHT, i -> Math.min(MAX_WEIGHT, i));

    int insertedValue1 = 1;
    int insertedValue2 = 2;

    queue.put(insertedValue1);
    queue.put(insertedValue2);

    assertEquals(insertedValue1 + insertedValue2, queue.queuedElementsWeight());
    assertEquals(2, queue.size());
    assertEquals(insertedValue1, (int) queue.poll());
    assertEquals(1, queue.size());
  }

  @Test
  public void testPoll_withTimeout() throws InterruptedException {
    WeightedBoundedQueue<Integer> queue =
        WeightedBoundedQueue.create(MAX_WEIGHT, i -> Math.min(MAX_WEIGHT, i));
    int pollWaitTimeMillis = 10000;
    int insertedValue1 = 1;

    AtomicInteger pollResult = new AtomicInteger();
    Thread pollThread =
        new Thread(
            () -> {
              int polled;
              try {
                polled = queue.poll(pollWaitTimeMillis, TimeUnit.MILLISECONDS);
                pollResult.set(polled);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            });

    pollThread.start();
    Thread.sleep(pollWaitTimeMillis / 100);
    queue.put(insertedValue1);
    pollThread.join();

    assertEquals(insertedValue1, pollResult.get());
  }

  @Test
  public void testPoll_withTimeout_timesOut() throws InterruptedException {
    WeightedBoundedQueue<Integer> queue =
        WeightedBoundedQueue.create(MAX_WEIGHT, i -> Math.min(MAX_WEIGHT, i));
    int defaultPollResult = -10;
    int pollWaitTimeMillis = 100;
    int insertedValue1 = 1;

    // AtomicInteger default isn't null, so set it to a negative value and verify that it doesn't
    // change.
    AtomicInteger pollResult = new AtomicInteger(defaultPollResult);

    Thread pollThread =
        new Thread(
            () -> {
              int polled;
              try {
                polled = queue.poll(pollWaitTimeMillis, TimeUnit.MILLISECONDS);
                pollResult.set(polled);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            });

    pollThread.start();
    Thread.sleep(pollWaitTimeMillis * 100);
    queue.put(insertedValue1);
    pollThread.join();

    assertEquals(defaultPollResult, pollResult.get());
  }

  @Test
  public void testPoll_emptyQueue() {
    WeightedBoundedQueue<Integer> queue =
        WeightedBoundedQueue.create(MAX_WEIGHT, i -> Math.min(MAX_WEIGHT, i));

    assertNull(queue.poll());
  }

  @Test
  public void testTake() throws InterruptedException {
    WeightedBoundedQueue<Integer> queue =
        WeightedBoundedQueue.create(MAX_WEIGHT, i -> Math.min(MAX_WEIGHT, i));

    AtomicInteger value = new AtomicInteger();
    // Should block until value is available
    Thread takeThread =
        new Thread(
            () -> {
              try {
                value.set(queue.take());
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
            });
    takeThread.start();

    Thread.sleep(100);
    queue.put(MAX_WEIGHT);

    takeThread.join();

    assertEquals(MAX_WEIGHT, value.get());
  }
}
