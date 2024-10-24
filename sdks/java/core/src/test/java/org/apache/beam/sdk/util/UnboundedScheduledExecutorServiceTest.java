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
package org.apache.beam.sdk.util;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.util.UnboundedScheduledExecutorService.ScheduledFutureTask;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.hamcrest.core.IsIterableContaining;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link UnboundedScheduledExecutorService}. */
@RunWith(JUnit4.class)
public class UnboundedScheduledExecutorServiceTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(UnboundedScheduledExecutorServiceTest.class);

  private static final Runnable RUNNABLE =
      () -> {
        // no-op
      };
  private static final Callable<String> CALLABLE = () -> "A";

  private static final Callable<String> FAILING_CALLABLE =
      () -> {
        throw new Exception("Test");
      };

  @Test
  public void testScheduleMethodErrorChecking() throws Exception {
    FastNanoClockAndSleeper fastNanoClockAndSleeper = new FastNanoClockAndSleeper();
    UnboundedScheduledExecutorService executorService =
        new UnboundedScheduledExecutorService(fastNanoClockAndSleeper);
    UnboundedScheduledExecutorService shutdownExecutorService =
        new UnboundedScheduledExecutorService(fastNanoClockAndSleeper);
    shutdownExecutorService.shutdown();

    assertThrows(
        NullPointerException.class, () -> executorService.schedule((Runnable) null, 10, SECONDS));
    assertThrows(NullPointerException.class, () -> executorService.schedule(RUNNABLE, 10, null));
    assertThrows(
        RejectedExecutionException.class,
        () -> shutdownExecutorService.schedule(RUNNABLE, 10, SECONDS));

    assertThrows(
        NullPointerException.class,
        () -> executorService.schedule((Callable<String>) null, 10, SECONDS));
    assertThrows(NullPointerException.class, () -> executorService.schedule(CALLABLE, 10, null));
    assertThrows(
        RejectedExecutionException.class,
        () -> shutdownExecutorService.schedule(CALLABLE, 10, SECONDS));

    assertThrows(
        NullPointerException.class,
        () -> executorService.scheduleAtFixedRate(null, 10, 10, SECONDS));
    assertThrows(
        NullPointerException.class,
        () -> executorService.scheduleAtFixedRate(RUNNABLE, 10, 10, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> executorService.scheduleAtFixedRate(RUNNABLE, 10, -10, SECONDS));
    assertThrows(
        RejectedExecutionException.class,
        () -> shutdownExecutorService.scheduleAtFixedRate(RUNNABLE, 10, 10, SECONDS));

    assertThrows(
        NullPointerException.class,
        () -> executorService.scheduleWithFixedDelay((Runnable) null, 10, 10, SECONDS));
    assertThrows(
        NullPointerException.class,
        () -> executorService.scheduleWithFixedDelay(RUNNABLE, 10, 10, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> executorService.scheduleWithFixedDelay(RUNNABLE, 10, -10, SECONDS));
    assertThrows(
        RejectedExecutionException.class,
        () -> shutdownExecutorService.scheduleWithFixedDelay(RUNNABLE, 10, 10, SECONDS));

    assertThat(executorService.shutdownNow(), empty());
    assertThat(executorService.shutdownNow(), empty());
  }

  @Test
  public void testSubmitMethodErrorChecking() throws Exception {
    FastNanoClockAndSleeper fastNanoClockAndSleeper = new FastNanoClockAndSleeper();
    UnboundedScheduledExecutorService executorService =
        new UnboundedScheduledExecutorService(fastNanoClockAndSleeper);
    UnboundedScheduledExecutorService shutdownExecutorService =
        new UnboundedScheduledExecutorService(fastNanoClockAndSleeper);
    shutdownExecutorService.shutdown();

    assertThrows(NullPointerException.class, () -> executorService.submit(null, "result"));
    assertThrows(
        RejectedExecutionException.class, () -> shutdownExecutorService.submit(RUNNABLE, "result"));

    assertThrows(NullPointerException.class, () -> executorService.submit((Runnable) null));
    assertThrows(RejectedExecutionException.class, () -> shutdownExecutorService.submit(RUNNABLE));

    assertThrows(NullPointerException.class, () -> executorService.submit((Callable<String>) null));
    assertThrows(RejectedExecutionException.class, () -> shutdownExecutorService.submit(CALLABLE));

    assertThat(executorService.shutdownNow(), empty());
    assertThat(executorService.shutdownNow(), empty());
  }

  @Test
  public void testInvokeMethodErrorChecking() throws Exception {
    FastNanoClockAndSleeper fastNanoClockAndSleeper = new FastNanoClockAndSleeper();
    UnboundedScheduledExecutorService executorService =
        new UnboundedScheduledExecutorService(fastNanoClockAndSleeper);
    UnboundedScheduledExecutorService shutdownExecutorService =
        new UnboundedScheduledExecutorService(fastNanoClockAndSleeper);
    shutdownExecutorService.shutdown();

    assertThrows(NullPointerException.class, () -> executorService.invokeAll(null));
    assertThrows(
        NullPointerException.class, () -> executorService.invokeAll(Collections.singleton(null)));
    assertThrows(
        RejectedExecutionException.class,
        () -> shutdownExecutorService.invokeAll(Collections.singleton(CALLABLE)));

    assertThrows(NullPointerException.class, () -> executorService.invokeAll(null, 10, SECONDS));
    assertThrows(
        NullPointerException.class,
        () -> executorService.invokeAll(Collections.singleton(null), 10, SECONDS));
    assertThrows(
        NullPointerException.class,
        () -> executorService.invokeAll(Collections.singleton(CALLABLE), 10, null));
    assertThrows(
        RejectedExecutionException.class,
        () -> shutdownExecutorService.invokeAll(Collections.singleton(CALLABLE), 10, SECONDS));

    assertThrows(NullPointerException.class, () -> executorService.invokeAny(null));
    assertThrows(
        NullPointerException.class, () -> executorService.invokeAny(Collections.singleton(null)));
    assertThrows(
        IllegalArgumentException.class, () -> executorService.invokeAny(Collections.emptyList()));
    assertThrows(
        ExecutionException.class,
        () -> executorService.invokeAny(Arrays.asList(FAILING_CALLABLE, FAILING_CALLABLE)));
    assertThrows(
        RejectedExecutionException.class,
        () -> shutdownExecutorService.invokeAny(Collections.singleton(CALLABLE)));

    assertThrows(NullPointerException.class, () -> executorService.invokeAny(null, 10, SECONDS));
    assertThrows(
        NullPointerException.class,
        () -> executorService.invokeAny(Collections.singleton(null), 10, SECONDS));
    assertThrows(
        NullPointerException.class,
        () -> executorService.invokeAny(Collections.singleton(CALLABLE), 10, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> executorService.invokeAny(Collections.emptyList(), 10, SECONDS));
    assertThrows(
        ExecutionException.class,
        () ->
            executorService.invokeAny(
                Arrays.asList(FAILING_CALLABLE, FAILING_CALLABLE), 10, SECONDS));
    assertThrows(
        RejectedExecutionException.class,
        () -> shutdownExecutorService.invokeAny(Collections.singleton(CALLABLE), 10, SECONDS));

    assertThat(executorService.shutdownNow(), empty());
    assertThat(executorService.shutdownNow(), empty());
  }

  @Test
  public void testExecuteMethodErrorChecking() throws Exception {
    FastNanoClockAndSleeper fastNanoClockAndSleeper = new FastNanoClockAndSleeper();
    UnboundedScheduledExecutorService executorService =
        new UnboundedScheduledExecutorService(fastNanoClockAndSleeper);
    UnboundedScheduledExecutorService shutdownExecutorService =
        new UnboundedScheduledExecutorService(fastNanoClockAndSleeper);
    shutdownExecutorService.shutdown();

    assertThrows(NullPointerException.class, () -> executorService.execute(null));
    assertThrows(RejectedExecutionException.class, () -> shutdownExecutorService.execute(RUNNABLE));

    assertThat(executorService.shutdownNow(), empty());
    assertThat(executorService.shutdownNow(), empty());
  }

  @Test
  public void testAllMethodsReturnScheduledFutures() throws Exception {
    FastNanoClockAndSleeper fastNanoClockAndSleeper = new FastNanoClockAndSleeper();
    UnboundedScheduledExecutorService executorService =
        new UnboundedScheduledExecutorService(fastNanoClockAndSleeper);

    assertThat(executorService.submit(RUNNABLE), instanceOf(ScheduledFutureTask.class));
    assertThat(executorService.submit(CALLABLE), instanceOf(ScheduledFutureTask.class));
    assertThat(executorService.submit(RUNNABLE, "Answer"), instanceOf(ScheduledFutureTask.class));

    assertThat(
        executorService.schedule(RUNNABLE, 10, SECONDS), instanceOf(ScheduledFutureTask.class));
    assertThat(
        executorService.schedule(CALLABLE, 10, SECONDS), instanceOf(ScheduledFutureTask.class));
    assertThat(
        executorService.scheduleAtFixedRate(RUNNABLE, 10, 10, SECONDS),
        instanceOf(ScheduledFutureTask.class));
    assertThat(
        executorService.scheduleWithFixedDelay(RUNNABLE, 10, 10, SECONDS),
        instanceOf(ScheduledFutureTask.class));

    assertThat(
        executorService.invokeAll(Arrays.asList(CALLABLE, CALLABLE)),
        IsIterableContainingInOrder.contains(
            instanceOf(ScheduledFutureTask.class), instanceOf(ScheduledFutureTask.class)));
    assertThat(
        executorService.invokeAll(Arrays.asList(CALLABLE, CALLABLE), 10, SECONDS),
        IsIterableContainingInOrder.contains(
            instanceOf(ScheduledFutureTask.class), instanceOf(ScheduledFutureTask.class)));

    executorService.shutdownNow();
  }

  @Test
  public void testShutdown() throws Exception {
    FastNanoClockAndSleeper fastNanoClockAndSleeper = new FastNanoClockAndSleeper();
    UnboundedScheduledExecutorService executorService =
        new UnboundedScheduledExecutorService(fastNanoClockAndSleeper);

    Runnable runnable1 = Mockito.mock(Runnable.class);
    Runnable runnable2 = Mockito.mock(Runnable.class);
    Runnable runnable3 = Mockito.mock(Runnable.class);
    Callable<?> callable1 = Mockito.mock(Callable.class);

    Future<?> rFuture1 = executorService.schedule(runnable1, 10, SECONDS);
    Future<?> cFuture1 = executorService.schedule(callable1, 10, SECONDS);
    Future<?> rFuture2 = executorService.scheduleAtFixedRate(runnable2, 10, 10, SECONDS);
    Future<?> rFuture3 = executorService.scheduleWithFixedDelay(runnable3, 10, 10, SECONDS);

    assertThat(
        executorService.shutdownNow(),
        IsIterableContaining.hasItems(
            (Runnable) rFuture1, (Runnable) rFuture2, (Runnable) rFuture3, (Runnable) cFuture1));
    verifyNoInteractions(runnable1, runnable2, runnable3, callable1);

    assertTrue(executorService.isShutdown());
    assertTrue(executorService.awaitTermination(10, SECONDS));
    assertTrue(executorService.isTerminated());
  }

  @Test
  public void testExecute() throws Exception {
    FastNanoClockAndSleeper fastNanoClockAndSleeper = new FastNanoClockAndSleeper();
    UnboundedScheduledExecutorService executorService =
        new UnboundedScheduledExecutorService(fastNanoClockAndSleeper);

    AtomicInteger callCount = new AtomicInteger();
    CountDownLatch countDownLatch = new CountDownLatch(1);

    executorService.execute(
        () -> {
          callCount.incrementAndGet();
          countDownLatch.countDown();
        });

    countDownLatch.await();
    assertEquals(1, callCount.get());
  }

  @Test
  public void testSubmit() throws Exception {
    List<AtomicInteger> callCounts = new ArrayList<>();
    List<ScheduledFutureTask<?>> futures = new ArrayList<>();

    FastNanoClockAndSleeper fastNanoClockAndSleeper = new FastNanoClockAndSleeper();
    UnboundedScheduledExecutorService executorService =
        new UnboundedScheduledExecutorService(fastNanoClockAndSleeper);

    callCounts.add(new AtomicInteger());
    futures.add(
        (ScheduledFutureTask<?>)
            executorService.submit(
                (Runnable) callCounts.get(callCounts.size() - 1)::incrementAndGet));
    callCounts.add(new AtomicInteger());
    futures.add(
        (ScheduledFutureTask<?>)
            executorService.submit(
                callCounts.get(callCounts.size() - 1)::incrementAndGet, "Result"));
    callCounts.add(new AtomicInteger());
    futures.add(
        (ScheduledFutureTask<?>)
            executorService.submit(callCounts.get(callCounts.size() - 1)::incrementAndGet));

    assertNull(futures.get(0).get());
    assertEquals("Result", futures.get(1).get());
    assertEquals(1, futures.get(2).get());

    for (int i = 0; i < callCounts.size(); ++i) {
      assertFalse(futures.get(i).isPeriodic());
      assertEquals(1, callCounts.get(i).get());
    }
  }

  @Test
  public void testSchedule() throws Exception {
    List<AtomicInteger> callCounts = new ArrayList<>();
    List<ScheduledFutureTask<?>> futures = new ArrayList<>();

    FastNanoClockAndSleeper fastNanoClockAndSleeper = new FastNanoClockAndSleeper();
    UnboundedScheduledExecutorService executorService =
        new UnboundedScheduledExecutorService(fastNanoClockAndSleeper);

    callCounts.add(new AtomicInteger());
    futures.add(
        (ScheduledFutureTask<?>)
            executorService.schedule(
                (Runnable) callCounts.get(callCounts.size() - 1)::incrementAndGet,
                100,
                MILLISECONDS));
    callCounts.add(new AtomicInteger());
    futures.add(
        (ScheduledFutureTask<?>)
            executorService.schedule(
                callCounts.get(callCounts.size() - 1)::incrementAndGet, 100, MILLISECONDS));

    // No tasks should have been picked up
    wakeUpAndCheckTasks(executorService);
    for (int i = 0; i < callCounts.size(); ++i) {
      assertEquals(0, callCounts.get(i).get());
    }

    // No tasks should have been picked up even if the time advances 99 seconds
    fastNanoClockAndSleeper.sleep(99);
    wakeUpAndCheckTasks(executorService);
    for (int i = 0; i < callCounts.size(); ++i) {
      assertEquals(0, callCounts.get(i).get());
    }

    // All tasks should wake up and pick-up tasks
    fastNanoClockAndSleeper.sleep(1);
    wakeUpAndCheckTasks(executorService);

    assertNull(futures.get(0).get());
    assertEquals(1, futures.get(1).get());

    for (int i = 0; i < callCounts.size(); ++i) {
      assertFalse(futures.get(i).isPeriodic());
      assertEquals(1, callCounts.get(i).get());
    }

    assertThat(executorService.shutdownNow(), empty());
  }

  @Test
  public void testSchedulePeriodicWithFixedDelay() throws Exception {
    FastNanoClockAndSleeper fastNanoClockAndSleeper = new FastNanoClockAndSleeper();
    UnboundedScheduledExecutorService executorService =
        new UnboundedScheduledExecutorService(fastNanoClockAndSleeper);

    AtomicInteger callCount = new AtomicInteger();
    CountDownLatch latch = new CountDownLatch(1);

    ScheduledFutureTask<?> future =
        (ScheduledFutureTask<?>)
            executorService.scheduleWithFixedDelay(
                () -> {
                  callCount.incrementAndGet();
                  latch.countDown();
                },
                100,
                50,
                MILLISECONDS);

    // No tasks should have been picked up
    wakeUpAndCheckTasks(executorService);
    assertEquals(0, callCount.get());

    // No tasks should have been picked up even if the time advances 99 seconds
    fastNanoClockAndSleeper.sleep(99);
    wakeUpAndCheckTasks(executorService);
    assertEquals(0, callCount.get());

    // We should have picked up the task 1 time, next task should be scheduled in 50 even though we
    // advanced to 109
    fastNanoClockAndSleeper.sleep(10);
    wakeUpAndCheckTasks(executorService);
    latch.await();
    assertEquals(1, callCount.get());

    for (; ; ) {
      synchronized (executorService.tasks) {
        ScheduledFutureTask<?> task = executorService.tasks.peek();
        if (task != null) {
          assertEquals(50, task.getDelay(MILLISECONDS));
          break;
        }
      }
      Thread.sleep(1);
    }

    assertTrue(future.isPeriodic());
    assertFalse(future.isDone());

    future.cancel(true);
    assertTrue(future.isCancelled());
    assertTrue(future.isDone());

    // Cancelled tasks should not be returned during shutdown
    assertThat(executorService.shutdownNow(), empty());
  }

  @Test
  public void testSchedulePeriodicWithFixedRate() throws Exception {
    FastNanoClockAndSleeper fastNanoClockAndSleeper = new FastNanoClockAndSleeper();
    UnboundedScheduledExecutorService executorService =
        new UnboundedScheduledExecutorService(fastNanoClockAndSleeper);

    AtomicInteger callCount = new AtomicInteger();
    CountDownLatch latch = new CountDownLatch(1);

    ScheduledFutureTask<?> future =
        (ScheduledFutureTask<?>)
            executorService.scheduleAtFixedRate(
                () -> {
                  callCount.incrementAndGet();
                  latch.countDown();
                },
                100,
                50,
                MILLISECONDS);

    // No tasks should have been picked up
    wakeUpAndCheckTasks(executorService);
    assertEquals(0, callCount.get());

    // No tasks should have been picked up even if the time advances 99 seconds
    fastNanoClockAndSleeper.sleep(99);
    wakeUpAndCheckTasks(executorService);
    assertEquals(0, callCount.get());

    // We should have picked up the task 1 time, next task should be scheduled in 41 since we
    // advanced to 109
    fastNanoClockAndSleeper.sleep(10);
    wakeUpAndCheckTasks(executorService);
    latch.await();
    assertEquals(1, callCount.get());

    for (; ; ) {
      synchronized (executorService.tasks) {
        ScheduledFutureTask<?> task = executorService.tasks.peek();
        if (task != null) {
          assertEquals(41, task.getDelay(MILLISECONDS));
          break;
        }
      }
      Thread.sleep(1);
    }

    assertTrue(future.isPeriodic());
    assertFalse(future.isDone());

    future.cancel(true);
    assertTrue(future.isCancelled());
    assertTrue(future.isDone());

    // Cancelled tasks should not be returned during shutdown
    assertThat(executorService.shutdownNow(), empty());
  }

  void wakeUpAndCheckTasks(UnboundedScheduledExecutorService executorService) throws Exception {
    synchronized (executorService.tasks) {
      executorService.tasks.notify();
    }
    Thread.sleep(100);
  }

  @Test
  public void testThreadsAreAddedOnlyAsNeededWithContention() throws Exception {
    UnboundedScheduledExecutorService executorService = new UnboundedScheduledExecutorService();
    CountDownLatch start = new CountDownLatch(100);

    ThreadPoolExecutor executor =
        new ThreadPoolExecutor(100, 100, Long.MAX_VALUE, MILLISECONDS, new SynchronousQueue<>());
    // Schedule 100 threads that are going to be scheduling work non-stop but sequentially.
    for (int i = 0; i < 100; ++i) {
      executor.execute(
          () -> {
            start.countDown();
            try {
              start.await();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            for (int j = 0; j < 1000; ++j) {
              try {
                executorService
                    .submit(
                        () -> {
                          try {
                            Thread.sleep(1);
                          } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                          }
                        })
                    .get();
              } catch (InterruptedException | ExecutionException e) {
                // Ignore, happens on executor shutdown.
              }
            }
          });
    }

    executor.shutdown();
    executor.awaitTermination(3, MINUTES);

    int largestPool = executorService.threadPoolExecutor.getLargestPoolSize();
    LOG.info("Created {} threads to execute at most 100 parallel tasks", largestPool);
    // Ideally we would never create more than 100, however with contention it is still possible
    // some extra threads will be created.
    assertTrue(largestPool <= 110);
    executorService.shutdown();
  }
}
