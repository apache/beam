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

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.math.LongMath;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Longs;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.MoreExecutors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.KeyForBottom;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An unbounded {@link ScheduledExecutorService} based upon the {@link ScheduledThreadPoolExecutor}
 * API contract.
 *
 * <p>Note that this implementation differs from a {@link ScheduledThreadPoolExecutor} in the
 * following ways:
 *
 * <ul>
 *   <li>The core pool size is always 0.
 *   <li>Any work that is immediately executable is given to a thread before returning from the
 *       corresponding {@code execute}, {@code submit}, {@code schedule*} methods.
 *   <li>An unbounded number of threads can be started.
 * </ul>
 */
public final class UnboundedScheduledExecutorService implements ScheduledExecutorService {
  /**
   * A {@link FutureTask} that handles periodically rescheduling tasks.
   *
   * <p>Note that it is important that this class extends {@link FutureTask} and {@link
   * RunnableScheduledFuture} to be compatible with the types of objects returned by a {@link
   * ScheduledThreadPoolExecutor}.
   */
  @VisibleForTesting
  @SuppressFBWarnings(
      value = "EQ_COMPARETO_USE_OBJECT_EQUALS",
      justification =
          "Default equals/hashCode is what we want since two scheduled tasks are only equivalent if they point to the same instance.")
  final class ScheduledFutureTask<@Nullable @KeyForBottom V> extends FutureTask<V>
      implements RunnableScheduledFuture<V> {

    /** Sequence number to break ties FIFO. */
    private final long sequenceNumber;

    /** The time the task is enabled to execute in nanoTime units. */
    private long time;

    /**
     * Period in nanoseconds for repeating tasks. A positive value indicates fixed-rate execution. A
     * negative value indicates fixed-delay execution. A value of 0 indicates a non-repeating
     * (one-shot) task.
     */
    private final long period;

    /** Creates a one-shot action with given nanoTime-based trigger time. */
    ScheduledFutureTask(Runnable r, @Nullable V result, long triggerTime) {
      this(r, result, triggerTime, 0);
    }

    /** Creates a periodic action with given nanoTime-based initial trigger time and period. */
    @SuppressWarnings("argument")
    ScheduledFutureTask(Runnable r, @Nullable V result, long triggerTime, long period) {
      super(r, result);
      this.time = triggerTime;
      this.period = period;
      this.sequenceNumber = sequencer.getAndIncrement();
    }

    /** Creates a one-shot action with given nanoTime-based trigger time. */
    ScheduledFutureTask(Callable<V> callable, long triggerTime) {
      super(callable);
      this.time = triggerTime;
      this.period = 0;
      this.sequenceNumber = sequencer.getAndIncrement();
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(LongMath.saturatedSubtract(time, clock.nanoTime()), NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
      if (other == this) // compare zero if same object
      {
        return 0;
      }
      if (other instanceof ScheduledFutureTask) {
        ScheduledFutureTask<?> x = (ScheduledFutureTask<?>) other;
        int diff = Longs.compare(time, x.time);
        if (diff != 0) {
          return diff;
        }
        if (sequenceNumber < x.sequenceNumber) {
          return -1;
        }
        return 1;
      }
      long diff = LongMath.saturatedSubtract(getDelay(NANOSECONDS), other.getDelay(NANOSECONDS));
      return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
    }

    @Override
    public boolean isPeriodic() {
      return period != 0;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      boolean cancelled = super.cancel(mayInterruptIfRunning);
      synchronized (tasks) {
        tasks.remove(this);
      }
      return cancelled;
    }

    /** Overrides {@link FutureTask} so as to reset/requeue if periodic. */
    @Override
    public void run() {
      boolean periodic = isPeriodic();
      if (!periodic) {
        super.run();
      } else if (super.runAndReset()) {
        // Set the next runtime
        if (period > 0) {
          time = LongMath.saturatedAdd(time, period);
        } else {
          time = triggerTime(-period);
        }
        synchronized (tasks) {
          tasks.add(this);
          tasks.notify();
        }
      }
    }
  }

  // Used to break ties in ordering of future tasks that are scheduled for the same time
  // so that they have a consistent ordering based upon their insertion order into
  // this ScheduledExecutorService.
  private final AtomicLong sequencer = new AtomicLong();

  private final NanoClock clock;
  @VisibleForTesting final ThreadPoolExecutor threadPoolExecutor;
  @VisibleForTesting final PriorityQueue<ScheduledFutureTask<?>> tasks;
  private final AbstractExecutorService invokeMethodsAdapter;
  private final Future<?> launchTasks;

  public UnboundedScheduledExecutorService() {
    this(NanoClock.SYSTEM);
  }

  @VisibleForTesting
  UnboundedScheduledExecutorService(NanoClock clock) {
    this.clock = clock;
    ThreadFactoryBuilder threadFactoryBuilder = new ThreadFactoryBuilder();
    threadFactoryBuilder.setThreadFactory(MoreExecutors.platformThreadFactory());
    threadFactoryBuilder.setDaemon(true);

    this.threadPoolExecutor =
        new ThreadPoolExecutor(
            0,
            Integer.MAX_VALUE, // Allow an unlimited number of re-usable threads.
            // Put a high-timeout on non-core threads. This reduces memory for per-thread caches
            // over time.
            1,
            HOURS,
            new SynchronousQueue<Runnable>() {
              @Override
              public boolean offer(Runnable r) {
                try {
                  // By blocking for a little we hope to delay thread creation if there are existing
                  // threads that will eventually return. We expect this timeout to be very rarely
                  // hit as the high-watermark of necessary threads will remain for up to an hour.
                  if (offer(r, 10, MILLISECONDS)) {
                    return true;
                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                return false;
              }
            },
            threadFactoryBuilder.build());

    // Create an internal adapter so that execute does not re-wrap the ScheduledFutureTask again
    this.invokeMethodsAdapter =
        new AbstractExecutorService() {

          @Override
          protected <@KeyForBottom T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
            return new ScheduledFutureTask<>(runnable, value, 0);
          }

          @Override
          protected <@KeyForBottom T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
            return new ScheduledFutureTask<>(callable, 0);
          }

          @Override
          public void shutdown() {
            throw new UnsupportedOperationException();
          }

          @Override
          public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean isShutdown() {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean isTerminated() {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            throw new UnsupportedOperationException();
          }

          @Override
          /* UnboundedScheduledExecutorService is the only caller after it has been initialized.*/
          @SuppressWarnings("method.invocation")
          public void execute(Runnable command) {
            // These are already guaranteed to be a ScheduledFutureTask so there is no need to wrap
            // it in another ScheduledFutureTask.
            threadPoolExecutor.execute(command);
          }
        };
    this.tasks = new PriorityQueue<>();
    this.launchTasks =
        threadPoolExecutor.submit(new TaskLauncher(tasks, threadPoolExecutor, clock));
  }

  private static class TaskLauncher implements Callable<Void> {
    private final PriorityQueue<ScheduledFutureTask<?>> tasks;
    private final ThreadPoolExecutor threadPoolExecutor;
    private final NanoClock clock;

    private TaskLauncher(
        PriorityQueue<ScheduledFutureTask<?>> tasks,
        ThreadPoolExecutor threadPoolExecutor,
        NanoClock clock) {
      this.tasks = tasks;
      this.threadPoolExecutor = threadPoolExecutor;
      this.clock = clock;
    }

    @Override
    public Void call() throws Exception {
      while (true) {
        synchronized (tasks) {
          if (threadPoolExecutor.isShutdown()) {
            return null;
          }
          ScheduledFutureTask<?> task = tasks.peek();
          if (task == null) {
            tasks.wait();
            continue;
          }
          long nanosToWait = LongMath.saturatedSubtract(task.time, clock.nanoTime());
          if (nanosToWait > 0) {
            long millisToWait = nanosToWait / 1_000_000;
            int nanosRemainder = (int) (nanosToWait % 1_000_000);
            tasks.wait(millisToWait, nanosRemainder);
            continue;
          }
          // Remove the task from the queue since it is ready to be scheduled now
          task = tasks.remove();
          threadPoolExecutor.execute(task);
        }
      }
    }
  }

  @Override
  public void shutdown() {
    threadPoolExecutor.shutdown();
    synchronized (tasks) {
      // Notify tasks which checks to see if the ThreadPoolExecutor is shutdown and exits cleanly.
      tasks.notify();
    }

    // Re-throw any errors during shutdown of the launchTasks thread.
    try {
      launchTasks.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e.getCause());
    }
  }

  @Override
  public List<Runnable> shutdownNow() {
    shutdown();
    synchronized (tasks) {
      List<Runnable> rval = new ArrayList<>(tasks);
      tasks.clear();
      rval.addAll(threadPoolExecutor.shutdownNow());
      return rval;
    }
  }

  @Override
  public boolean isShutdown() {
    return threadPoolExecutor.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return threadPoolExecutor.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return threadPoolExecutor.awaitTermination(timeout, unit);
  }

  @Override
  public void execute(Runnable command) {
    if (command == null) {
      throw new NullPointerException();
    }
    ScheduledFutureTask<Void> task = new ScheduledFutureTask<>(command, null, triggerTime(0));
    threadPoolExecutor.execute(task);
  }

  @Override
  public Future<@Nullable ?> submit(Runnable command) {
    if (command == null) {
      throw new NullPointerException();
    }
    ScheduledFutureTask<Void> task = new ScheduledFutureTask<>(command, null, triggerTime(0));
    threadPoolExecutor.execute(task);
    return task;
  }

  @Override
  /* Ignore improper flag since FB detects that ScheduledExecutorService can't have nullable V. */
  @SuppressWarnings("override.return")
  public <@Nullable @KeyForBottom T> Future<T> submit(Runnable command, T result) {
    if (command == null) {
      throw new NullPointerException();
    }
    ScheduledFutureTask<T> task = new ScheduledFutureTask<>(command, result, triggerTime(0));
    runNowOrScheduleInTheFuture(task);
    return task;
  }

  @Override
  /* Ignore improper flag since FB detects that ScheduledExecutorService can't have nullable V. */
  @SuppressWarnings({"override.param", "override.return"})
  public <@Nullable @KeyForBottom T> Future<T> submit(Callable<T> command) {
    if (command == null) {
      throw new NullPointerException();
    }
    ScheduledFutureTask<T> task = new ScheduledFutureTask<>(command, triggerTime(0));
    threadPoolExecutor.execute(task);
    return task;
  }

  @Override
  public <@KeyForBottom T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return invokeMethodsAdapter.invokeAll(tasks);
  }

  @Override
  public <@KeyForBottom T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    if (tasks == null || unit == null) {
      throw new NullPointerException();
    }
    return invokeMethodsAdapter.invokeAll(tasks, timeout, unit);
  }

  @Override
  public <@KeyForBottom T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return invokeMethodsAdapter.invokeAny(tasks);
  }

  @Override
  public <@KeyForBottom T> T invokeAny(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return invokeMethodsAdapter.invokeAny(tasks, timeout, unit);
  }

  @Override
  public ScheduledFuture<@Nullable ?> schedule(Runnable command, long delay, TimeUnit unit) {
    if (command == null || unit == null) {
      throw new NullPointerException();
    }
    ScheduledFutureTask<Void> task =
        new ScheduledFutureTask<>(command, null, triggerTime(delay, unit));
    runNowOrScheduleInTheFuture(task);
    return task;
  }

  @Override
  /* Ignore improper flag since FB detects that ScheduledExecutorService can't have nullable V. */
  @SuppressWarnings({"override.param", "override.return"})
  public <@Nullable @KeyForBottom V> ScheduledFuture<V> schedule(
      Callable<V> callable, long delay, TimeUnit unit) {
    if (callable == null || unit == null) {
      throw new NullPointerException();
    }
    ScheduledFutureTask<V> task = new ScheduledFutureTask<>(callable, triggerTime(delay, unit));
    runNowOrScheduleInTheFuture(task);
    return task;
  }

  @Override
  public ScheduledFuture<@Nullable ?> scheduleAtFixedRate(
      Runnable command, long initialDelay, long period, TimeUnit unit) {
    if (command == null || unit == null) {
      throw new NullPointerException();
    }
    if (period <= 0) {
      throw new IllegalArgumentException();
    }
    ScheduledFutureTask<Void> task =
        new ScheduledFutureTask<Void>(
            command, null, triggerTime(initialDelay, unit), unit.toNanos(period));
    runNowOrScheduleInTheFuture(task);
    return task;
  }

  @Override
  public ScheduledFuture<@Nullable ?> scheduleWithFixedDelay(
      Runnable command, long initialDelay, long delay, TimeUnit unit) {
    if (command == null || unit == null) {
      throw new NullPointerException();
    }
    if (delay <= 0) {
      throw new IllegalArgumentException();
    }
    ScheduledFutureTask<Void> task =
        new ScheduledFutureTask<>(
            command, null, triggerTime(initialDelay, unit), -unit.toNanos(delay));
    runNowOrScheduleInTheFuture(task);
    return task;
  }

  private <@Nullable @KeyForBottom T> void runNowOrScheduleInTheFuture(
      ScheduledFutureTask<T> task) {
    long nanosToWait = LongMath.saturatedSubtract(task.time, clock.nanoTime());
    if (nanosToWait <= 0) {
      threadPoolExecutor.execute(task);
      return;
    }

    synchronized (tasks) {
      if (isShutdown()) {
        threadPoolExecutor
            .getRejectedExecutionHandler()
            .rejectedExecution(task, threadPoolExecutor);
      }
      tasks.add(task);
      tasks.notify();
    }
  }

  /** Returns the nanoTime-based trigger time of a delayed action. */
  private long triggerTime(long delay, TimeUnit unit) {
    return triggerTime(unit.toNanos((delay < 0) ? 0 : delay));
  }

  /** Returns the nanoTime-based trigger time of a delayed action. */
  private long triggerTime(long delay) {
    return LongMath.saturatedAdd(clock.nanoTime(), delay);
  }
}
