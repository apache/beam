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
package org.apache.beam.runners.dataflow.worker.util;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.runners.dataflow.worker.WorkerUncaughtExceptionHandler;
import org.apache.beam.runners.dataflow.worker.util.common.worker.JvmRuntime;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

/**
 * Utility class for {@link java.util.concurrent.ExecutorService}s that will terminate the JVM on
 * uncaught exceptions.
 *
 * @implNote Ensures that all threads produced by the {@link ExecutorService}s have a {@link
 *     WorkerUncaughtExceptionHandler} attached to prevent hidden/silent exceptions and errors.
 */
public final class TerminatingExecutors {
  private TerminatingExecutors() {}

  public static TerminatingExecutorService newSingleThreadExecutor(
      ThreadFactoryBuilder threadFactoryBuilder, Logger logger) {
    return new TerminatingExecutorService(
        Executors.newSingleThreadExecutor(terminatingThreadFactory(threadFactoryBuilder, logger)));
  }

  public static TerminatingScheduledExecutorService newSingleThreadScheduledExecutor(
      ThreadFactoryBuilder threadFactoryBuilder, Logger logger) {
    return new TerminatingScheduledExecutorService(
        Executors.newSingleThreadScheduledExecutor(
            terminatingThreadFactory(threadFactoryBuilder, logger)));
  }

  public static TerminatingExecutorService newCachedThreadPool(
      ThreadFactoryBuilder threadFactoryBuilder, Logger logger) {
    return new TerminatingExecutorService(
        Executors.newCachedThreadPool(terminatingThreadFactory(threadFactoryBuilder, logger)));
  }

  public static TerminatingExecutorService newFixedThreadPool(
      int numThreads, ThreadFactoryBuilder threadFactoryBuilder, Logger logger) {
    return new TerminatingExecutorService(
        Executors.newFixedThreadPool(
            numThreads, terminatingThreadFactory(threadFactoryBuilder, logger)));
  }

  public static TerminatingExecutorService newSingleThreadedExecutorForTesting(
      JvmRuntime jvmRuntime, ThreadFactoryBuilder threadFactoryBuilder, Logger logger) {
    return new TerminatingExecutorService(
        Executors.newSingleThreadExecutor(
            terminatingThreadFactoryForTesting(jvmRuntime, threadFactoryBuilder, logger)));
  }

  private static ThreadFactory terminatingThreadFactory(
      ThreadFactoryBuilder threadFactoryBuilder, Logger logger) {
    return threadFactoryBuilder
        .setUncaughtExceptionHandler(new WorkerUncaughtExceptionHandler(logger))
        .build();
  }

  private static ThreadFactory terminatingThreadFactoryForTesting(
      JvmRuntime jvmRuntime, ThreadFactoryBuilder threadFactoryBuilder, Logger logger) {
    return threadFactoryBuilder
        .setUncaughtExceptionHandler(new WorkerUncaughtExceptionHandler(jvmRuntime, logger))
        .build();
  }

  /** Wrapper for {@link ScheduledExecutorService}(s) created by {@link TerminatingExecutors}. */
  public static final class TerminatingScheduledExecutorService extends TerminatingExecutorService
      implements ScheduledExecutorService {
    private final ScheduledExecutorService delegate;

    private TerminatingScheduledExecutorService(ScheduledExecutorService delegate) {
      super(delegate);
      this.delegate = delegate;
    }

    @Override
    public ScheduledFuture<@Nullable ?> schedule(Runnable command, long delay, TimeUnit unit) {
      return delegate.schedule(command, delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
      return delegate.schedule(callable, delay, unit);
    }

    @Override
    public ScheduledFuture<@Nullable ?> scheduleAtFixedRate(
        Runnable command, long initialDelay, long period, TimeUnit unit) {
      return delegate.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<@Nullable ?> scheduleWithFixedDelay(
        Runnable command, long initialDelay, long delay, TimeUnit unit) {
      return delegate.scheduleWithFixedDelay(command, initialDelay, delay, unit);
    }
  }

  /** Wrapper for {@link ExecutorService}(s) created by {@link TerminatingExecutors}. */
  public static class TerminatingExecutorService implements ExecutorService {
    private final ExecutorService delegate;

    private TerminatingExecutorService(ExecutorService delegate) {
      this.delegate = delegate;
    }

    @Override
    public void shutdown() {
      delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
      return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
      return delegate.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
      return delegate.submit(task, result);
    }

    @Override
    public Future<@Nullable ?> submit(Runnable task) {
      return delegate.submit(task);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
        throws InterruptedException {
      return delegate.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(
        Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException {
      return delegate.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
        throws InterruptedException, ExecutionException {
      return delegate.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return delegate.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
      delegate.execute(command);
    }
  }
}
