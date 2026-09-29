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
package org.apache.beam.sdk.testing;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.annotations.Internal;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.experimental.categories.Category;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerScheduler;

/**
 * A JUnit 4 runner that supports concurrent execution of {@code @Test} methods within a test class,
 * with independent parallelism settings for {@link ValidatesRunner} tests versus other tests:
 *
 * <ul>
 *   <li>{@code -Dbeam.validatesRunner.parallelThreads=N}: number of concurrent threads per test
 *       worker JVM for {@code @Test} methods annotated with
 *       {@code @Category(ValidatesRunner.class)} (defaults to {@code beam.test.parallelThreads}).
 *   <li>{@code -Dbeam.test.parallelThreads=N}: number of concurrent threads per test worker JVM for
 *       all other {@code @Test} methods (defaults to {@code 1}, i.e., sequential execution).
 * </ul>
 *
 * <p>Classes or methods annotated with {@link SerialTest} are always executed sequentially on the
 * calling thread after draining any in-flight parallel test methods in the class.
 */
@Internal
public final class BeamParallelJunit4Runner extends BlockJUnit4ClassRunner {

  /**
   * Marks a test class or {@code @Test} method as requiring serial (non-parallel) execution even
   * when parallel test execution is enabled.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD})
  public @interface SerialTest {}

  public static final String VALIDATES_RUNNER_THREADS_PROPERTY =
      "beam.validatesRunner.parallelThreads";
  public static final String DEFAULT_TEST_THREADS_PROPERTY = "beam.test.parallelThreads";

  private static final ConcurrentHashMap<String, ExecutorService> EXECUTORS =
      new ConcurrentHashMap<>();

  private static final ConcurrentHashMap<Class<?>, Boolean> CLASS_SERIAL_CACHE =
      new ConcurrentHashMap<>();

  private final List<Future<?>> pendingFutures = Collections.synchronizedList(new ArrayList<>());

  private static @Nullable ExecutorService getOrCreateExecutor(String poolKey, int threads) {
    if (threads <= 1) {
      return null;
    }
    return EXECUTORS.computeIfAbsent(
        poolKey + ":" + threads,
        k -> {
          AtomicInteger counter = new AtomicInteger(1);
          return Executors.newFixedThreadPool(
              threads,
              runnable -> {
                Thread thread = new Thread(runnable);
                thread.setDaemon(true);
                thread.setName(poolKey + "-" + counter.getAndIncrement());
                return thread;
              });
        });
  }

  public BeamParallelJunit4Runner(Class<?> klass) throws InitializationError {
    super(klass);
    setScheduler(
        new RunnerScheduler() {
          @Override
          public void schedule(Runnable childStatement) {
            childStatement.run();
          }

          @Override
          public void finished() {
            awaitPendingFutures();
          }
        });
  }

  private void awaitPendingFutures() {
    Throwable firstError = null;
    List<Future<?>> snapshot;
    synchronized (pendingFutures) {
      snapshot = new ArrayList<>(pendingFutures);
      pendingFutures.clear();
    }
    for (Future<?> future : snapshot) {
      try {
        future.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        if (firstError == null) {
          firstError = e;
        }
      } catch (ExecutionException e) {
        if (firstError == null) {
          firstError = e.getCause() != null ? e.getCause() : e;
        }
      }
    }
    if (firstError instanceof RuntimeException) {
      throw (RuntimeException) firstError;
    } else if (firstError instanceof Error) {
      throw (Error) firstError;
    } else if (firstError != null) {
      throw new RuntimeException(firstError);
    }
  }

  @Override
  protected void runChild(final FrameworkMethod method, final RunNotifier notifier) {
    if (isIgnored(method)) {
      super.runChild(method, notifier);
      return;
    }
    ExecutorService executor = selectExecutor(method);
    if (executor == null) {
      super.runChild(method, notifier);
      return;
    }
    if (isClassMarkedSerial(getTestClass().getJavaClass()) || isMethodMarkedSerial(method)) {
      // This is a serial test. Make sure to wait for any in-flight tests to complete, and then run
      // this test
      // serially.
      awaitPendingFutures();
      super.runChild(method, notifier);
      return;
    }
    pendingFutures.add(executor.submit(() -> super.runChild(method, notifier)));
  }

  private @Nullable ExecutorService selectExecutor(FrameworkMethod method) {
    int defaultThreads = Integer.getInteger(DEFAULT_TEST_THREADS_PROPERTY, 1);
    if (isValidatesRunnerMethod(method)) {
      int vrThreads = Integer.getInteger(VALIDATES_RUNNER_THREADS_PROPERTY, defaultThreads);
      return getOrCreateExecutor("beam-vr-worker", vrThreads);
    }
    return getOrCreateExecutor("beam-test-worker", defaultThreads);
  }

  private boolean isValidatesRunnerMethod(FrameworkMethod method) {
    return hasValidatesRunnerCategory(method.getAnnotation(Category.class))
        || hasValidatesRunnerCategory(getTestClass().getAnnotation(Category.class));
  }

  private static boolean hasValidatesRunnerCategory(@Nullable Category category) {
    if (category == null) {
      return false;
    }
    for (Class<?> c : category.value()) {
      if (ValidatesRunner.class.isAssignableFrom(c)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isMethodMarkedSerial(FrameworkMethod method) {
    return method.getAnnotation(SerialTest.class) != null;
  }

  private static boolean isClassMarkedSerial(@Nullable Class<?> clazz) {
    if (clazz == null || clazz == Object.class) {
      return false;
    }
    return CLASS_SERIAL_CACHE.computeIfAbsent(clazz, BeamParallelJunit4Runner::computeClassSerial);
  }

  private static boolean computeClassSerial(@Nullable Class<?> clazz) {
    if (clazz == null || clazz == Object.class) {
      return false;
    }
    return clazz.isAnnotationPresent(SerialTest.class)
        || computeClassSerial(clazz.getSuperclass())
        || computeClassSerial(clazz.getEnclosingClass());
  }
}
