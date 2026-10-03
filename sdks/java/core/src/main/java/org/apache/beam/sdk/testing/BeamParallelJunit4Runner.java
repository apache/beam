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

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.annotations.Internal;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
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

  private CompletableFuture<Void> pendingFutures = CompletableFuture.allOf();

  /**
   * The test-class instance currently executing on this thread (and threads it spawns), so that
   * framework code such as {@link TestPipeline} can inspect the test's rules.
   */
  private static final ThreadLocal<@Nullable Object> CURRENT_TEST_INSTANCE =
      new InheritableThreadLocal<>();

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

  @Override
  protected Object createTest() throws Exception {
    Object instance = super.createTest();
    CURRENT_TEST_INSTANCE.set(instance);
    return instance;
  }

  /**
   * Returns {@code true} if the test currently executing on this thread has configured an active
   * {@link ExpectedException} rule expectation (i.e. the test expects to fail), which means its
   * pipeline must not be merged into a shared batch execution.
   */
  public static boolean isExpectingException() {
    Object instance = CURRENT_TEST_INSTANCE.get();
    if (instance == null) {
      return false;
    }
    for (Class<?> clazz = instance.getClass();
        clazz != null && clazz != Object.class;
        clazz = clazz.getSuperclass()) {
      for (Field field : clazz.getDeclaredFields()) {
        if (ExpectedException.class.isAssignableFrom(field.getType())) {
          try {
            field.setAccessible(true);
            Object ruleValue = field.get(instance);
            if (ruleValue instanceof ExpectedException
                && isExpectedExceptionActive((ExpectedException) ruleValue)) {
              return true;
            }
          } catch (ReflectiveOperationException | SecurityException ignored) {
            // Ignore and continue inspecting remaining fields.
          }
        }
      }
    }
    return false;
  }

  private static boolean isExpectedExceptionActive(ExpectedException rule) {
    try {
      Method method = ExpectedException.class.getDeclaredMethod("isAnyExceptionExpected");
      method.setAccessible(true);
      Object result = method.invoke(rule);
      if (result instanceof Boolean) {
        return (Boolean) result;
      }
    } catch (ReflectiveOperationException | SecurityException ignored) {
      // Fall back to inspecting the matcherBuilder field below.
    }
    try {
      Field builderField = ExpectedException.class.getDeclaredField("matcherBuilder");
      builderField.setAccessible(true);
      Object builder = builderField.get(rule);
      if (builder != null) {
        Method method = builder.getClass().getDeclaredMethod("isAnyExceptionExpected");
        method.setAccessible(true);
        Object result = method.invoke(builder);
        if (result instanceof Boolean) {
          return (Boolean) result;
        }
      }
    } catch (ReflectiveOperationException | SecurityException ignored) {
      // Ignore.
    }
    return false;
  }

  private void awaitPendingFutures() {
    try {
      pendingFutures.join();
    } finally {
      pendingFutures = CompletableFuture.allOf();
    }
  }

  private void runChildInternal(final FrameworkMethod method, final RunNotifier notifier) {
    try {
      super.runChild(method, notifier);
    } finally {
      CURRENT_TEST_INSTANCE.remove();
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
      runChildInternal(method, notifier);
      return;
    }
    if (isClassMarkedSerial(getTestClass().getJavaClass()) || isMethodMarkedSerial(method)) {
      // This is a serial test. Make sure to wait for any in-flight tests to complete, and then run
      // this test
      // serially.
      awaitPendingFutures();
      runChildInternal(method, notifier);
      return;
    }
    pendingFutures =
        CompletableFuture.allOf(
            pendingFutures,
            CompletableFuture.runAsync(() -> runChildInternal(method, notifier), executor));
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

  /** Returns {@code true} if {@code annotations} contains {@link SerialTest}. */
  static boolean hasSerialAnnotation(Collection<Annotation> annotations) {
    for (Annotation annotation : annotations) {
      if (annotation.annotationType() == SerialTest.class) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns {@code true} if {@code clazz}, any of its superclasses, or any of its enclosing classes
   * is annotated with {@link SerialTest}.
   */
  static boolean isClassMarkedSerial(@Nullable Class<?> clazz) {
    if (clazz == null || clazz == Object.class) {
      return false;
    }
    return CLASS_SERIAL_CACHE.computeIfAbsent(clazz, BeamParallelJunit4Runner::computeClassSerial);
  }

  private static boolean computeClassSerial(@Nullable Class<?> clazz) {
    if (clazz == null || clazz == Object.class) {
      return false;
    }
    // Don't access CLASS_SERIAL_CACHE here, as ConcurrentHashMap does not support reentrancy.
    return clazz.isAnnotationPresent(SerialTest.class)
        || computeClassSerial(clazz.getSuperclass())
        || computeClassSerial(clazz.getEnclosingClass());
  }
}
