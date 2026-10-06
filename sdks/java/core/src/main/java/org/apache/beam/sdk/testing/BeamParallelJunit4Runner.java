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
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.experimental.categories.Category;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.RunnerScheduler;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *
 * <p>Runners that merge the pipelines of concurrently running tests into one job rely on this
 * runner in two ways: it lets each {@link TestPipeline} rule see its test instance (to recognize
 * tests that must not be merged), and it re-executes a test once, from scratch and with merging
 * disabled, when the runner could not reach a verdict for it from a merged job (see {@link
 * TestPipeline.StandaloneRerunRequested}).
 */
@Internal
public final class BeamParallelJunit4Runner extends BlockJUnit4ClassRunner {

  private static final Logger LOG = LoggerFactory.getLogger(BeamParallelJunit4Runner.class);

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

  /**
   * Hands the test instance to its {@link TestPipeline} rule(s), which inspect the instance's other
   * rules to decide whether the test's pipeline may be merged with others. A {@code TestRule} never
   * sees the instance itself; the runner is the only place that does.
   */
  @Override
  protected Object createTest() throws Exception {
    Object instance = super.createTest();
    TestPipeline.attachTestInstance(instance);
    return instance;
  }

  private void awaitPendingFutures() {
    try {
      pendingFutures.join();
    } finally {
      pendingFutures = CompletableFuture.allOf();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Re-executing a test whose pipeline was merged into a shared job without a verdict
  // ---------------------------------------------------------------------------------------------
  //
  // A runner that merges several tests' pipelines into one job may be unable to tell from that job
  // whether a given test passed (see TestPipeline.StandaloneRerunRequested). The pipeline object
  // cannot simply be run again, so the test is executed a second time from scratch: new instance,
  // @Before/@After, rules and all, with its TestPipeline told to insist on a job of its own. Both
  // attempts are one test as far as JUnit is concerned; it sees a single start/finish pair.

  /**
   * Tests currently in their standalone re-execution, by {@link Description}. Keyed by description
   * rather than thread because the test body does not necessarily run on the thread that started it
   * (JUnit's Timeout rule, for one, evaluates it on a thread of its own). A method is never
   * re-executed concurrently with itself, so an entry unambiguously refers to the current attempt.
   */
  private static final Set<Description> STANDALONE_RERUNS = ConcurrentHashMap.newKeySet();

  /**
   * Returns {@code true} while the test described by {@code description} is being re-executed after
   * an inconclusive merged attempt; {@link TestPipeline} consults this when its rule is applied.
   */
  static boolean isStandaloneRerun(Description description) {
    return STANDALONE_RERUNS.contains(description);
  }

  /**
   * The {@link TestPipeline.StandaloneRerunRequested} behind {@code failure}, or {@code null} if
   * there is none. Test code and JUnit itself may wrap what {@code run()} threw ({@code
   * assertThrows}, {@code @Test(expected)}, an {@code @After} failing alongside it), so the cause
   * chain and {@link MultipleFailureException}'s failures are searched too.
   */
  @VisibleForTesting
  static TestPipeline.@Nullable StandaloneRerunRequested standaloneRerunRequest(Throwable failure) {
    int depth = 0;
    for (Throwable t = failure; t != null && depth < 32; t = t.getCause(), depth++) {
      if (t instanceof TestPipeline.StandaloneRerunRequested) {
        return (TestPipeline.StandaloneRerunRequested) t;
      }
      if (t instanceof MultipleFailureException) {
        for (Throwable each : ((MultipleFailureException) t).getFailures()) {
          TestPipeline.StandaloneRerunRequested nested = standaloneRerunRequest(each);
          if (nested != null) {
            return nested;
          }
        }
      }
    }
    return null;
  }

  /**
   * Like {@link BlockJUnit4ClassRunner#runChild}, except that a test ending in {@link
   * TestPipeline.StandaloneRerunRequested} is executed a second time (see above). A request raised
   * by that second attempt is a bug in the requesting runner and is reported as the test's failure.
   */
  private void runChildInternal(final FrameworkMethod method, final RunNotifier notifier) {
    final Description description = describeChild(method);
    Statement statement =
        new Statement() {
          @Override
          public void evaluate() throws Throwable {
            try {
              methodBlock(method).evaluate();
            } catch (Throwable firstAttempt) {
              TestPipeline.StandaloneRerunRequested request = standaloneRerunRequest(firstAttempt);
              if (request == null) {
                throw firstAttempt;
              }
              LOG.info(
                  "Re-executing {} with its pipeline as a job of its own. {}",
                  description.getDisplayName(),
                  String.valueOf(request));
              STANDALONE_RERUNS.add(description);
              try {
                methodBlock(method).evaluate();
              } finally {
                STANDALONE_RERUNS.remove(description);
              }
            }
          }
        };
    runLeaf(statement, description, notifier);
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
