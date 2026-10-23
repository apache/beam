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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.testing.BeamParallelJunit4Runner.SerialTest;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.RunWith;
import org.junit.runner.notification.Failure;
import org.junit.runners.JUnit4;
import org.junit.runners.model.MultipleFailureException;

/** Tests for {@link BeamParallelJunit4Runner}. */
@RunWith(JUnit4.class)
public class BeamParallelJunit4RunnerTest {

  private static final AtomicBoolean IN_TEST_HARNESS = new AtomicBoolean(false);

  @Rule public final TestRule restoreProperties = new RestoreSystemProperties();

  @RunWith(BeamParallelJunit4Runner.class)
  public static class SampleMixedCases {
    static final CountDownLatch VR_LATCH = new CountDownLatch(2);
    static final Set<String> VR_THREADS = ConcurrentHashMap.newKeySet();
    static final Set<String> UNIT_THREADS = ConcurrentHashMap.newKeySet();
    static final AtomicInteger UNIT_ACTIVE = new AtomicInteger(0);
    static final AtomicInteger UNIT_MAX_ACTIVE = new AtomicInteger(0);

    @Test
    @Category(ValidatesRunner.class)
    public void testVr1() throws InterruptedException {
      assumeTrue(IN_TEST_HARNESS.get());
      VR_THREADS.add(Thread.currentThread().getName());
      VR_LATCH.countDown();
      assertTrue(VR_LATCH.await(5, TimeUnit.SECONDS));
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testVr2() throws InterruptedException {
      assumeTrue(IN_TEST_HARNESS.get());
      VR_THREADS.add(Thread.currentThread().getName());
      VR_LATCH.countDown();
      assertTrue(VR_LATCH.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testUnit1() throws InterruptedException {
      assumeTrue(IN_TEST_HARNESS.get());
      int active = UNIT_ACTIVE.incrementAndGet();
      UNIT_MAX_ACTIVE.accumulateAndGet(active, Math::max);
      UNIT_THREADS.add(Thread.currentThread().getName());
      Thread.sleep(50);
      UNIT_ACTIVE.decrementAndGet();
    }

    @Test
    public void testUnit2() throws InterruptedException {
      assumeTrue(IN_TEST_HARNESS.get());
      int active = UNIT_ACTIVE.incrementAndGet();
      UNIT_MAX_ACTIVE.accumulateAndGet(active, Math::max);
      UNIT_THREADS.add(Thread.currentThread().getName());
      Thread.sleep(50);
      UNIT_ACTIVE.decrementAndGet();
    }
  }

  @SerialTest
  @RunWith(BeamParallelJunit4Runner.class)
  public static class SampleSerialClassCases {
    static final AtomicInteger ACTIVE = new AtomicInteger(0);
    static final AtomicInteger MAX_ACTIVE = new AtomicInteger(0);

    @Test
    @Category(ValidatesRunner.class)
    public void test1() throws InterruptedException {
      assumeTrue(IN_TEST_HARNESS.get());
      int active = ACTIVE.incrementAndGet();
      MAX_ACTIVE.accumulateAndGet(active, Math::max);
      Thread.sleep(50);
      ACTIVE.decrementAndGet();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void test2() throws InterruptedException {
      assumeTrue(IN_TEST_HARNESS.get());
      int active = ACTIVE.incrementAndGet();
      MAX_ACTIVE.accumulateAndGet(active, Math::max);
      Thread.sleep(50);
      ACTIVE.decrementAndGet();
    }
  }

  @RunWith(BeamParallelJunit4Runner.class)
  public static class SampleSerialMethodCases {
    static final AtomicInteger ACTIVE = new AtomicInteger(0);
    static final AtomicInteger SERIAL_OBSERVED_ACTIVE = new AtomicInteger(0);

    @Test
    @Category(ValidatesRunner.class)
    public void test1Parallel() throws InterruptedException {
      assumeTrue(IN_TEST_HARNESS.get());
      ACTIVE.incrementAndGet();
      Thread.sleep(60);
      ACTIVE.decrementAndGet();
    }

    @Test
    @SerialTest
    @Category(ValidatesRunner.class)
    public void test2Serial() throws InterruptedException {
      assumeTrue(IN_TEST_HARNESS.get());
      int active = ACTIVE.incrementAndGet();
      SERIAL_OBSERVED_ACTIVE.accumulateAndGet(active, Math::max);
      Thread.sleep(40);
      ACTIVE.decrementAndGet();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void test3Parallel() throws InterruptedException {
      assumeTrue(IN_TEST_HARNESS.get());
      ACTIVE.incrementAndGet();
      Thread.sleep(60);
      ACTIVE.decrementAndGet();
    }
  }

  @Test
  public void testSeparateParallelismForValidatesRunnerVsUnitTests() {
    System.setProperty(BeamParallelJunit4Runner.VALIDATES_RUNNER_THREADS_PROPERTY, "2");
    System.setProperty(BeamParallelJunit4Runner.DEFAULT_TEST_THREADS_PROPERTY, "1");

    IN_TEST_HARNESS.set(true);
    try {
      Result result = JUnitCore.runClasses(SampleMixedCases.class);
      assertEquals(0, result.getFailureCount());
      assertEquals(4, result.getRunCount());
      assertEquals(2, SampleMixedCases.VR_THREADS.size());
      assertEquals(1, SampleMixedCases.UNIT_MAX_ACTIVE.get());
    } finally {
      IN_TEST_HARNESS.set(false);
    }
  }

  @Test
  public void testSerialTestClassRunsSerially() {
    System.setProperty(BeamParallelJunit4Runner.VALIDATES_RUNNER_THREADS_PROPERTY, "4");

    IN_TEST_HARNESS.set(true);
    try {
      Result result = JUnitCore.runClasses(SampleSerialClassCases.class);
      assertEquals(0, result.getFailureCount());
      assertEquals(2, result.getRunCount());
      assertEquals(1, SampleSerialClassCases.MAX_ACTIVE.get());
    } finally {
      IN_TEST_HARNESS.set(false);
    }
  }

  @Test
  public void testSerialTestMethodDrainsAndRunsAlone() {
    System.setProperty(BeamParallelJunit4Runner.VALIDATES_RUNNER_THREADS_PROPERTY, "4");

    IN_TEST_HARNESS.set(true);
    try {
      Result result = JUnitCore.runClasses(SampleSerialMethodCases.class);
      assertEquals(0, result.getFailureCount());
      assertEquals(3, result.getRunCount());
      assertEquals(1, SampleSerialMethodCases.SERIAL_OBSERVED_ACTIVE.get());
    } finally {
      IN_TEST_HARNESS.set(false);
    }
  }

  @RunWith(BeamParallelJunit4Runner.class)
  public static class SampleExpectedExceptionCases {
    static final AtomicBoolean OBSERVED_WHEN_EXPECTING = new AtomicBoolean(false);
    static final AtomicBoolean OBSERVED_WHEN_NOT_EXPECTING = new AtomicBoolean(true);
    static final AtomicBoolean OBSERVED_ON_CHILD_THREAD = new AtomicBoolean(false);

    @Rule public ExpectedException thrown = ExpectedException.none();
    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testExpecting() throws InterruptedException {
      assumeTrue(IN_TEST_HARNESS.get());
      thrown.expect(IllegalStateException.class);
      OBSERVED_WHEN_EXPECTING.set(pipeline.testExpectsException());
      // Detection is tied to the test instance, not to a thread: rules such as Timeout evaluate
      // the test body on a thread of their own.
      Thread child =
          new Thread(() -> OBSERVED_ON_CHILD_THREAD.set(pipeline.testExpectsException()));
      child.start();
      child.join();
      throw new IllegalStateException("expected");
    }

    @Test
    public void testNotExpecting() {
      assumeTrue(IN_TEST_HARNESS.get());
      OBSERVED_WHEN_NOT_EXPECTING.set(pipeline.testExpectsException());
    }
  }

  @Test
  public void testDetectsActiveExpectedExceptionRule() {
    IN_TEST_HARNESS.set(true);
    try {
      Result result = JUnitCore.runClasses(SampleExpectedExceptionCases.class);
      assertEquals(0, result.getFailureCount());
      assertEquals(2, result.getRunCount());
      assertTrue(SampleExpectedExceptionCases.OBSERVED_WHEN_EXPECTING.get());
      assertFalse(SampleExpectedExceptionCases.OBSERVED_WHEN_NOT_EXPECTING.get());
      assertTrue(SampleExpectedExceptionCases.OBSERVED_ON_CHILD_THREAD.get());
    } finally {
      IN_TEST_HARNESS.set(false);
    }
  }

  @RunWith(BeamParallelJunit4Runner.class)
  public static class SampleStandaloneRerunCases {
    static final AtomicInteger BEFORE_CALLS = new AtomicInteger(0);
    static final List<Object> RERUN_INSTANCES = Collections.synchronizedList(new ArrayList<>());
    static final AtomicBoolean STANDALONE_ON_FIRST_ATTEMPT = new AtomicBoolean(true);
    static final AtomicBoolean STANDALONE_ON_RERUN = new AtomicBoolean(false);
    static final AtomicInteger ALWAYS_REQUESTING_ATTEMPTS = new AtomicInteger(0);

    @Rule public final transient TestPipeline pipeline = TestPipeline.create();
    // Evaluates the test body on a thread of its own, as in the ValidatesRunner suites.
    @Rule public final Timeout timeout = Timeout.seconds(30);

    @Before
    public void before() {
      BEFORE_CALLS.incrementAndGet();
    }

    @Test
    public void testRerunOnce() {
      assumeTrue(IN_TEST_HARNESS.get());
      RERUN_INSTANCES.add(this);
      if (RERUN_INSTANCES.size() == 1) {
        STANDALONE_ON_FIRST_ATTEMPT.set(pipeline.isStandaloneExecutionRequired());
        // Wrapped, as test code catching what run() threw would do.
        throw new RuntimeException(
            "wrapped", new TestPipeline.StandaloneRerunRequested("no verdict from merged job"));
      }
      STANDALONE_ON_RERUN.set(pipeline.isStandaloneExecutionRequired());
    }

    @Test
    public void testAlwaysRequesting() {
      assumeTrue(IN_TEST_HARNESS.get());
      ALWAYS_REQUESTING_ATTEMPTS.incrementAndGet();
      throw new TestPipeline.StandaloneRerunRequested("still no verdict");
    }
  }

  @Test
  public void testReexecutesTestOnceWhenStandaloneRerunIsRequested() {
    System.setProperty(BeamParallelJunit4Runner.DEFAULT_TEST_THREADS_PROPERTY, "2");
    IN_TEST_HARNESS.set(true);
    try {
      Result result = JUnitCore.runClasses(SampleStandaloneRerunCases.class);
      // Two attempts are one test to JUnit.
      assertEquals(2, result.getRunCount());

      // testRerunOnce: a second, fresh instance (and @Before) with standalone execution forced.
      assertEquals(2, SampleStandaloneRerunCases.RERUN_INSTANCES.size());
      assertNotSame(
          SampleStandaloneRerunCases.RERUN_INSTANCES.get(0),
          SampleStandaloneRerunCases.RERUN_INSTANCES.get(1));
      assertFalse(SampleStandaloneRerunCases.STANDALONE_ON_FIRST_ATTEMPT.get());
      assertTrue(SampleStandaloneRerunCases.STANDALONE_ON_RERUN.get());

      // testAlwaysRequesting: re-executed exactly once, then the request is the failure.
      assertEquals(2, SampleStandaloneRerunCases.ALWAYS_REQUESTING_ATTEMPTS.get());
      assertEquals(1, result.getFailureCount());
      Failure failure = result.getFailures().get(0);
      assertEquals("testAlwaysRequesting", failure.getDescription().getMethodName());
      assertThat(failure.getException(), instanceOf(TestPipeline.StandaloneRerunRequested.class));

      // Two tests, two attempts each.
      assertEquals(4, SampleStandaloneRerunCases.BEFORE_CALLS.get());
    } finally {
      IN_TEST_HARNESS.set(false);
    }
  }

  @Test
  public void testStandaloneRerunRequestIsFoundBehindWrappers() {
    TestPipeline.StandaloneRerunRequested request =
        new TestPipeline.StandaloneRerunRequested("no verdict");
    assertSame(request, BeamParallelJunit4Runner.standaloneRerunRequest(request));
    assertSame(
        request,
        BeamParallelJunit4Runner.standaloneRerunRequest(
            new AssertionError("unexpected exception type", request)));
    assertSame(
        request,
        BeamParallelJunit4Runner.standaloneRerunRequest(
            new MultipleFailureException(
                ImmutableList.of(new IllegalStateException("@After failed"), request))));
    assertNull(BeamParallelJunit4Runner.standaloneRerunRequest(new RuntimeException("other")));
    // A cause cycle must not hang detection.
    RuntimeException a = new RuntimeException("a");
    RuntimeException b = new RuntimeException("b", a);
    a.initCause(b);
    assertNull(BeamParallelJunit4Runner.standaloneRerunRequest(a));
    // It is an Error so that test code catching Exception around run() cannot swallow it.
    assertThat(request, not(instanceOf(Exception.class)));
  }
}
