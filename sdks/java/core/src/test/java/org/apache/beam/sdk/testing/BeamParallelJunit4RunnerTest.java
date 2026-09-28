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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.sdk.testing.BeamParallelJunit4Runner.SerialTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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

  @NotThreadSafe
  @RunWith(BeamParallelJunit4Runner.class)
  public static class SampleNotThreadSafeCases {
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
  public void testNotThreadSafeClassRunsSerially() {
    System.setProperty(BeamParallelJunit4Runner.VALIDATES_RUNNER_THREADS_PROPERTY, "4");

    IN_TEST_HARNESS.set(true);
    try {
      Result result = JUnitCore.runClasses(SampleNotThreadSafeCases.class);
      assertEquals(0, result.getFailureCount());
      assertEquals(2, result.getRunCount());
      assertEquals(1, SampleNotThreadSafeCases.MAX_ACTIVE.get());
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
}
