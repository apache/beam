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
package org.apache.beam.runners.direct;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.theInstance;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.UserCodeException;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DoFnLifecycleManager}. */
@RunWith(JUnit4.class)
public class DoFnLifecycleManagerTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private TestFn fn = new TestFn();
  private DoFnLifecycleManager mgr = DoFnLifecycleManager.of(fn, PipelineOptionsFactory.create());

  @Test
  public void setupOnGet() throws Exception {
    TestFn obtained = (TestFn) mgr.get();

    assertThat(obtained, not(theInstance(fn)));
    assertThat(obtained.setupCalled, is(true));
    assertThat(obtained.teardownCalled, is(false));
  }

  @Test
  public void getMultipleCallsSingleSetupCall() throws Exception {
    TestFn obtained = (TestFn) mgr.get();
    TestFn secondObtained = (TestFn) mgr.get();

    assertThat(obtained, theInstance(secondObtained));
    assertThat(obtained.setupCalled, is(true));
    assertThat(obtained.teardownCalled, is(false));
  }

  @Test
  public void getMultipleThreadsDifferentInstances() throws Exception {
    CountDownLatch startSignal = new CountDownLatch(1);
    ExecutorService executor = Executors.newCachedThreadPool();
    List<Future<TestFn>> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      futures.add(executor.submit(new GetFnCallable(mgr, startSignal)));
    }
    startSignal.countDown();
    List<TestFn> fns = new ArrayList<>();
    for (Future<TestFn> future : futures) {
      fns.add(future.get(1L, TimeUnit.SECONDS));
    }

    for (TestFn fn : fns) {
      assertThat(fn.setupCalled, is(true));
      int sameInstances = 0;
      for (TestFn otherFn : fns) {
        if (otherFn == fn) {
          sameInstances++;
        }
      }
      assertThat(sameInstances, equalTo(1));
    }
  }

  @Test
  public void teardownOnRemove() throws Exception {
    TestFn obtained = (TestFn) mgr.get();
    mgr.remove();

    assertThat(obtained, not(theInstance(fn)));
    assertThat(obtained.setupCalled, is(true));
    assertThat(obtained.teardownCalled, is(true));

    assertThat(mgr.get(), not(Matchers.<DoFn<?, ?>>theInstance(obtained)));
  }

  @Test
  public void teardownThrowsRemoveThrows() throws Exception {
    TestFn obtained = (TestFn) mgr.get();
    obtained.teardown();

    thrown.expect(UserCodeException.class);
    thrown.expectCause(isA(IllegalStateException.class));
    thrown.expectMessage("Cannot call teardown: already torn down");
    mgr.remove();
  }

  @Test
  public void teardownAllOnRemoveAll() throws Exception {
    CountDownLatch startSignal = new CountDownLatch(1);
    ExecutorService executor = Executors.newCachedThreadPool();
    List<Future<TestFn>> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      futures.add(executor.submit(new GetFnCallable(mgr, startSignal)));
    }
    startSignal.countDown();
    List<TestFn> fns = new ArrayList<>();
    for (Future<TestFn> future : futures) {
      fns.add(future.get(1L, TimeUnit.SECONDS));
    }
    mgr.removeAll();

    for (TestFn fn : fns) {
      assertThat(fn.setupCalled, is(true));
      assertThat(fn.teardownCalled, is(true));
    }
  }

  @Test
  public void removeAndRemoveAllConcurrent() throws Exception {
    CountDownLatch startSignal = new CountDownLatch(1);
    ExecutorService executor = Executors.newCachedThreadPool();
    List<Future<TestFn>> futures = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      futures.add(executor.submit(new GetFnCallable(mgr, startSignal)));
    }
    startSignal.countDown();
    List<TestFn> fns = new ArrayList<>();
    for (Future<TestFn> future : futures) {
      fns.add(future.get(1L, TimeUnit.SECONDS));
    }
    CountDownLatch removeSignal = new CountDownLatch(1);
    List<Future<Void>> removeFutures = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      // These will reuse the threads used in the GetFns
      removeFutures.add(executor.submit(new TeardownFnCallable(mgr, removeSignal)));
    }
    removeSignal.countDown();
    assertThat(mgr.removeAll(), Matchers.emptyIterable());
    for (Future<Void> removed : removeFutures) {
      // Should not have thrown an exception.
      removed.get();
    }

    for (TestFn fn : fns) {
      assertThat(fn.setupCalled, is(true));
      assertThat(fn.teardownCalled, is(true));
    }
  }

  private static class GetFnCallable implements Callable<TestFn> {
    private final DoFnLifecycleManager mgr;
    private final CountDownLatch startSignal;

    private GetFnCallable(DoFnLifecycleManager mgr, CountDownLatch startSignal) {
      this.mgr = mgr;
      this.startSignal = startSignal;
    }

    @Override
    public TestFn call() throws Exception {
      startSignal.await();
      return (TestFn) mgr.get();
    }
  }

  private static class TeardownFnCallable implements Callable<Void> {
    private final DoFnLifecycleManager mgr;
    private final CountDownLatch startSignal;

    private TeardownFnCallable(DoFnLifecycleManager mgr, CountDownLatch startSignal) {
      this.mgr = mgr;
      this.startSignal = startSignal;
    }

    @Override
    public Void call() throws Exception {
      startSignal.await();
      // Will throw an exception if the TestFn has already been removed from this thread
      mgr.remove();
      return null;
    }
  }

  private static class TestFn extends DoFn<Object, Object> {
    boolean setupCalled = false;
    boolean teardownCalled = false;

    @Setup
    public void setup() {
      checkState(!setupCalled, "Cannot call setup: already set up");
      checkState(!teardownCalled, "Cannot call setup: already torn down");

      setupCalled = true;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {}

    @Teardown
    public void teardown() {
      checkState(setupCalled, "Cannot call teardown: not set up");
      checkState(!teardownCalled, "Cannot call teardown: already torn down");

      teardownCalled = true;
    }
  }
}
