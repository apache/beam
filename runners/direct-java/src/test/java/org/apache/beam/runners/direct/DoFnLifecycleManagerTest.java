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

import static com.google.common.base.Preconditions.checkState;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.theInstance;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.transforms.DoFn;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link DoFnLifecycleManager}.
 */
@RunWith(JUnit4.class)
public class DoFnLifecycleManagerTest {
  private TestFn fn = new TestFn();
  private DoFnLifecycleManager mgr = DoFnLifecycleManager.of(fn);

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


  private static class TestFn extends DoFn<Object, Object> {
    boolean setupCalled = false;
    boolean teardownCalled = false;

    @Setup
    public void setup() {
      checkState(!setupCalled);
      checkState(!teardownCalled);

      setupCalled = true;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
    }

    @Teardown
    public void teardown() {
      checkState(setupCalled);
      checkState(!teardownCalled);

      teardownCalled = true;
    }
  }
}
