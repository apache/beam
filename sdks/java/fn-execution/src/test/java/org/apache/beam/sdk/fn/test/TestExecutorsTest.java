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
package org.apache.beam.sdk.fn.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.fn.test.TestExecutors.TestExecutorService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.Statement;

/** Tests for {@link TestExecutors}. */
@RunWith(JUnit4.class)
public class TestExecutorsTest {
  @Test
  public void testSuccessfulTermination() throws Throwable {
    ExecutorService service = Executors.newSingleThreadExecutor();
    final TestExecutorService testService = TestExecutors.from(service);
    final AtomicBoolean taskRan = new AtomicBoolean();
    testService
        .apply(
            new Statement() {
              @Override
              public void evaluate() throws Throwable {
                testService.submit(() -> taskRan.set(true)).get();
              }
            },
            null)
        .evaluate();
    assertTrue(service.isTerminated());
    assertTrue(taskRan.get());
  }

  @Test
  // FutureReturnValueIgnored suppression is safe because testService is
  // expected to *not* shutdown cleanly on the task it was given to execute.
  // If we try to obtain the result of the future the test will timeout.
  @SuppressWarnings("FutureReturnValueIgnored")
  public void testTaskBlocksForeverCausesFailure() throws Throwable {
    ExecutorService service = Executors.newSingleThreadExecutor();
    final TestExecutorService testService = TestExecutors.from(service);
    final AtomicBoolean taskStarted = new AtomicBoolean();
    final AtomicBoolean taskWasInterrupted = new AtomicBoolean();
    try {
      testService
          .apply(
              new Statement() {
                @Override
                public void evaluate() throws Throwable {
                  testService.submit(this::taskToRun);
                }

                private void taskToRun() {
                  taskStarted.set(true);
                  try {
                    while (true) {
                      Thread.sleep(10000);
                    }
                  } catch (InterruptedException e) {
                    taskWasInterrupted.set(true);
                    return;
                  }
                }
              },
              null)
          .evaluate();
      fail();
    } catch (IllegalStateException e) {
      assertEquals(IllegalStateException.class, e.getClass());
      assertEquals("Test executor failed to shutdown cleanly.", e.getMessage());
    }
    assertTrue(service.isShutdown());
  }

  @Test
  public void testStatementFailurePropagatedCleanly() throws Throwable {
    ExecutorService service = Executors.newSingleThreadExecutor();
    final TestExecutorService testService = TestExecutors.from(service);
    final RuntimeException exceptionToThrow = new RuntimeException();
    try {
      testService
          .apply(
              new Statement() {
                @Override
                public void evaluate() throws Throwable {
                  throw exceptionToThrow;
                }
              },
              null)
          .evaluate();
      fail();
    } catch (RuntimeException thrownException) {
      assertSame(exceptionToThrow, thrownException);
    }
    assertTrue(service.isShutdown());
  }

  @Test
  // FutureReturnValueIgnored suppression is safe because testService is
  // expected to *not* shutdown cleanly on the task it was given to execute.
  // If we try to obtain the result of the future the test will timeout.
  @SuppressWarnings("FutureReturnValueIgnored")
  public void testStatementFailurePropagatedWhenExecutorServiceFailingToTerminate()
      throws Throwable {
    ExecutorService service = Executors.newSingleThreadExecutor();
    final TestExecutorService testService = TestExecutors.from(service);
    final AtomicBoolean taskStarted = new AtomicBoolean();
    final AtomicBoolean taskWasInterrupted = new AtomicBoolean();
    final RuntimeException exceptionToThrow = new RuntimeException();
    try {
      testService
          .apply(
              new Statement() {
                @Override
                public void evaluate() throws Throwable {
                  testService.submit(this::taskToRun);
                  throw exceptionToThrow;
                }

                private void taskToRun() {
                  taskStarted.set(true);
                  try {
                    while (true) {
                      Thread.sleep(10000);
                    }
                  } catch (InterruptedException e) {
                    taskWasInterrupted.set(true);
                    return;
                  }
                }
              },
              null)
          .evaluate();
      fail();
    } catch (RuntimeException thrownException) {
      assertSame(exceptionToThrow, thrownException);
      assertEquals(1, exceptionToThrow.getSuppressed().length);
      assertEquals(IllegalStateException.class, exceptionToThrow.getSuppressed()[0].getClass());
      assertEquals(
          "Test executor failed to shutdown cleanly.",
          exceptionToThrow.getSuppressed()[0].getMessage());
    }
    assertTrue(service.isShutdown());
  }
}
