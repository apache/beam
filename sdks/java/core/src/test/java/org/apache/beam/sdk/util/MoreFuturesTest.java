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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.util.MoreFutures.ExceptionOrResult;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link MoreFutures}. */
@RunWith(JUnit4.class)
public class MoreFuturesTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void supplyAsyncSuccess() throws Exception {
    CompletionStage<Integer> future = MoreFutures.supplyAsync(() -> 42);
    assertThat(MoreFutures.get(future), equalTo(42));
  }

  @Test
  public void supplyAsyncFailure() throws Exception {
    final String testMessage = "this is just a test";
    CompletionStage<Long> future =
        MoreFutures.supplyAsync(
            () -> {
              throw new IllegalStateException(testMessage);
            });

    thrown.expect(ExecutionException.class);
    thrown.expectCause(isA(IllegalStateException.class));
    thrown.expectMessage(testMessage);
    MoreFutures.get(future);
  }

  @Test
  public void runAsyncSuccess() throws Exception {
    AtomicInteger result = new AtomicInteger(0);
    CompletionStage<Void> sideEffectFuture =
        MoreFutures.runAsync(
            () -> {
              result.set(42);
            });

    MoreFutures.get(sideEffectFuture);
    assertThat(result.get(), equalTo(42));
  }

  @Test
  public void runAsyncFailure() throws Exception {
    final String testMessage = "this is just a test";
    CompletionStage<Void> sideEffectFuture =
        MoreFutures.runAsync(
            () -> {
              throw new IllegalStateException(testMessage);
            });

    thrown.expect(ExecutionException.class);
    thrown.expectCause(isA(IllegalStateException.class));
    thrown.expectMessage(testMessage);
    MoreFutures.get(sideEffectFuture);
  }

  @Test
  public void testAllAsListRespectsOriginalList() throws Exception {
    CountDownLatch waitTillThreadRunning = new CountDownLatch(1);
    CountDownLatch waitTillClearHasHappened = new CountDownLatch(1);
    List<CompletionStage<Void>> stages = new ArrayList<>();
    stages.add(MoreFutures.runAsync(waitTillThreadRunning::countDown));
    stages.add(MoreFutures.runAsync(waitTillClearHasHappened::await));

    CompletionStage<List<Void>> results = MoreFutures.allAsList(stages);
    waitTillThreadRunning.await();
    stages.clear();
    waitTillClearHasHappened.countDown();
    assertEquals(MoreFutures.get(results), Arrays.asList(null, null));
  }

  @Test
  public void testAllAsListNoExceptionDueToMutation() throws Exception {
    // This loop runs many times trying to exercise a race condition that existed where mutation
    // of the passed in completion stages lead to various exceptions (such as a
    // ConcurrentModificationException). See https://github.com/apache/beam/issues/23809
    for (int i = 0; i < 10000; ++i) {
      CountDownLatch waitTillThreadRunning = new CountDownLatch(1);
      List<CompletionStage<Void>> stages = new ArrayList<>();
      stages.add(MoreFutures.runAsync(waitTillThreadRunning::countDown));

      CompletionStage<List<Void>> results = MoreFutures.allAsList(stages);
      waitTillThreadRunning.await();
      stages.clear();
      MoreFutures.get(results);
    }
  }

  @Test
  public void testAllAsListWithExceptionsRespectsOriginalList() throws Exception {
    CountDownLatch waitTillThreadRunning = new CountDownLatch(1);
    CountDownLatch waitTillClearHasHappened = new CountDownLatch(1);
    List<CompletionStage<Void>> stages = new ArrayList<>();
    stages.add(MoreFutures.runAsync(waitTillThreadRunning::countDown));
    stages.add(MoreFutures.runAsync(waitTillClearHasHappened::await));

    CompletionStage<List<ExceptionOrResult<Void>>> results =
        MoreFutures.allAsListWithExceptions(stages);
    waitTillThreadRunning.await();
    stages.clear();
    waitTillClearHasHappened.countDown();
    assertEquals(
        MoreFutures.get(results),
        Arrays.asList(ExceptionOrResult.result(null), ExceptionOrResult.result(null)));
  }

  @Test
  public void testAllAsListWithExceptionsNoExceptionDueToMutation() throws Exception {
    // This loop runs many times trying to exercise a race condition that existed where mutation
    // of the passed in completion stages lead to various exceptions (such as a
    // ConcurrentModificationException). See https://github.com/apache/beam/issues/23809
    for (int i = 0; i < 10000; ++i) {
      CountDownLatch waitTillThreadRunning = new CountDownLatch(1);
      List<CompletionStage<Void>> stages = new ArrayList<>();
      stages.add(MoreFutures.runAsync(waitTillThreadRunning::countDown));

      CompletionStage<List<ExceptionOrResult<Void>>> results =
          MoreFutures.allAsListWithExceptions(stages);
      waitTillThreadRunning.await();
      stages.clear();
      MoreFutures.get(results);
    }
  }
}
