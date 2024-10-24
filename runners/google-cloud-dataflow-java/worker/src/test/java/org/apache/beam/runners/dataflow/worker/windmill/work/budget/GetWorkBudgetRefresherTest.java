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
package org.apache.beam.runners.dataflow.worker.windmill.work.budget;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertFalse;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GetWorkBudgetRefresherTest {
  private static final int WAIT_BUFFER = 10;
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);

  private GetWorkBudgetRefresher createBudgetRefresher(Runnable redistributeBudget) {
    return createBudgetRefresher(false, redistributeBudget);
  }

  private GetWorkBudgetRefresher createBudgetRefresher(
      boolean isBudgetRefreshPaused, Runnable redistributeBudget) {
    return new GetWorkBudgetRefresher(() -> isBudgetRefreshPaused, redistributeBudget);
  }

  @Test
  public void testStop_successfullyTerminates() throws InterruptedException {
    CountDownLatch redistributeBudgetLatch = new CountDownLatch(1);
    Runnable redistributeBudget = redistributeBudgetLatch::countDown;
    GetWorkBudgetRefresher budgetRefresher = createBudgetRefresher(redistributeBudget);
    budgetRefresher.start();
    budgetRefresher.stop();
    budgetRefresher.requestBudgetRefresh();
    boolean redistributeBudgetRan =
        redistributeBudgetLatch.await(WAIT_BUFFER, TimeUnit.MILLISECONDS);
    // Make sure that redistributeBudgetLatch.countDown() is never called.
    assertThat(redistributeBudgetLatch.getCount()).isEqualTo(1);
    assertFalse(redistributeBudgetRan);
  }

  @Test
  public void testRequestBudgetRefresh_triggersBudgetRefresh() throws InterruptedException {
    CountDownLatch redistributeBudgetLatch = new CountDownLatch(1);
    Runnable redistributeBudget = redistributeBudgetLatch::countDown;
    GetWorkBudgetRefresher budgetRefresher = createBudgetRefresher(redistributeBudget);
    budgetRefresher.start();
    budgetRefresher.requestBudgetRefresh();
    // Wait for redistribute budget to run.
    redistributeBudgetLatch.await();
    assertThat(redistributeBudgetLatch.getCount()).isEqualTo(0);
  }

  @Test
  public void testScheduledBudgetRefresh() throws InterruptedException {
    CountDownLatch redistributeBudgetLatch = new CountDownLatch(1);
    Runnable redistributeBudget = redistributeBudgetLatch::countDown;
    GetWorkBudgetRefresher budgetRefresher = createBudgetRefresher(redistributeBudget);
    budgetRefresher.start();
    // Wait for scheduled redistribute budget to run.
    redistributeBudgetLatch.await();
    assertThat(redistributeBudgetLatch.getCount()).isEqualTo(0);
  }

  @Test
  public void testTriggeredAndScheduledBudgetRefresh_concurrent() throws InterruptedException {
    CountDownLatch redistributeBudgetLatch = new CountDownLatch(2);
    Runnable redistributeBudget = redistributeBudgetLatch::countDown;
    GetWorkBudgetRefresher budgetRefresher = createBudgetRefresher(redistributeBudget);
    budgetRefresher.start();
    Thread budgetRefreshTriggerThread = new Thread(budgetRefresher::requestBudgetRefresh);
    budgetRefreshTriggerThread.start();
    budgetRefreshTriggerThread.join();
    // Wait for triggered and scheduled redistribute budget to run.
    redistributeBudgetLatch.await();
    assertThat(redistributeBudgetLatch.getCount()).isEqualTo(0);
  }

  @Test
  public void testTriggeredBudgetRefresh_doesNotRunWhenBudgetRefreshPaused()
      throws InterruptedException {
    CountDownLatch redistributeBudgetLatch = new CountDownLatch(1);
    Runnable redistributeBudget = redistributeBudgetLatch::countDown;
    GetWorkBudgetRefresher budgetRefresher = createBudgetRefresher(true, redistributeBudget);
    budgetRefresher.start();
    budgetRefresher.requestBudgetRefresh();
    boolean redistributeBudgetRan =
        redistributeBudgetLatch.await(WAIT_BUFFER, TimeUnit.MILLISECONDS);
    // Make sure that redistributeBudgetLatch.countDown() is never called.
    assertThat(redistributeBudgetLatch.getCount()).isEqualTo(1);
    assertFalse(redistributeBudgetRan);
  }

  @Test
  public void testScheduledBudgetRefresh_doesNotRunWhenBudgetRefreshPaused()
      throws InterruptedException {
    CountDownLatch redistributeBudgetLatch = new CountDownLatch(1);
    Runnable redistributeBudget = redistributeBudgetLatch::countDown;
    GetWorkBudgetRefresher budgetRefresher = createBudgetRefresher(true, redistributeBudget);
    budgetRefresher.start();
    boolean redistributeBudgetRan =
        redistributeBudgetLatch.await(
            GetWorkBudgetRefresher.SCHEDULED_BUDGET_REFRESH_MILLIS + WAIT_BUFFER,
            TimeUnit.MILLISECONDS);
    // Make sure that redistributeBudgetLatch.countDown() is never called.
    assertThat(redistributeBudgetLatch.getCount()).isEqualTo(1);
    assertFalse(redistributeBudgetRan);
  }
}
