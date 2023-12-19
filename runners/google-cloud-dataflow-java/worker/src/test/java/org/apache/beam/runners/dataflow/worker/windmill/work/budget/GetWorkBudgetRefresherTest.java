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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class GetWorkBudgetRefresherTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  private static final int WAIT_BUFFER = 10;
  private final Runnable redistributeBudget = Mockito.mock(Runnable.class);

  private GetWorkBudgetRefresher createBudgetRefresher() {
    return createBudgetRefresher(false);
  }

  private GetWorkBudgetRefresher createBudgetRefresher(Boolean isBudgetRefreshPaused) {
    return new GetWorkBudgetRefresher(() -> isBudgetRefreshPaused, redistributeBudget);
  }

  @Test
  public void testStop_successfullyTerminates() throws InterruptedException {
    GetWorkBudgetRefresher budgetRefresher = createBudgetRefresher();
    budgetRefresher.start();
    budgetRefresher.stop();
    budgetRefresher.requestBudgetRefresh();
    Thread.sleep(WAIT_BUFFER);
    verifyNoInteractions(redistributeBudget);
  }

  @Test
  public void testRequestBudgetRefresh_triggersBudgetRefresh() throws InterruptedException {
    GetWorkBudgetRefresher budgetRefresher = createBudgetRefresher();
    budgetRefresher.start();
    budgetRefresher.requestBudgetRefresh();
    // Wait a bit for redistribute budget to run.
    Thread.sleep(WAIT_BUFFER);
    verify(redistributeBudget, times(1)).run();
  }

  @Test
  public void testScheduledBudgetRefresh() throws InterruptedException {
    GetWorkBudgetRefresher budgetRefresher = createBudgetRefresher();
    budgetRefresher.start();
    Thread.sleep(GetWorkBudgetRefresher.SCHEDULED_BUDGET_REFRESH_MILLIS + WAIT_BUFFER);
    verify(redistributeBudget, times(1)).run();
  }

  @Test
  public void testTriggeredAndScheduledBudgetRefresh_concurrent() throws InterruptedException {
    GetWorkBudgetRefresher budgetRefresher = createBudgetRefresher();
    budgetRefresher.start();
    Thread budgetRefreshTriggerThread = new Thread(budgetRefresher::requestBudgetRefresh);
    budgetRefreshTriggerThread.start();
    Thread.sleep(GetWorkBudgetRefresher.SCHEDULED_BUDGET_REFRESH_MILLIS + WAIT_BUFFER);
    budgetRefreshTriggerThread.join();

    // Wait a bit for redistribute budget to run.
    Thread.sleep(WAIT_BUFFER);
    verify(redistributeBudget, times(2)).run();
  }

  @Test
  public void testTriggeredBudgetRefresh_doesNotRunWhenBudgetRefreshPaused()
      throws InterruptedException {
    GetWorkBudgetRefresher budgetRefresher = createBudgetRefresher(true);
    budgetRefresher.start();
    budgetRefresher.requestBudgetRefresh();
    Thread.sleep(WAIT_BUFFER);
    verifyNoInteractions(redistributeBudget);
  }

  @Test
  public void testScheduledBudgetRefresh_doesNotRunWhenBudgetRefreshPaused()
      throws InterruptedException {
    GetWorkBudgetRefresher budgetRefresher = createBudgetRefresher(true);
    budgetRefresher.start();
    Thread.sleep(GetWorkBudgetRefresher.SCHEDULED_BUDGET_REFRESH_MILLIS + WAIT_BUFFER);
    verifyNoInteractions(redistributeBudget);
  }
}
