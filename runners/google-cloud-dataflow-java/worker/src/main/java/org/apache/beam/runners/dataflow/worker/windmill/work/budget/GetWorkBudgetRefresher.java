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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.util.TerminatingExecutors;
import org.apache.beam.runners.dataflow.worker.util.TerminatingExecutors.TerminatingExecutorService;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.fn.stream.AdvancingPhaser;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles refreshing the budget either via triggered or scheduled execution using a {@link
 * java.util.concurrent.Phaser} to emulate publish/subscribe pattern.
 */
@Internal
@ThreadSafe
public final class GetWorkBudgetRefresher {
  @VisibleForTesting public static final int SCHEDULED_BUDGET_REFRESH_MILLIS = 100;
  private static final int INITIAL_BUDGET_REFRESH_PHASE = 0;
  private static final String BUDGET_REFRESH_THREAD = "GetWorkBudgetRefreshThread";
  private static final Logger LOG = LoggerFactory.getLogger(GetWorkBudgetRefresher.class);

  private final AdvancingPhaser budgetRefreshTrigger;
  private final TerminatingExecutorService budgetRefreshExecutor;
  private final Supplier<Boolean> isBudgetRefreshPaused;
  private final Runnable redistributeBudget;

  public GetWorkBudgetRefresher(
      Supplier<Boolean> isBudgetRefreshPaused, Runnable redistributeBudget) {
    this.budgetRefreshTrigger = new AdvancingPhaser(1);
    this.budgetRefreshExecutor =
        TerminatingExecutors.newSingleThreadExecutor(
            new ThreadFactoryBuilder()
                .setNameFormat(BUDGET_REFRESH_THREAD)
                .setUncaughtExceptionHandler(
                    (t, e) ->
                        LOG.error(
                            "{} failed due to uncaught exception during execution. ",
                            t.getName(),
                            e)),
            LOG);
    this.isBudgetRefreshPaused = isBudgetRefreshPaused;
    this.redistributeBudget = redistributeBudget;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  public void start() {
    budgetRefreshExecutor.submit(this::subscribeToRefreshBudget);
  }

  /** Publishes an event to trigger a budget refresh. */
  public void requestBudgetRefresh() {
    budgetRefreshTrigger.arrive();
  }

  public void stop() {
    budgetRefreshTrigger.arriveAndDeregister();
    // Put the budgetRefreshTrigger in a terminated state, #waitForBudgetRefreshEventWithTimeout
    // will subsequently return false, and #subscribeToRefreshBudget will return, completing the
    // task.
    budgetRefreshTrigger.forceTermination();
    budgetRefreshExecutor.shutdownNow();
  }

  private void subscribeToRefreshBudget() {
    int currentBudgetRefreshPhase = INITIAL_BUDGET_REFRESH_PHASE;
    // Runs forever until #stop is called.
    while (true) {
      currentBudgetRefreshPhase = waitForBudgetRefreshEventWithTimeout(currentBudgetRefreshPhase);
      // Phaser.awaitAdvanceInterruptibly(...) returns a negative value if the phaser is
      // terminated, else returns when either a budget refresh has been manually triggered or
      // SCHEDULED_BUDGET_REFRESH_MILLIS have passed.
      if (currentBudgetRefreshPhase < 0) {
        return;
      }
      // Budget refreshes are paused during endpoint updates.
      if (!isBudgetRefreshPaused.get()) {
        redistributeBudget.run();
      }
    }
  }

  /**
   * Waits for a budget refresh trigger event with a timeout. Returns the current phase of the
   * {@link #budgetRefreshTrigger}, to be used for following waits for the {@link
   * #budgetRefreshTrigger} to advance.
   *
   * <p>Budget refresh event is triggered when {@link #budgetRefreshTrigger} moves on from the given
   * currentBudgetRefreshPhase.
   */
  private int waitForBudgetRefreshEventWithTimeout(int currentBudgetRefreshPhase) {
    try {
      // Wait for budgetRefreshTrigger to advance FROM the current phase.
      return budgetRefreshTrigger.awaitAdvanceInterruptibly(
          currentBudgetRefreshPhase, SCHEDULED_BUDGET_REFRESH_MILLIS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new BudgetRefreshException("Error occurred waiting for budget refresh.", e);
    } catch (TimeoutException ignored) {
      // Intentionally do nothing since we trigger the budget refresh on the timeout.
    }

    return currentBudgetRefreshPhase;
  }

  private static class BudgetRefreshException extends RuntimeException {
    private BudgetRefreshException(String msg, Throwable sourceException) {
      super(msg, sourceException);
    }
  }
}
