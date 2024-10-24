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
package org.apache.beam.runners.dataflow.worker.windmill.work.processing.failures;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.KeyTokenInvalidException;
import org.apache.beam.runners.dataflow.worker.WorkItemCancelledException;
import org.apache.beam.runners.dataflow.worker.status.LastExceptionDataProvider;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutableWork;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Processes a failure that occurs during user processing of {@link Work}. */
@ThreadSafe
@Internal
public final class WorkFailureProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(WorkFailureProcessor.class);
  private static final Duration MAX_LOCAL_PROCESSING_RETRY_DURATION = Duration.standardMinutes(5);
  private static final int DEFAULT_RETRY_LOCALLY_MS = 10000;

  private final BoundedQueueExecutor workUnitExecutor;
  private final FailureTracker failureTracker;
  private final HeapDumper heapDumper;
  private final Supplier<Instant> clock;
  private final int retryLocallyDelayMs;

  private WorkFailureProcessor(
      BoundedQueueExecutor workUnitExecutor,
      FailureTracker failureTracker,
      HeapDumper heapDumper,
      Supplier<Instant> clock,
      int retryLocallyDelayMs) {
    this.workUnitExecutor = workUnitExecutor;
    this.failureTracker = failureTracker;
    this.heapDumper = heapDumper;
    this.clock = clock;
    this.retryLocallyDelayMs = retryLocallyDelayMs;
  }

  public static WorkFailureProcessor create(
      BoundedQueueExecutor workUnitExecutor,
      FailureTracker failureTracker,
      HeapDumper heapDumper,
      Supplier<Instant> clock) {
    return new WorkFailureProcessor(
        workUnitExecutor, failureTracker, heapDumper, clock, DEFAULT_RETRY_LOCALLY_MS);
  }

  @VisibleForTesting
  public static WorkFailureProcessor forTesting(
      BoundedQueueExecutor workUnitExecutor,
      FailureTracker failureTracker,
      HeapDumper heapDumper,
      Supplier<Instant> clock,
      int retryLocallyDelayMs) {
    return new WorkFailureProcessor(
        workUnitExecutor,
        failureTracker,
        heapDumper,
        clock,
        retryLocallyDelayMs >= 0 ? retryLocallyDelayMs : DEFAULT_RETRY_LOCALLY_MS);
  }

  /** Returns whether an exception was caused by a {@link OutOfMemoryError}. */
  private static boolean isOutOfMemoryError(Throwable t) {
    while (t != null) {
      if (t instanceof OutOfMemoryError) {
        return true;
      }
      t = t.getCause();
    }
    return false;
  }

  /**
   * Processes failures caused by thrown exceptions that occur during execution of {@link Work}. May
   * attempt to retry execution of the {@link Work} or drop it if it is invalid.
   */
  public void logAndProcessFailure(
      String computationId,
      ExecutableWork executableWork,
      Throwable t,
      Consumer<Work> onInvalidWork) {
    if (shouldRetryLocally(computationId, executableWork.work(), t)) {
      // Try again after some delay and at the end of the queue to avoid a tight loop.
      executeWithDelay(retryLocallyDelayMs, executableWork);
    } else {
      // Consider the item invalid. It will eventually be retried by Windmill if it still needs to
      // be processed.
      onInvalidWork.accept(executableWork.work());
    }
  }

  private String tryToDumpHeap() {
    return heapDumper
        .dumpAndGetHeap()
        .map(heapDump -> "written to '" + heapDump + "'")
        .orElseGet(() -> "not written");
  }

  private void executeWithDelay(long delayMs, ExecutableWork executableWork) {
    Uninterruptibles.sleepUninterruptibly(delayMs, TimeUnit.MILLISECONDS);
    workUnitExecutor.forceExecute(executableWork, executableWork.getWorkItem().getSerializedSize());
  }

  private boolean shouldRetryLocally(String computationId, Work work, Throwable t) {
    Throwable parsedException = t instanceof UserCodeException ? t.getCause() : t;
    if (KeyTokenInvalidException.isKeyTokenInvalidException(parsedException)) {
      LOG.debug(
          "Execution of work for computation '{}' on key '{}' failed due to token expiration. "
              + "Work will not be retried locally.",
          computationId,
          work.getWorkItem().getKey().toStringUtf8());
    } else if (WorkItemCancelledException.isWorkItemCancelledException(parsedException)) {
      LOG.debug(
          "Execution of work for computation '{}' on key '{}' failed. "
              + "Work will not be retried locally.",
          computationId,
          work.getWorkItem().getShardingKey());
    } else {
      LastExceptionDataProvider.reportException(parsedException);
      LOG.debug("Failed work: {}", work);
      Duration elapsedTimeSinceStart = new Duration(work.getStartTime(), clock.get());
      if (!failureTracker.trackFailure(computationId, work.getWorkItem(), parsedException)) {
        LOG.error(
            "Execution of work for computation '{}' on key '{}' failed with uncaught exception, "
                + "and Windmill indicated not to retry locally.",
            computationId,
            work.getWorkItem().getKey().toStringUtf8(),
            parsedException);
      } else if (isOutOfMemoryError(parsedException)) {
        String heapDump = tryToDumpHeap();
        LOG.error(
            "Execution of work for computation '{}' for key '{}' failed with out-of-memory. "
                + "Work will not be retried locally. Heap dump {}.",
            computationId,
            work.getWorkItem().getKey().toStringUtf8(),
            heapDump,
            parsedException);
      } else if (elapsedTimeSinceStart.isLongerThan(MAX_LOCAL_PROCESSING_RETRY_DURATION)) {
        LOG.error(
            "Execution of work for computation '{}' for key '{}' failed with uncaught exception, "
                + "and it will not be retried locally because the elapsed time since start {} "
                + "exceeds {}.",
            computationId,
            work.getWorkItem().getKey().toStringUtf8(),
            elapsedTimeSinceStart,
            MAX_LOCAL_PROCESSING_RETRY_DURATION,
            parsedException);
      } else {
        LOG.error(
            "Execution of work for computation '{}' on key '{}' failed with uncaught exception. "
                + "Work will be retried locally.",
            computationId,
            work.getWorkItem().getKey().toStringUtf8(),
            parsedException);
        return true;
      }
    }

    return false;
  }
}
