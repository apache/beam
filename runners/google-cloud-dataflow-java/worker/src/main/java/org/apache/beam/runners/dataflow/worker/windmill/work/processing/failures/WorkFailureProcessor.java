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

import java.io.File;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.KeyTokenInvalidException;
import org.apache.beam.runners.dataflow.worker.WorkItemCancelledException;
import org.apache.beam.runners.dataflow.worker.status.LastExceptionDataProvider;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.util.UserCodeException;
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

  private final BoundedQueueExecutor workUnitExecutor;
  private final FailureReporter failureReporter;
  private final Supplier<Optional<File>> heapDumpFetcher;
  private final Supplier<Instant> clock;
  private final int retryLocallyDelayMs;

  public WorkFailureProcessor(
      BoundedQueueExecutor workUnitExecutor,
      FailureReporter failureReporter,
      Supplier<Optional<File>> heapDumpFetcher,
      Supplier<Instant> clock,
      int retryLocallyDelayMs) {
    this.workUnitExecutor = workUnitExecutor;
    this.failureReporter = failureReporter;
    this.heapDumpFetcher = heapDumpFetcher;
    this.clock = clock;
    this.retryLocallyDelayMs = retryLocallyDelayMs;
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
      String computationId, Work work, Throwable t, Consumer<Work> onInvalidWork) {
    if (shouldRetryLocally(
        computationId, work, t instanceof UserCodeException ? t.getCause() : t)) {
      // Try again after some delay and at the end of the queue to avoid a tight loop.
      executeWithDelay(retryLocallyDelayMs, work);
    } else {
      // Consider the item invalid. It will eventually be retried by Windmill if it still needs to
      // be processed.
      onInvalidWork.accept(work);
    }
  }

  private String getHeapDumpLog() {
    return heapDumpFetcher
        .get()
        .map(heapDump -> "written to '" + heapDump + "'")
        .orElseGet(() -> "not written");
  }

  private void executeWithDelay(long delayMs, Work work) {
    Uninterruptibles.sleepUninterruptibly(delayMs, TimeUnit.MILLISECONDS);
    workUnitExecutor.forceExecute(work, work.getWorkItem().getSerializedSize());
  }

  private boolean shouldRetryLocally(String computationId, Work work, Throwable t) {
    if (KeyTokenInvalidException.isKeyTokenInvalidException(t)) {
      LOG.debug(
          "Execution of work for computation '{}' on key '{}' failed due to token expiration. "
              + "Work will not be retried locally.",
          computationId,
          work.getWorkItem().getKey().toStringUtf8());
    } else if (WorkItemCancelledException.isWorkItemCancelledException(t)) {
      LOG.debug(
          "Execution of work for computation '{}' on key '{}' failed. "
              + "Work will not be retried locally.",
          computationId,
          work.getWorkItem().getShardingKey());
    } else {
      LastExceptionDataProvider.reportException(t);
      LOG.debug("Failed work: {}", work);
      Duration elapsedTimeSinceStart = new Duration(work.getStartTime(), clock.get());
      if (!failureReporter.reportFailure(computationId, work.getWorkItem(), t)) {
        LOG.error(
            "Execution of work for computation '{}' on key '{}' failed with uncaught exception, "
                + "and Windmill indicated not to retry locally.",
            computationId,
            work.getWorkItem().getKey().toStringUtf8(),
            t);
      } else if (isOutOfMemoryError(t)) {
        LOG.error(
            "Execution of work for computation '{}' for key '{}' failed with out-of-memory. "
                + "Work will not be retried locally. Heap dump {}.",
            computationId,
            work.getWorkItem().getKey().toStringUtf8(),
            getHeapDumpLog(),
            t);
      } else if (elapsedTimeSinceStart.isLongerThan(MAX_LOCAL_PROCESSING_RETRY_DURATION)) {
        LOG.error(
            "Execution of work for computation '{}' for key '{}' failed with uncaught exception, "
                + "and it will not be retried locally because the elapsed time since start {} "
                + "exceeds {}.",
            computationId,
            work.getWorkItem().getKey().toStringUtf8(),
            elapsedTimeSinceStart,
            MAX_LOCAL_PROCESSING_RETRY_DURATION,
            t);
      } else {
        LOG.error(
            "Execution of work for computation '{}' on key '{}' failed with uncaught exception. "
                + "Work will be retried locally.",
            computationId,
            work.getWorkItem().getKey().toStringUtf8(),
            t);
        return true;
      }
    }

    return false;
  }
}
