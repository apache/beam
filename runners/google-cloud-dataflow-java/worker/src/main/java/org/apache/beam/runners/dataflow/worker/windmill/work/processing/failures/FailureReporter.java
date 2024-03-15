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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList.toImmutableList;

import com.google.api.services.dataflow.model.Status;
import com.google.rpc.Code;
import java.util.function.Supplier;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.WorkItem;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.EvictingQueue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

/** Reports failures that occur during user processing. */
@ThreadSafe
@Internal
public abstract class FailureReporter implements Supplier<ImmutableList<Status>> {

  private final int maxStackTraceDepthToReport;

  @GuardedBy("pendingFailuresToReport")
  private final EvictingQueue<String> pendingFailuresToReport;

  protected FailureReporter(
      int maxStackTraceDepthToReport, EvictingQueue<String> pendingFailuresToReport) {
    this.maxStackTraceDepthToReport = maxStackTraceDepthToReport;
    this.pendingFailuresToReport = pendingFailuresToReport;
  }

  /**
   * Reports the failure to streaming backend. Returns whether the processing should be retried
   * locally.
   */
  public boolean reportFailure(String computationId, WorkItem work, Throwable failure) {
    addFailure(buildExceptionStackTrace(failure));
    return shouldRetryLocally(computationId, work);
  }

  @Override
  public ImmutableList<Status> get() {
    return getAndResetPendingFailures();
  }

  private String buildExceptionStackTrace(Throwable t) {
    StringBuilder builder = new StringBuilder(1024);
    Throwable cur = t;
    for (int depth = 0; cur != null && depth < maxStackTraceDepthToReport; cur = cur.getCause()) {
      if (depth > 0) {
        builder.append("\nCaused by: ");
      }
      builder.append(cur);
      depth++;
      for (StackTraceElement frame : cur.getStackTrace()) {
        if (depth < maxStackTraceDepthToReport) {
          builder.append("\n        ");
          builder.append(frame);
          depth++;
        }
      }
    }
    if (cur != null) {
      builder.append("\nStack trace truncated. Please see Cloud Logging for the entire trace.");
    }
    return builder.toString();
  }

  protected abstract boolean shouldRetryLocally(String computationId, Windmill.WorkItem work);

  /**
   * Adds the given failure message to the queue of messages to be reported to DFE in periodic
   * updates.
   */
  private void addFailure(String failureMessage) {
    synchronized (pendingFailuresToReport) {
      pendingFailuresToReport.add(failureMessage);
    }
  }

  private ImmutableList<Status> getAndResetPendingFailures() {
    synchronized (pendingFailuresToReport) {
      ImmutableList<Status> pendingFailures =
          pendingFailuresToReport.stream()
              .map(
                  stackTrace ->
                      new Status().setCode(Code.UNKNOWN.getNumber()).setMessage(stackTrace))
              .collect(toImmutableList());
      pendingFailuresToReport.clear(); // Best effort only, no need to wait till successfully sent.

      return pendingFailures;
    }
  }
}
